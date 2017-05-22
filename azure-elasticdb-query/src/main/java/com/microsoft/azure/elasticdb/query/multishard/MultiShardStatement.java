package com.microsoft.azure.elasticdb.query.multishard;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.google.common.base.Stopwatch;
import com.microsoft.azure.elasticdb.core.commons.logging.ActivityIdScope;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryBehavior;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.query.exception.MultiShardAggregateException;
import com.microsoft.azure.elasticdb.query.logging.CommandBehavior;
import com.microsoft.azure.elasticdb.query.logging.MultiShardExecutionOptions;
import com.microsoft.azure.elasticdb.query.logging.MultiShardExecutionPolicy;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Complements the <see cref="MultiShardConnection"/> with a command object similar to the triad of
 * <see cref="Connection"/>, <see cref="Statement"/>, and <see cref="ResultSet"/>. The <see
 * cref="MultiShardStatement"/> takes a T-SQL command statement as its input and executes the
 * command across its collection of shards specified by its corresponding <see
 * cref="MultiShardConnection"/>. The results from processing the <see cref="MultiShardStatement"/>
 * are made available through the execute methods and the <see cref="MultiShardResultSet"/>.
 * Purpose: Complements the MultiShardConnection and abstracts away the work of running a given
 * command against multiple shards Notes: This class is NOT thread-safe. Since the Sync API
 * internally invokes the async API, connections to shards with connection string property "context
 * connection = true" are not supported. Transaction semantics are not supported.
 */
public final class MultiShardStatement implements AutoCloseable {

  /**
   * The Logger.
   */
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Default command timeout per shard in seconds.
   */
  private static final int DEFAULT_COMMAND_TIMEOUT_PER_SHARD = 30;

  /**
   * Default command timeout in seconds.
   */
  private static final int DEFAULT_COMMAND_TIMEOUT = 300;

  /**
   * Lock to enable thread-safe Cancel().
   */
  private final Object cancellationLock = new Object();

  /**
   * The sql command to be executed against shards.
   */
  private String commandText;

  /**
   * Task associated with current command invocation.
   */
  private FutureTask<MultiShardResultSet> currentTask = null;

  //The point of these properties is precisely to allow the user to specify whatever SQL they wish.
  /**
   * ActivityId of the current command being executed.
   */
  private UUID activityId;

  /**
   * Whether this command has already been disposed.
   */
  private boolean isDisposed = false;

  private int commandTimeout;

  private int commandTimeoutPerShard;

  /**
   * The retry behavior for detecting transient faults that could occur when connecting to and
   * executing commands against individual shards.
   * The <see cref="Microsoft.Azure.SqlDatabase.ElasticScale.RetryBehavior.DefaultRetryBehavior"/>
   * is the default.
   */
  private RetryBehavior retryBehavior;

  /**
   * The execution policy to use when executing
   * commands against shards. Through this policy,
   * users can control whether complete results are required,
   * or whether partial results are acceptable.
   */
  private MultiShardExecutionPolicy executionPolicy;

  /**
   * Gets the current instance of the <see cref="MultiShardConnection"/> associated with this
   * command.
   */
  private MultiShardConnection connection;

  /**
   * Gets or sets options that control how the command is executed.
   * For instance, you can use this to include the shard name as
   * an additional column into the result.
   */
  private MultiShardExecutionOptions executionOptions;

  /**
   * The retry policy to use when connecting to and
   * executing commands against individual shards.
   */
  private RetryPolicy retryPolicy;

  // The SqlCommand underlies the object we will return.  We don't want to dispose it. The point of
  // this c-tor is to allow the user to specify whatever sql text they wish.

  /**
   * Creates an instance of this class.
   *
   * @param connection The connection to shards
   * @param commandText The sql command to execute against the shards
   * @param commandTimeout Command timeout for given commandText to be run against ALL shards
   */
  private MultiShardStatement(MultiShardConnection connection, String commandText,
      int commandTimeout) {
    this.connection = connection;
    this.commandTimeout = commandTimeout;
    this.commandText = commandText;

    // Set defaults
    this.setRetryPolicy(RetryPolicy.DefaultRetryPolicy);
    this.setRetryBehavior(RetryBehavior.getDefaultRetryBehavior());
    this.setExecutionPolicy(MultiShardExecutionPolicy.CompleteResults);
    this.setExecutionOptions(MultiShardExecutionOptions.None);
  }

  /**
   * Instance constructor of this class. Default command timeout of 300 seconds is used.
   *
   * @param connection The connection to shards
   * @param commandText The command text to execute against shards
   * @return An Instance of this class.
   */
  public static MultiShardStatement create(MultiShardConnection connection, String commandText) {
    return MultiShardStatement.create(connection, commandText,
        MultiShardStatement.DEFAULT_COMMAND_TIMEOUT);
  }

  /**
   * Instance constructor of this class. Default command type is text.
   *
   * @param connection The connection to shards
   * @param commandText The command text to execute against shards
   * @param commandTimeout Command timeout for given commandText to be run against ALL shards
   * @return An Instance of this class.
   */
  public static MultiShardStatement create(MultiShardConnection connection, String commandText,
      int commandTimeout) {
    return new MultiShardStatement(connection, commandText, commandTimeout);
  }

  /**
   * Gets the command text to execute against the set of shards.
   */
  public String getCommandText() {
    return commandText;
  }

  /**
   * Sets the command text to execute against the set of shards.
   */
  public void setCommandText(String value) {
    commandText = value;
  }

  /**
   * Time in seconds to wait for the command to be executed on ALL shards.
   * A value of 0 indicates no wait time limit. The default is 300 seconds.
   */
  public int getCommandTimeout() {
    return this.commandTimeout <= 0 ? DEFAULT_COMMAND_TIMEOUT : this.commandTimeout;
  }

  public void setCommandTimeout(int commandTimeout) {
    this.commandTimeout = commandTimeout;
  }

  /**
   * This property controls the timeout for running a command against individual shards.
   */
  public int getCommandTimeoutPerShard() {
    return this.commandTimeoutPerShard <= 0 ? DEFAULT_COMMAND_TIMEOUT_PER_SHARD
        : this.commandTimeoutPerShard;
  }

  /**
   * This property controls the timeout for running a command against individual shards.
   */
  public void setCommandTimeoutPerShard(int value) {
    this.commandTimeoutPerShard = value;
  }

  public RetryBehavior getRetryBehavior() {
    return retryBehavior;
  }

  public void setRetryBehavior(RetryBehavior value) {
    retryBehavior = value;
  }

  public MultiShardExecutionPolicy getExecutionPolicy() {
    return executionPolicy;
  }

  public void setExecutionPolicy(MultiShardExecutionPolicy value) {
    executionPolicy = value;
  }

  public MultiShardConnection getConnection() {
    return connection;
  }

  public MultiShardExecutionOptions getExecutionOptions() {
    return executionOptions;
  }

  public void setExecutionOptions(MultiShardExecutionOptions value) {
    executionOptions = value;
  }

  public RetryPolicy getRetryPolicy() {
    return retryPolicy;
  }

  public void setRetryPolicy(RetryPolicy value) {
    retryPolicy = value;
  }

  /**
   * The ExecuteReader methods of the MultiShardStatement execute the given command statement on
   * each shard and return the concatenation (i.e. UNION ALL) of the individual results from the
   * shards in a <see cref="MultiShardResultSet"/>. The execution policy regarding result
   * completeness can be controlled by setting the <see cref="MultiShardExecutionPolicy"/>. The
   * default execution policy is to return complete results.
   *
   * @return the <see cref="MultiShardResultSet"/> instance with the overall concatenated result
   * set.
   * @throws IllegalStateException thrown if the commandText is null or empty
   * @throws TimeoutException thrown if the CommandTimeout elapsed prior to completion
   */
  public MultiShardResultSet executeQuery() throws Exception {
    // We want to return exceptions via the task so that they can be dealt with on the main thread.
    // Gotta catch 'em all. We are returning the sharded ResultSet variable via the task. We don't
    // want to dispose it. This method is part of the defined API.
    // We can't move it to a different class.
    return executeQuery(CommandBehavior.Default);
  }

  /**
   * The ExecuteReader methods of the MultiShardStatement execute the given command statement on
   * each shard and return the concatenation (i.e. UNION ALL) of the individual results from the
   * shards in a <see cref="MultiShardResultSet"/>. The execution policy regarding result
   * completeness can be controlled by setting the <see cref="MultiShardExecutionPolicy"/>. The
   * default execution policy is to return complete results.
   *
   * @param behavior specifies the <see cref="CommandBehavior"/> to use.
   * @return the <see cref="MultiShardResultSet"/> instance with the overall concatenated ResultSet.
   * @throws IllegalStateException thrown if the commandText is null or empty
   * @throws TimeoutException thrown if the CommandTimeout elapsed prior to completion
   */
  public MultiShardResultSet executeQuery(CommandBehavior behavior) {
    return executeQuery(behavior, MultiShardUtils.getSqlCommandRetryPolicy(this.retryPolicy,
        this.retryBehavior), MultiShardUtils.getSqlConnectionRetryPolicy(this.retryPolicy,
        this.retryBehavior), this.executionPolicy);
  }

  /**
   * Runs the given query against all shards and returns a reader that encompasses results from
   * them.
   * Design Principles - Commands are executed in a parallel, non-blocking manner. - Only the
   * calling thread is blocked until the command is complete against all shards.
   *
   * @param behavior Command behavior to use
   * @param commandRetryPolicy The retry policy to use when executing commands against the shards
   * @param connectionRetryPolicy The retry policy to use when connecting to shards
   * @param executionPolicy The execution policy to use
   * @return MultiShardResultSet instance that encompasses results from all shards
   * @throws IllegalStateException If the commandText is null or empty
   * @throws TimeoutException If the commandTimeout elapsed prior to completion
   * @throws MultiShardAggregateException If one or more errors occurred while executing the query
   */
  public MultiShardResultSet executeQuery(CommandBehavior behavior,
      RetryPolicy commandRetryPolicy,
      RetryPolicy connectionRetryPolicy,
      MultiShardExecutionPolicy executionPolicy) {
    try {
      return this.executeQueryAsync(behavior, commandRetryPolicy,
          connectionRetryPolicy, executionPolicy).call();
    } catch (Exception e) {
      e.printStackTrace();
      throw new MultiShardAggregateException(e.getMessage(), (RuntimeException) e);
    }
  }

  /**
   * The ExecuteReader methods of the MultiShardStatement execute the given command statement on
   * each shard and return the concatenation (i.e. UNION ALL) of the individual results from the
   * shards in a <see cref="MultiShardResultSet"/>. The execution policy regarding result
   * completeness can be controlled by setting the <see cref="MultiShardExecutionPolicy"/>. The
   * default execution policy is to return complete results.
   *
   * @return a task warapping the <see cref="MultiShardResultSet"/> instance with the overall
   * concatenated result set.
   * @throws IllegalStateException thrown if the commandText is null or empty, or if the specified
   * command behavior is not supported such as CloseConnection or SingleRow.
   * @throws TimeoutException thrown if the commandTimeout elapsed prior to completion.
   */
  public Callable<MultiShardResultSet> executeQueryAsync() {
    return this.executeQueryAsync(CommandBehavior.Default);
  }

  /**
   * The ExecuteReader methods of the MultiShardStatement execute the given command statement on
   * each shard and return the concatenation (i.e. UNION ALL) of the individual results from the
   * shards in a <see cref="MultiShardResultSet"/>. The execution policy regarding result
   * completeness can be controlled by setting the <see cref="MultiShardExecutionPolicy"/>. The
   * default execution policy is to return complete results.
   *
   * @param behavior Command behavior to use //@param cancellationToken Cancellation token to cancel
   * the command execution Any exceptions during command execution are conveyed via the returned
   * Task.
   * @return a task warapping the <see cref="MultiShardResultSet"/> instance with the overall
   * concatenated result set.
   * @throws IllegalStateException thrown if the commandText is null or empty, or if the specified
   * command behavior is not supported such as CloseConnection or SingleRow.
   * @throws TimeoutException thrown if the commandTimeout elapsed prior to completion.
   */
  public Callable<MultiShardResultSet> executeQueryAsync(CommandBehavior behavior) {
    return this.executeQueryAsync(behavior,
        MultiShardUtils.getSqlCommandRetryPolicy(this.getRetryPolicy(), this.getRetryBehavior()),
        MultiShardUtils.getSqlConnectionRetryPolicy(this.getRetryPolicy(), this.getRetryBehavior()),
        this.getExecutionPolicy());
  }

  /**
   * Executes the given query against all shards asynchronously.
   *
   * @param behavior Command behavior to use //@param outerCancellationToken Cancellation token to
   * cancel the command execution
   * @param commandRetryPolicy The retry policy to use when executing commands against the shards
   * @param connectionRetryPolicy The retry policy to use when connecting to shards
   * @param executionPolicy The execution policy to use
   * @return A task with a TResult that encompasses results from all shards Any exceptions during
   * command execution are conveyed via the returned Task
   * @throws IllegalStateException If the commandText is null or empty
   */
  public Callable<MultiShardResultSet> executeQueryAsync(CommandBehavior behavior,
      RetryPolicy commandRetryPolicy,
      RetryPolicy connectionRetryPolicy,
      MultiShardExecutionPolicy executionPolicy) {

    try {
      this.validateCommand(behavior);

      // Create a list of sql commands to run against each of the shards
      List<Pair<ShardLocation, Statement>> shardCommands = this.getShardCommands();

      // Don't allow a new invocation if a Cancel() is already in progress
      synchronized (cancellationLock) {
        // Set the activity id
        activityId = UUID.randomUUID();
        try (ActivityIdScope activityIdScope = new ActivityIdScope(activityId)) {
          Stopwatch stopwatch = Stopwatch.createStarted();

          log.info("MultiShardStatement.ExecuteReaderAsync; Start; Command Timeout: {};"
                  + "Command Text: {}; Execution Policy: {}", this.getCommandTimeout(),
              this.getCommandText(), this.getExecutionPolicy());

          List<Callable<LabeledResultSet>> tasks = this.getLabeledResultSetCallableList(behavior,
              shardCommands, commandRetryPolicy, connectionRetryPolicy, executionPolicy);

          try {
            return () -> {
              List<LabeledResultSet> resultSets = executeAsync(tasks.size(), tasks.stream())
                  .collect(Collectors.toList());

              stopwatch.stop();

              log.info("Complete; Execution Time: {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));

              //TODO: Make sure we have results in all resultSets.

              if ((this.getExecutionOptions().getValue()
                  & MultiShardExecutionOptions.IncludeShardNameColumn.getValue()) != 0) {
                resultSets.forEach(r -> {
                  r.setShardLabel(r.getShardLocation().getDatabase());
                });
              }

              // Hand-off the responsibility of cleanup to the MultiShardResultSet.
              return new MultiShardResultSet(resultSets);
            };
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    } catch (RuntimeException ex) {
      ex.printStackTrace();
    }
    return null;
  }

  private Stream<LabeledResultSet> executeAsync(int numberOfThreads,
      Stream<Callable<LabeledResultSet>> callables)
      throws ExecutionException, InterruptedException, TimeoutException {
    ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);

    try {
      // CompletionService allows to terminate the parallel execution if one of the treads throws
      // an exception
      CompletionService<LabeledResultSet> completionService
          = new ExecutorCompletionService<>(executorService);

      List<Future<LabeledResultSet>> futures = callables.map(completionService::submit)
          .collect(Collectors.toList());

      try {
        // Looping over the futures in order of completion: the first future to
        // complete (or fail) is returned first by .take()
        for (int i = 0; i < futures.size(); ++i) {
          completionService.take().get(this.getCommandTimeout(), TimeUnit.SECONDS);
        }
      } catch (Exception e) {
        if (this.getExecutionPolicy().equals(MultiShardExecutionPolicy.CompleteResults)) {
          //In case one callable fails, cancel all pending and executing operations.
          futures.forEach(f -> f.cancel(true));
          throw e;
        }
      }

      return futures.stream().map(future -> {
        try {
          return future.get();
        } catch (Exception e) {
          return null;
        }
      });
    } finally {
      executorService.shutdown();
    }
  }

  private List<Callable<LabeledResultSet>> getLabeledResultSetCallableList(CommandBehavior behavior,
      List<Pair<ShardLocation, Statement>> commands,
      RetryPolicy commandRetryPolicy,
      RetryPolicy connectionRetryPolicy,
      MultiShardExecutionPolicy executionPolicy) {
    List<Callable<LabeledResultSet>> shardCommandTasks = new ArrayList<>();

    commands.forEach(cmd -> shardCommandTasks.add(this.getLabeledResultSetTask(
        behavior, cmd, commandRetryPolicy, connectionRetryPolicy, executionPolicy)));

    return shardCommandTasks;
  }

  /**
   * Helper that generates a Task to return a LabaledDbDataReader rather than just a plain
   * ResultSet so that we can affiliate the shard label with the Task returned from a call to
   * Statement.ExecuteReaderAsync.
   * We are returning the LabeledDataReader via the task.  We don't want to dispose it.
   *
   * @param behavior Command behavior to use
   * @param shardStatements A tuple of the Shard and the command to be executed //@param
   * cmdCancellationMgr Manages the cancellation tokens
   * @param commandRetryPolicy The retry policy to use when executing commands against the shards
   * @param connectionRetryPolicy The retry policy to use when connecting to shards
   * @param executionPolicy The execution policy to use
   * @return A Task that will return a LabaledDbDataReader.
   *
   * We should be able to tap into this code to trap and gracefully deal with command execution
   * errors as well.
   */
  private Callable<LabeledResultSet> getLabeledResultSetTask(CommandBehavior behavior,
      Pair<ShardLocation, Statement> shardStatements,
      RetryPolicy commandRetryPolicy,
      RetryPolicy connectionRetryPolicy,
      MultiShardExecutionPolicy executionPolicy) {
    ShardLocation shard = shardStatements.getLeft();
    PreparedStatement statement = (PreparedStatement) shardStatements.getRight();
    return () -> {
      Stopwatch stopwatch = Stopwatch.createStarted();

      // Always the close connection once the reader is done
      //
      // Commented out because of VSTS BUG# 3936154: When this command behavior is enabled,
      // SqlClient seems to be running into a deadlock when we invoke a cancellation on
      // ExecuteReaderAsync(cancellationToken) with a CommandText that would lead to an error
      // (Ex. "select * from non_existant_table").
      // As a workaround, we now explicitly close the connection associated with each shard's
      // ResultSet once we are done reading through it in MultiShardResultSet.
      // Please refer to the bug to find a sample app with a repro, dump and symbols.
      //
      // behavior |= CommandBehavior.CloseConnection;
      log.info("MultiShardStatement.GetLabeledDbDataReaderTask; Starting command execution for"
          + "Shard: {}; Behavior: {}; Retry Policy: {}", shard, behavior, this.getRetryPolicy());

      //TODO: Make use of command and connection retry policies
      ResultSet resultSet = statement.executeQuery();

      stopwatch.stop();

      log.info("MultiShardStatement.GetLabeledDbDataReaderTask; Completed command execution for"
          + "Shard: {}; Execution Time: {} ", shard, stopwatch.elapsed(TimeUnit.MILLISECONDS));

      return new LabeledResultSet(resultSet, shard, statement);
    };
  }

  /**
   * Attempts to cancel an in progress <see cref="MultiShardStatement"/> and any ongoing work that
   * is performed at the shards on behalf of the command. We don't want cancel throwing any
   * exceptions. Just cancel.
   */
  public void cancel() {
    synchronized (cancellationLock) {
      try {
        FutureTask<MultiShardResultSet> currentTask = this.currentTask;

        if (currentTask != null) {
          if (isExecutionInProgress()) {
            // Call could've been made from a worker thread
            try (ActivityIdScope activityIdScope = new ActivityIdScope(activityId)) {
              log.info("MultiShardStatement.Cancel Command was canceled; Current task status: {}",
                  currentTask);

              //TODO: innerCts.Cancel(); currentTask.Wait();
            }
          }

          //Debug.Assert(currentTask.IsCompleted, "Current task should be complete.");

          // For tasks that failed or were cancelled we assume that they are already cleaned up.
          //if (currentTask.Status == TaskStatus.RanToCompletion) {
          FutureTask<MultiShardResultSet> executeReaderTask = ((currentTask instanceof FutureTask)
              ? currentTask : null);

          if (currentTask != null) {
            // Cancel all the active readers on MultiShardResultSet.
            executeReaderTask.get().close();
          }
          //}
        }
      } catch (java.lang.Exception e) { // Cancel doesn't throw any exceptions
      } finally {
        /*if (innerCts.IsCancellationRequested) {
          innerCts = new CancellationTokenSource();
        }*/
      }
    }
  }

  /**
   * Dispose off any unmanaged/managed resources held. We purposely want to ignore exceptions.
   */
  protected void dispose(boolean disposing) {
    if (!isDisposed) {
      if (disposing) {
        try {
          // Cancel any commands that are in progress
          this.cancel();

          // Close any open connections
          this.getConnection().close();
        } catch (RuntimeException | IOException e) { // Ignore any exceptions
        }

        // Dispose the cancellation token source
        /*TODO: try (innerCts) { }*/

        isDisposed = true;

        log.info("MultiShardStatement.Dispose", "Command disposed");
      }
    }
  }

  /**
   * Resets the <see cref="commandTimeout"/> property to its default value.
   */
  public void resetCommandTimeout() {
    this.commandTimeout = DEFAULT_COMMAND_TIMEOUT;
  }

  /**
   * Resets the <see cref="CommandTimeoutPerShard"/> property to its default value.
   */
  public void resetCommandTimeoutPerShard() {
    this.commandTimeoutPerShard = DEFAULT_COMMAND_TIMEOUT_PER_SHARD;
  }

  private void validateCommand(CommandBehavior behavior) {
    // Enforce only one async invocation at a time
    if (isExecutionInProgress()) {
      IllegalStateException ex = new IllegalStateException("The command execution cannot proceed"
          + "due to a pending asynchronous operation already in progress.");

      log.error("MultiShardStatement.ValidateCommand; Exception {}; Current Task Status: {}", ex,
          currentTask);

      throw ex;
    }

    // Make sure command text is valid
    if (StringUtilsLocal.isNullOrEmpty(this.commandText.trim())) {
      throw new IllegalStateException("CommandText cannot be null");
    }

    // Validate the command behavior
    validateCommandBehavior(behavior);

    // Validate the parameters
  }

  private void validateCommandBehavior(CommandBehavior cmdBehavior) {
    int value = cmdBehavior.getValue();
    if (((value & CommandBehavior.CloseConnection.getValue()) != 0)
        || ((value & CommandBehavior.SingleResult.getValue()) != 0)
        || ((value & CommandBehavior.SingleRow.getValue()) != 0)) {
      throw new UnsupportedOperationException(String.format("CommandBehavior %1$s is not supported",
          cmdBehavior));
    }
  }

  /**
   * Whether execution is already in progress against this command instance.
   *
   * @return True if execution is in progress
   */
  private boolean isExecutionInProgress() {
    FutureTask<MultiShardResultSet> currentTask = this.currentTask;
    return currentTask != null && !currentTask.isDone();
  }

  /**
   * Creates a list of commands to be executed against the shards associated with the connection.
   *
   * @return Pairs of shard locations and associated commands.
   */
  private List<Pair<ShardLocation, Statement>> getShardCommands() {
    return this.connection.getShardConnections().stream().map(sc -> {
      try {
        Statement statement = sc.getRight().prepareStatement(this.commandText);
        statement.setQueryTimeout(this.getCommandTimeoutPerShard());
        return new ImmutablePair<>(sc.getLeft(), statement);
      } catch (SQLException e) {
        e.printStackTrace();
        return null;
      }
    }).collect(Collectors.toList());
  }

  @Override
  public void close() throws Exception {
    dispose(true);
  }
}
