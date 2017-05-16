package com.microsoft.azure.elasticdb.query.multishard;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.google.common.base.Stopwatch;
import com.microsoft.azure.elasticdb.core.commons.logging.ActivityIdScope;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryBehavior;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.query.exception.MultiShardAggregateException;
import com.microsoft.azure.elasticdb.query.exception.MultiShardException;
import com.microsoft.azure.elasticdb.query.logging.CommandBehavior;
import com.microsoft.azure.elasticdb.query.logging.MultiShardExecutionOptions;
import com.microsoft.azure.elasticdb.query.logging.MultiShardExecutionPolicy;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;
import java.lang.invoke.MethodHandles;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiShardCommand implements AutoCloseable {

  /**
   * Default command timeout per shard in seconds.
   */
  private static final int DEFAULT_COMMAND_TIMEOUT = 300;
  /**
   * Default command timeout in seconds.
   */
  private static final int DEFAULT_COMMAND_TIMEOUT_PER_SHARD = 30;
  /**
   * The Logger.
   */
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private MultiShardConnection connection;
  private String commandText;
  private MultiShardExecutionOptions executionOptions;
  private MultiShardExecutionPolicy executionPolicy;
  private int commandTimeout;
  private RetryPolicy retryPolicy;
  private RetryBehavior retryBehaviour;
  private FutureTask currentTask;

  /**
   * Creates an instance of this class.
   *
   * @param connection The connection to shards
   * @param commandText The command text to execute against the shards
   * @param commandTimeout Command timeout<paramref name="command"/> against ALL shards
   */
  private MultiShardCommand(MultiShardConnection connection, String commandText,
      int commandTimeout) {
    this.connection = connection;
    this.commandText = commandText;
    this.commandTimeout = commandTimeout;
  }

  /**
   * Instance constructor of this class. Default command timeout of 300 seconds is used.
   *
   * @param connection The connection to shards
   * @param commandText The command text to execute against shards
   */
  public static MultiShardCommand create(MultiShardConnection connection, String commandText) {
    return create(connection, commandText, DEFAULT_COMMAND_TIMEOUT);
  }

  /**
   * Instance constructor of this class. Default command type is text.
   *
   * @param connection The connection to shards
   * @param commandText The command text to execute against shards
   * @param commandTimeout Command timeout for given commandText to be run against ALL shards
   */
  public static MultiShardCommand create(MultiShardConnection connection, String commandText,
      int commandTimeout) {
    return new MultiShardCommand(connection, commandText, commandTimeout);
  }

  public MultiShardConnection getConnection() {
    return this.connection;
  }

  public void setConnection(MultiShardConnection connection) {
    this.connection = connection;
  }

  public String getCommandText() {
    return this.commandText;
  }

  public void setCommandText(String commandText) {
    this.commandText = commandText;
  }

  public MultiShardExecutionOptions getExecutionOptions() {
    return this.executionOptions;
  }

  public void setExecutionOptions(MultiShardExecutionOptions executionOptions) {
    this.executionOptions = executionOptions;
  }

  public MultiShardExecutionPolicy getExecutionPolicy() {
    return this.executionPolicy;
  }

  public void setExecutionPolicy(MultiShardExecutionPolicy executionPolicy) {
    this.executionPolicy = executionPolicy;
  }

  public int getCommandTimeout() {
    return this.commandTimeout;
  }

  public void setCommandTimeout(int commandTimeout) {
    this.commandTimeout = commandTimeout;
  }

  @Override
  public void close() throws Exception {
    this.connection.close();
  }

  /**
   * The ExecuteReader methods of the MultiShardCommand execute the given command statement on each
   * shard and return the concatenation (i.e. UNION ALL) of the individual results from the shards
   * in a <see cref="MultiShardDataReader"/>. The execution policy regarding result completeness can
   * be controlled by setting the <see cref="MultiShardExecutionPolicy"/>. The default execution
   * policy is to return complete results.
   *
   * @return the <see cref="MultiShardDataReader"/> instance with the overall concatenated result
   * set.
   * @throws IllegalStateException thrown if the commandText is null or empty //* @throws
   * TimeoutException thrown if the CommandTimeout elapsed prior to completion
   */
  public MultiShardDataReader executeReader() throws Exception {
    return executeReader(CommandBehavior.Default);
  }

  /**
   * The ExecuteReader methods of the MultiShardCommand execute the given command statement on each
   * shard and return the concatenation (i.e. UNION ALL) of the individual results from the shards
   * in a <see cref="MultiShardDataReader"/>. The execution policy regarding result completeness can
   * be controlled by setting the <see cref="MultiShardExecutionPolicy"/>. The default execution
   * policy is to return complete results.
   *
   * @return the <see cref="MultiShardDataReader"/> instance with the overall concatenated result
   * set.
   * @throws IllegalStateException thrown if the commandText is null or empty //* @throws
   * TimeoutException thrown if the CommandTimeout elapsed prior to completion
   */
  public MultiShardDataReader executeReader(CommandBehavior behavior) {
    return executeReader(behavior, MultiShardUtils.getSqlCommandRetryPolicy(this.retryPolicy,
        this.retryBehaviour), MultiShardUtils.getSqlConnectionRetryPolicy(this.retryPolicy,
        this.retryBehaviour), this.executionPolicy);
  }

  /**
   * The ExecuteReader methods of the MultiShardCommand execute the given command statement on each
   * shard and return the concatenation (i.e. UNION ALL) of the individual results from the shards
   * in a <see cref="MultiShardDataReader"/>. The execution policy regarding result completeness can
   * be controlled by setting the <see cref="MultiShardExecutionPolicy"/>. The default execution
   * policy is to return complete results.
   *
   * @return the <see cref="MultiShardDataReader"/> instance with the overall concatenated result
   * set.
   * @throws IllegalStateException thrown if the commandText is null or empty //* @throws
   * TimeoutException thrown if the CommandTimeout elapsed prior to completion
   */
  public MultiShardDataReader executeReader(CommandBehavior behavior, RetryPolicy cmdRetryPolicy,
      RetryPolicy connRetryPolicy, MultiShardExecutionPolicy executionPolicy) {
    try {
      return executeReaderAsync(behavior, cmdRetryPolicy, connRetryPolicy,
          executionPolicy).call();
    } catch (Exception e) {
      e.printStackTrace();
      throw new MultiShardAggregateException(e.getMessage(), (RuntimeException) e);
    }
  }

  public Callable<MultiShardDataReader> executeReaderAsync() {
    return this.executeReaderAsync(CommandBehavior.Default);
  }

  /**
   * The ExecuteReader methods of the MultiShardCommand execute the given command statement on each
   * shard and return the concatenation (i.e. UNION ALL) of the individual results from the shards
   * in a <see cref="MultiShardDataReader"/>. The execution policy regarding result completeness can
   * be controlled by setting the <see cref="MultiShardExecutionPolicy"/>. The default execution
   * policy is to return complete results.
   *
   * @return the <see cref="MultiShardDataReader"/> instance with the overall concatenated result
   * set.
   * @throws IllegalStateException thrown if the commandText is null or empty //* @throws
   * TimeoutException thrown if the CommandTimeout elapsed prior to completion
   */
  public Callable<MultiShardDataReader> executeReaderAsync(CommandBehavior behavior) {
    return executeReaderAsync(behavior, MultiShardUtils.getSqlCommandRetryPolicy(this.retryPolicy,
        this.retryBehaviour), MultiShardUtils.getSqlConnectionRetryPolicy(this.retryPolicy,
        this.retryBehaviour), this.executionPolicy);
  }

  /**
   * The ExecuteReader methods of the MultiShardCommand execute the given command statement on each
   * shard and return the concatenation (i.e. UNION ALL) of the individual results from the shards
   * in a <see cref="MultiShardDataReader"/>. The execution policy regarding result completeness can
   * be controlled by setting the <see cref="MultiShardExecutionPolicy"/>. The default execution
   * policy is to return complete results.
   *
   * @return the <see cref="MultiShardDataReader"/> instance with the overall concatenated result
   * set.
   * @throws IllegalStateException thrown if the commandText is null or empty //* @throws
   * TimeoutException thrown if the CommandTimeout elapsed prior to completion
   */
  public Callable<MultiShardDataReader> executeReaderAsync(CommandBehavior behavior,
      RetryPolicy cmdRetryPolicy,
      RetryPolicy connRetryPolicy, MultiShardExecutionPolicy executionPolicy) {
    return () -> {
      this.validateCommand(behavior);
      List<Pair<ShardLocation, Statement>> shardCommands = this.getShardDbCommands();
      //TODO: synchronized (cancellationLock){
      try (ActivityIdScope activityId = new ActivityIdScope(UUID.randomUUID())) {
        Stopwatch stopwatch = Stopwatch.createStarted();

        //TODO: Setup Cancellation Manager

        log.info("MultiShardCommand.ExecuteReaderAsync; Start; Command Timeout: {};"
                + "Command Text: {}; Execution Policy: {}", this.commandTimeout, this.commandText,
            executionPolicy);

        FanOutTask fanOutTask = this.executeReaderAsyncInternal(behavior, shardCommands,
            cmdRetryPolicy, connRetryPolicy, executionPolicy);

        FutureTask<List<LabeledDbDataReader>> commandTask = fanOutTask.getOuterTask();
      }
      //}
      return new MultiShardDataReader();
    };
  }

  private FanOutTask executeReaderAsyncInternal(CommandBehavior behavior,
      List<Pair<ShardLocation, Statement>> shardCommands, RetryPolicy cmdRetryPolicy,
      RetryPolicy connRetryPolicy, MultiShardExecutionPolicy executionPolicy) {

    List<FutureTask<LabeledDbDataReader>> shardCommandTasks = new ArrayList<>();

    shardCommands.forEach(cmd -> shardCommandTasks.add(this.getLabeledDbDataReaderTask(
        behavior, cmd, cmdRetryPolicy, connRetryPolicy, executionPolicy)));

    return new FanOutTask(this.getFanOutOuterTask(shardCommandTasks), shardCommandTasks);
  }

  private FutureTask<List<LabeledDbDataReader>> getFanOutOuterTask(
      List<FutureTask<LabeledDbDataReader>> shardCommandTasks) {

    //New Async Operation to continue after execution of the previous batch
    return new FutureTask<>(() -> {
      //stopwatch.stop();

      /*String logInfo = StringUtilsLocal.formatInvariant("Completed command execution for"
              + "Shard: {}; Execution Time: {}; Task Status: {}", shard,
          stopwatch.elapsed(TimeUnit.MILLISECONDS), "" *//*Previous Task Status*//*);*/

      //Switch based on previous task status
      //If previous task was faulted

      // Close any active readers.
      if (this.getExecutionPolicy().equals(MultiShardExecutionPolicy.CompleteResults)) {
        //MultiShardCommand.terminateActiveCommands(fanOutTask.InnerTasks);
      }

      //If previous task ran till completion
      try {
        log.info("MultiShardCommand.ExecuteReaderAsync");

        //If all child readers have exceptions, then aggregate the exceptions into the parent.
        List<MultiShardException> childExceptions = new ArrayList<>();
        //t.Result.Select(r -> r.Exception);

        if (childExceptions != null) {
          // All child readers have exceptions

          // This should only happen on PartialResults, because if we were in
          // CompleteResults then any failed child reader should have caused
          // the task to be in TaskStatus.Faulted
          assert this.getExecutionPolicy() == MultiShardExecutionPolicy.PartialResults;

        } else {
          // At least one child reader has succeeded
          boolean includeShardNameColumn = (this.getExecutionOptions().getValue()
              & MultiShardExecutionOptions.IncludeShardNameColumn.getValue()) != 0;

          // Hand-off the responsibility of cleanup to the MultiShardDataReader.
          MultiShardDataReader shardedReader = new MultiShardDataReader(/*this, t.Result,
                executionPolicy, includeShardNameColumn*/);

          //currentTask.SetResult(shardedReader);
        }
      } catch (RuntimeException e) {
        //HandleCommandExecutionException(currentTask, new MultiShardAggregateException(e));
      }

      return null;
    });
  }

  private FutureTask<LabeledDbDataReader> getLabeledDbDataReaderTask(CommandBehavior behavior,
      Pair<ShardLocation, Statement> cmd, RetryPolicy cmdRetryPolicy, RetryPolicy connRetryPolicy,
      MultiShardExecutionPolicy executionPolicy) {

    ShardLocation shard = cmd.getLeft();
    Statement statement = cmd.getRight();
    Stopwatch stopwatch = Stopwatch.createStarted();

    log.info("MultiShardCommand.GetLabeledDbDataReaderTask; Starting command execution for"
        + "Shard: {}; Behavior: {}; Retry Policy: {}", shard, behavior, this.retryPolicy);

    try {
      //TODO: cmdRetryPolicy.executeAction(() -> (Callable) () -> {

      // The connection to the shard has been successfully opened and the per-shard command is
      // about to execute. Raise the ShardExecutionBegan event.
      //TODO: this.OnShardExecutionBegan(shard);

      if (statement.execute(this.commandText)) {
        Pair<ResultSet, Statement> returnValue = new ImmutablePair<>(statement.getResultSet(),
            statement);
      }
      /*}
      return ;
    });

    return ;*/

      return new FutureTask<LabeledDbDataReader>(() -> {
        stopwatch.stop();

        String traceMsg = String.format(
            "Completed command execution for Shard: %1$s; Execution Time: %2$s; Task Status: %3$s",
            shard, stopwatch.elapsed(TimeUnit.MILLISECONDS), "" /*Task Status*/);

        /*switch (t.Status) {
          case TaskStatus.Faulted:*/
        MultiShardException exception = new MultiShardException(/*shard,
                t.Exception.InnerException*/);

        // Close the connection
        statement.close();

        // Workaround: SqlCommand sets the task status to Faulted if the token was canceled while
        // ExecuteReaderAsync was in progress. Interpret it and raise a canceled event instead.
        //if (cmdCancellationMgr.Token.IsCancellationRequested) {
        log.error("MultiShardCommand.GetLabeledDbDataReaderTask {}; Command was canceled. {}",
            exception, traceMsg);

        currentTask.cancel(true);

        // Raise the ShardExecutionCanceled event.
        //this.OnShardExecutionCanceled(shard);
        //} else {
        log.error("MultiShardCommand.GetLabeledDbDataReaderTask {}; Command failed. {}",
            exception, traceMsg);

        if (executionPolicy == MultiShardExecutionPolicy.CompleteResults) {
          //currentTask.SetException(exception);

          // Cancel any other tasks in-progress
          //cmdCancellationMgr.CompleteResultsCts.Cancel();
        } else {
          LabeledDbDataReader failedLabeledReader = new LabeledDbDataReader(exception, shard,
              statement);

          //currentTask.SetResult(failedLabeledReader);
        }

        // Raise the ShardExecutionFaulted event.
        //this.OnShardExecutionFaulted(shard, t.Exception.InnerException);
        //}
        /*break;
        case TaskStatus.Canceled:*/
        log.info("MultiShardCommand.GetLabeledDbDataReaderTask Command was canceled. {}", traceMsg);

        statement.close();

        currentTask.cancel(true);

        // Raise the ShardExecutionCanceled event.
        //this.OnShardExecutionCanceled(shard);

        /*break;
        case TaskStatus.RanToCompletion:*/
        log.info("MultiShardCommand.GetLabeledDbDataReaderTask", traceMsg);

        LabeledDbDataReader labeledReader = new LabeledDbDataReader(shard, statement);

        // Raise the ShardExecutionReaderReturned event.
        //this.OnShardExecutionReaderReturned(shard, labeledReader);

        //currentTask.SetResult(labeledReader);

        // Raise the ShardExecutionSucceeded event.
        //this.OnShardExecutionSucceeded(shard, labeledReader);

        /*break;
        default:
          currentTask.SetException(new IllegalStateException("Unexpected task status.."));
          break;
        }*/

        return null;//currentTask;
      }/*, TaskContinuationOptions.ExecuteSynchronously).Unwrap(*/);
    } catch (SQLException ex) {
      ex.printStackTrace();
    }
    return null;
  }

  private List<Pair<ShardLocation, Statement>> getShardDbCommands() {
    return this.connection.getShardConnections().stream().map(p -> {
      try {
        return new ImmutablePair<>(p.getLeft(),
            (Statement) p.getRight().prepareStatement(this.commandText));
      } catch (SQLException e) {
        e.printStackTrace();
        return null;
      }
    }).collect(Collectors.toList());
  }

  private void validateCommand(CommandBehavior behavior) {
    if (this.isExecutionInProgress()) {
      IllegalStateException ex = new IllegalStateException("The command execution cannot proceed"
          + " due to a pending asynchronous operation already in progress.");

      log.info("MultiShardCommand ValidateCommand; Current Task Status: {}; Exception: {}",
          this.currentTask.toString(), ex);

      throw ex;
    }

    if (StringUtilsLocal.isNullOrEmpty(this.commandText)) {
      throw new IllegalStateException("CommandText cannot be null");
    }

    this.validateCommandBehavior(behavior);

    this.validateParameters();
  }

  private void validateCommandBehavior(CommandBehavior behavior) {
    if (behavior == null || behavior.equals(CommandBehavior.CloseConnection)
        || behavior.equals(CommandBehavior.SingleResult)
        || behavior.equals(CommandBehavior.SingleRow)) {
      throw new UnsupportedOperationException(StringUtilsLocal.formatInvariant("CommandBehavior"
          + "%s is not supported", behavior));
    }
  }

  private void validateParameters() {
    //TODO
  }

  private boolean isExecutionInProgress() {
    FutureTask currentTask = this.currentTask;

    return currentTask != null && !currentTask.isDone();
  }

  /**
   * Encapsulates data structures representing state of tasks executing across all the shards.
   */
  private static class FanOutTask {

    /**
     * Parent task of all per-shard tasks.
     */
    private FutureTask<List<LabeledDbDataReader>> outerTask;
    /**
     * Collection of inner tasks that run against each shard.
     */
    private List<FutureTask<LabeledDbDataReader>> innerTasks;

    public FanOutTask(FutureTask<List<LabeledDbDataReader>> outerTask,
        List<FutureTask<LabeledDbDataReader>> innerTasks) {
      this.outerTask = outerTask;
      this.innerTasks = innerTasks;
    }

    public final FutureTask<List<LabeledDbDataReader>> getOuterTask() {
      return outerTask;
    }

    public final List<FutureTask<LabeledDbDataReader>> getInnerTasks() {
      return innerTasks;
    }
  }
}
