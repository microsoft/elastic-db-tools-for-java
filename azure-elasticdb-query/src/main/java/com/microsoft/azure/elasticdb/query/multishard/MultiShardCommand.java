package com.microsoft.azure.elasticdb.query.multishard;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.google.common.base.Stopwatch;
import com.microsoft.azure.elasticdb.core.commons.helpers.Event;
import com.microsoft.azure.elasticdb.core.commons.helpers.EventHandler;
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
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Complements the <see cref="MultiShardConnection"/> with a command object similar to the triad of
 * <see cref="Connection"/>, <see cref="Statement"/>, and <see cref="ResultSet"/>. The <see
 * cref="MultiShardCommand"/> takes a T-SQL command statement as its input and executes the command
 * across its collection of shards specified by its corresponding <see
 * cref="MultiShardConnection"/>. The results from processing the <see cref="MultiShardCommand"/>
 * are made available through the execute methods and the <see cref="MultiShardDataReader"/>.
 * Purpose: Complements the MultiShardConnection and abstracts away the work of running a given
 * command against multiple shards Notes: This class is NOT thread-safe. Since the Sync API
 * internally invokes the async API, connections to shards with connection string property "context
 * connection = true" are not supported. Transaction semantics are not supported.
 */
public final class MultiShardCommand implements AutoCloseable {
  ///#region Global Vars

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
   * The event handler invoked when execution has begun on a given shard.
   */
  public Event<EventHandler<ShardExecutionEventArgs>> ShardExecutionBegan = new Event<>();
  /**
   * The event handler invoked when execution has successfully completed on a given shard or its
   * shard-specific <see cref="IDataReader"/> has been returned.
   */
  public Event<EventHandler<ShardExecutionEventArgs>> ShardExecutionSucceeded = new Event<>();
  /**
   * The event handler invoked when execution on a given shard has faulted. This handler is only
   * invoked on exceptions for which execution could not be retried further as a result of
   * the exception's non-transience or as a result of the chosen <see cref="RetryBehavior"/>.
   */
  public Event<EventHandler<ShardExecutionEventArgs>> ShardExecutionFaulted = new Event<>();
  /**
   * The event handler invoked when execution on a given shard is canceled, either explicitly via
   * the provided <see cref="CancellationToken"/> or implicitly as a result of the chosen
   * <see cref="MultiShardExecutionPolicy"/>.
   */
  public Event<EventHandler<ShardExecutionEventArgs>> ShardExecutionCanceled = new Event<>();

  ///#endregion

  ///#region Ctors

  // The SqlCommand underlies the object we will return.  We don't want to dispose it. The point of
  // this c-tor is to allow the user to specify whatever sql text they wish.
  /**
   * The event handler invoked when ExecuteDataReader on a certain shard has successfully returned
   * a reader. This is an internal-only method, and differs from ShardExecutionSucceeded in that
   * it is invoked BEFORE the reader is added to the MultiShardDataReader; this adding is rife
   * with side effects that are difficult to isolate.
   */
  public Event<EventHandler<ShardExecutionEventArgs>> ShardExecutionReaderReturned = new Event<>();

  ///#endregion

  ///#region Instance Factories
  /**
   * The sql command to be executed against shards.
   */
  private String commandText;
  /**
   * Task associated with current command invocation.
   */
  private FutureTask<MultiShardDataReader> currentCommandTask = null;

  ///#endregion

  ///#region Properties

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
   *
   *
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

  /**
   * Creates an instance of this class.
   *
   * @param connection The connection to shards
   * @param commandText The sql command to execute against the shards
   * @param commandTimeout Command timeout for given commandText to be run against ALL shards
   */
  private MultiShardCommand(MultiShardConnection connection, String commandText,
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
  public static MultiShardCommand create(MultiShardConnection connection, String commandText) {
    return MultiShardCommand.create(connection, commandText,
        MultiShardCommand.DEFAULT_COMMAND_TIMEOUT);
  }

  /**
   * Instance constructor of this class. Default command type is text.
   *
   * @param connection The connection to shards
   * @param commandText The command text to execute against shards
   * @param commandTimeout Command timeout for given commandText to be run against ALL shards
   * @return An Instance of this class.
   */
  public static MultiShardCommand create(MultiShardConnection connection, String commandText,
      int commandTimeout) {
    return new MultiShardCommand(connection, commandText, commandTimeout);
  }

  /**
   * Terminates any active commands/readers for scenarios where we fail the request due to
   * strict execution policy or cancellation.
   *
   * @param readerTasks Collection of reader tasks associated with execution across all shards.
   */
  private static void terminateActiveCommands(List<FutureTask<LabeledDbDataReader>> readerTasks)
      throws ExecutionException, InterruptedException {
    for (int i = 0; i < readerTasks.size(); i++) {
      //TODO: if (readerTasks.get(i).Status == TaskStatus.RanToCompletion) {
        /*Debug.Assert(readerTasks[i].Result != null,
            "Must have a LabeledDbDataReader if task finished.");*/
      LabeledDbDataReader labeledReader = readerTasks.get(i).get();

      // This is a candidate for closing since we are in a faulted state.
        /*Debug.Assert(labeledReader.ResultSet != null, "Expecting reader for completed task.");*/

      try {
        /*try (labeledReader.Command) {
          try (labeledReader.ResultSet) {
            // Invoke cancellation before closing the reader. This is safe from deadlocks that
            // arise potentially due to parallel Cancel and Close calls because this is the only
            // thread that will be responsible for cleanup.
            labeledReader.Command.Cancel();
            labeledReader.ResultSet.Close();
          }
        }*/
      } catch (RuntimeException e) {
        // Catch everything for Cancel/Close.
      }
      //}
    }
  }

  private static void validateCommandBehavior(CommandBehavior cmdBehavior) {
    int value = cmdBehavior.getValue();
    if (((value & CommandBehavior.CloseConnection.getValue()) != 0)
        || ((value & CommandBehavior.SingleResult.getValue()) != 0)
        || ((value & CommandBehavior.SingleRow.getValue()) != 0)) {
      throw new UnsupportedOperationException(String.format("CommandBehavior %1$s is not supported",
          cmdBehavior));
    }
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
    return commandTimeout;
  }

  /**
   * This property controls the timeout for running a command against individual shards.
   */
  public int getCommandTimeoutPerShard() {
    return this.commandTimeoutPerShard;
  }

  /**
   * This property controls the timeout for running a command against individual shards.
   */
  public void setCommandTimeoutPerShard(int value) {
    if (value < 0) {
      this.commandTimeoutPerShard = DEFAULT_COMMAND_TIMEOUT_PER_SHARD;
    } else {
      this.commandTimeoutPerShard = value;
    }
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

  ///#endregion Properties

  ///#region APIs

  ///#region ExecuteReader Methods

  ///#region Synchronous Methods

  public RetryPolicy getRetryPolicy() {
    return retryPolicy;
  }

  public void setRetryPolicy(RetryPolicy value) {
    retryPolicy = value;
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
  ///#endregion

  ///#region Async Methods

  /**
   * The ExecuteReader methods of the MultiShardCommand execute the given command statement on each
   * shard and return the concatenation (i.e. UNION ALL) of the individual results from the shards
   * in a <see cref="MultiShardDataReader"/>. The execution policy regarding result completeness can
   * be controlled by setting the <see cref="MultiShardExecutionPolicy"/>. The default execution
   * policy is to return complete results.
   *
   * @return a task warapping the <see cref="MultiShardDataReader"/> instance with the overall
   * concatenated result set.
   * @throws IllegalStateException thrown if the commandText is null or empty, or if the
   * specified command behavior is not supported such as CloseConnection or SingleRow.
   * @throws System.TimeoutException thrown if the commandTimeout elapsed prior to completion. Any
   * exceptions during command execution are conveyed via the returned Task.
   */
  /*public FutureTask<MultiShardDataReader> executeReaderAsync() {
    return this.executeReaderAsync(CancellationToken.None);
  }*/

  /**
   * The ExecuteReader methods of the MultiShardCommand execute the given command statement on each
   * shard and return the concatenation (i.e. UNION ALL) of the individual results from the shards
   * in a <see cref="MultiShardDataReader"/>. The execution policy regarding result completeness can
   * be controlled by setting the <see cref="MultiShardExecutionPolicy"/>. The default execution
   * policy is to return complete results.
   *
   * @param behavior specifies the <see cref="CommandBehavior"/> to use.
   * @return the <see cref="MultiShardDataReader"/> instance with the overall concatenated result
   * set.
   * @throws IllegalStateException thrown if the commandText is null or empty //* @throws
   * TimeoutException thrown if the CommandTimeout elapsed prior to completion
   */
  public MultiShardDataReader executeReader(CommandBehavior behavior) {
    return executeReader(behavior, MultiShardUtils.getSqlCommandRetryPolicy(this.retryPolicy,
        this.retryBehavior), MultiShardUtils.getSqlConnectionRetryPolicy(this.retryPolicy,
        this.retryBehavior), this.executionPolicy);
  }

  /**
   * - Runs the given query against all shards and returns
   * a reader that encompasses results from them.
   *
   * Design Principles
   * - Commands are executed in a parallel, non-blocking manner.
   * - Only the calling thread is blocked until the command is complete against all shards.
   *
   * @param behavior Command behavior to use
   * @param commandRetryPolicy The retry policy to use when executing commands against the shards
   * @param connectionRetryPolicy The retry policy to use when connecting to shards
   * @param executionPolicy The execution policy to use
   * @return MultiShardDataReader instance that encompasses results from all shards
   * @throws IllegalStateException If the commandText is null or empty //@throws
   * System.TimeoutException If the commandTimeout elapsed prior to completion
   * @throws MultiShardAggregateException If one or more errors occured while executing the command
   */
  public MultiShardDataReader executeReader(CommandBehavior behavior,
      RetryPolicy commandRetryPolicy,
      RetryPolicy connectionRetryPolicy,
      MultiShardExecutionPolicy executionPolicy) {
    try {
      return this.executeReaderAsync(behavior, /*CancellationToken.None, */commandRetryPolicy,
          connectionRetryPolicy, executionPolicy).get();
    } catch (RuntimeException | InterruptedException | ExecutionException e) {
      e.printStackTrace();
      throw new MultiShardAggregateException(e.getMessage(), (RuntimeException) e);
    }
  }

  // Suppression rationale:
  //   We want to return exceptions via the task so that they can be dealt with on the main thread.  Gotta catch 'em all.
  //   We are returning the shardedReader variable via the task.  We don't want to dispose it.
  //   This method is part of the defined API.  We can't move it to a different class.
  //

  /**
   * The ExecuteReader methods of the MultiShardCommand execute the given command statement on each
   * shard and return the concatenation (i.e. UNION ALL) of the individual results from the shards
   * in a <see cref="MultiShardDataReader"/>. The execution policy regarding result completeness can
   * be controlled by setting the <see cref="MultiShardExecutionPolicy"/>. The default execution
   * policy is to return complete results.
   *
   * /@param cancellationToken Cancellation token to cancel the command execution Any exceptions
   * during command execution are conveyed via the returned Task.
   *
   * @return a task warapping the <see cref="MultiShardDataReader"/> instance with the overall
   * concatenated result set.
   * @throws IllegalStateException thrown if the commandText is null or empty, or if the specified
   * command behavior is not supported such as CloseConnection or SingleRow. //@throws
   * System.TimeoutException thrown if the commandTimeout elapsed prior to completion.
   */
  public FutureTask<MultiShardDataReader> executeReaderAsync(/*CancellationToken cancellationToken*/) {
    return this.executeReaderAsync(CommandBehavior.Default/*, cancellationToken*/);
  }

  ///#endregion

  /**
   * The ExecuteReader methods of the MultiShardCommand execute the given command statement on each
   * shard and return the concatenation (i.e. UNION ALL) of the individual results from the shards
   * in a <see cref="MultiShardDataReader"/>. The execution policy regarding result completeness can
   * be controlled by setting the <see cref="MultiShardExecutionPolicy"/>. The default execution
   * policy is to return complete results.
   *
   * @param behavior Command behavior to use //@param cancellationToken Cancellation token to cancel
   * the command execution Any exceptions during command execution are conveyed via the returned
   * Task.
   * @return a task warapping the <see cref="MultiShardDataReader"/> instance with the overall
   * concatenated result set.
   * @throws IllegalStateException thrown if the commandText is null or empty, or if the specified
   * command behavior is not supported such as CloseConnection or SingleRow. //@throws
   * System.TimeoutException thrown if the commandTimeout elapsed prior to completion.
   */
  public FutureTask<MultiShardDataReader> executeReaderAsync(CommandBehavior behavior/*,
      CancellationToken cancellationToken*/) {
    return this.executeReaderAsync(behavior, /*cancellationToken,*/
        MultiShardUtils.getSqlCommandRetryPolicy(this.getRetryPolicy(), this.getRetryBehavior()),
        MultiShardUtils.getSqlConnectionRetryPolicy(this.getRetryPolicy(), this.getRetryBehavior()),
        this.getExecutionPolicy());
  }

  /**
   * Executes the given query against all shards asynchronously
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
  public FutureTask<MultiShardDataReader> executeReaderAsync(CommandBehavior behavior,
      /*CancellationToken outerCancellationToken,*/
      RetryPolicy commandRetryPolicy,
      RetryPolicy connectionRetryPolicy,
      MultiShardExecutionPolicy executionPolicy) {
    //TODO
    // TaskCompletionSource<MultiShardDataReader> currentCompletion
    // = new TaskCompletionSource<MultiShardDataReader>();

    // Check if cancellation has already been requested by the user
    /*if (outerCancellationToken.IsCancellationRequested) {
      currentCompletion.SetCanceled();
      return currentCompletion.Task;
    }*/

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

          // Setup the Cancellation manager
          /*CommandCancellationManager cmdCancellationMgr = new CommandCancellationManager(
              innerCts.Token, outerCancellationToken, executionPolicy, this.getCommandTimeout());*/

          log.info("MultiShardCommand.ExecuteReaderAsync",
              "Start; Command Timeout: {0}; Command Text: {1}; Execution Policy: {2}",
              this.getCommandTimeout(), this.getCommandText(), this.getExecutionPolicy());

          FanOutTask fanOutTask = this.executeReaderAsyncInternal(behavior, shardCommands,
              /*cmdCancellationMgr,*/ commandRetryPolicy, connectionRetryPolicy, executionPolicy);

          //TODO:
          // FutureTask<MultiShardDataReader> commandTask = fanOutTask.outerTask.<FutureTask<MultiShardDataReader>>ContinueWith((t) ->
          FutureTask<MultiShardDataReader> commandTask = new FutureTask<MultiShardDataReader>(
              () -> {
                stopwatch.stop();

                String completionTrace = String.format("Complete; Execution Time: %1$s",
                    stopwatch.elapsed(TimeUnit.MILLISECONDS));

              /*switch (t.Status) {
                case TaskStatus.Faulted:*/
                // Close any active readers.
                if (this.getExecutionPolicy() == MultiShardExecutionPolicy.CompleteResults) {
                  try {
                    MultiShardCommand.terminateActiveCommands(fanOutTask.innerTasks);
                  } catch (ExecutionException | InterruptedException e) {
                    e.printStackTrace();
                  }
                }

                  /*this.handleCommandExecutionException(currentCompletion,
                      new MultiShardAggregateException(t.Exception.InnerExceptions),
                      completionTrace);
                  break;
                case TaskStatus.Canceled:*/
                // Close any active readers.
                if (this.getExecutionPolicy() == MultiShardExecutionPolicy.CompleteResults) {
                  MultiShardCommand.terminateActiveCommands(fanOutTask.innerTasks);
                }

                  /*this.handleCommandExecutionCanceled(currentCompletion, cmdCancellationMgr,
                      completionTrace);
                  break;
                case TaskStatus.RanToCompletion:*/
                try {
                  log.info("MultiShardCommand.ExecuteReaderAsync", completionTrace);

                  // If all child readers have exceptions, then aggregate the exceptions into this parent task.
                  List<MultiShardException> childExceptions = new ArrayList<>();
                  //t.Result.Select(r -> r.Exception);

                  if (childExceptions != null) {
                    // All child readers have exceptions

                    // This should only happen on PartialResults, because if we were in
                    // CompleteResults then any failed child reader should have caused
                    // the task to be in TaskStatus.Faulted
                    assert
                        this.getExecutionPolicy() == MultiShardExecutionPolicy.PartialResults;

                      /*this.HandleCommandExecutionException(currentCompletion,
                          new MultiShardAggregateException(childExceptions), completionTrace);*/
                  } else {
                    // At least one child reader has succeeded
                    boolean includeShardNameColumn = (this.getExecutionOptions().getValue()
                        & MultiShardExecutionOptions.IncludeShardNameColumn.getValue()) != 0;

                    // Hand-off the responsibility of cleanup to the MultiShardDataReader.
                    MultiShardDataReader shardedReader = new MultiShardDataReader(
                      /*this, t.Result, executionPolicy, includeShardNameColumn*/);

                    //TODO: currentCompletion.SetResult(shardedReader);
                  }
                } catch (RuntimeException ex) {
                /*HandleCommandExecutionException(currentCompletion,
                    new MultiShardAggregateException(ex));*/
                }
                  /*break;
                default:
                  currentCompletion
                      .SetException(new IllegalStateException("Unexpected task status."));
                  break;
              }*/

                return null;//TODO: currentCompletion.FutureTask;
              }/*, TaskContinuationOptions.ExecuteSynchronously).Unwrap(*/);

          currentCommandTask = commandTask;

          return commandTask;
        }
      }
    } catch (RuntimeException ex) {
      /*currentCompletion.SetException(ex);
      return currentCompletion.FutureTask;*/
      return null;
    }
  }

  private FanOutTask executeReaderAsyncInternal(CommandBehavior behavior,
      List<Pair<ShardLocation, Statement>> commands,
      /*CommandCancellationManager cancellationToken,*/
      RetryPolicy commandRetryPolicy,
      RetryPolicy connectionRetryPolicy,
      MultiShardExecutionPolicy executionPolicy) {
    List<FutureTask<LabeledDbDataReader>> shardCommandTasks = new ArrayList<>();

    commands.forEach(cmd -> shardCommandTasks.add(this.getLabeledDbDataReaderTask(
        behavior, cmd, commandRetryPolicy, connectionRetryPolicy, executionPolicy)));

    FanOutTask tempVar = new FanOutTask();
    tempVar.setOuterTask(new FutureTask<>(ArrayList::new));
    tempVar.setInnerTasks(shardCommandTasks);
    return tempVar;
  }

  /**
   * Helper that generates a Task to return a LabaledDbDataReader rather than just a plain
   * ResultSet so that we can affiliate the shard label with the Task returned from a call to
   * Statement.ExecuteReaderAsync.
   * We are returning the LabeledDataReader via the task.  We don't want to dispose it.
   *
   * @param behavior Command behavior to use
   * @param commandTuple A tuple of the Shard and the command to be executed //@param
   * cmdCancellationMgr Manages the cancellation tokens
   * @param commandRetryPolicy The retry policy to use when executing commands against the shards
   * @param connectionRetryPolicy The retry policy to use when connecting to shards
   * @param executionPolicy The execution policy to use
   * @return A Task that will return a LabaledDbDataReader.
   *
   * We should be able to tap into this code to trap and gracefully deal with command execution
   * errors as well.
   */
  private FutureTask<LabeledDbDataReader> getLabeledDbDataReaderTask(CommandBehavior behavior,
      Pair<ShardLocation, Statement> commandTuple,
      /*CommandCancellationManager cmdCancellationMgr,*/
      RetryPolicy commandRetryPolicy,
      RetryPolicy connectionRetryPolicy,
      MultiShardExecutionPolicy executionPolicy) {
    FutureTask<LabeledDbDataReader> currentCompletion = new FutureTask<LabeledDbDataReader>(null);

    ShardLocation shard = commandTuple.getLeft();
    Statement command = commandTuple.getRight();
    Stopwatch stopwatch = Stopwatch.createStarted();

    // Always the close connection once the reader is done
    //
    // Commented out because of VSTS BUG# 3936154: When this command behavior is enabled, SqlClient
    // seems to be running into a deadlock when we invoke a cancellation on
    // ExecuteReaderAsync(cancellationToken) with a CommandText that would lead to an error
    // (Ex. "select * from non_existant_table").
    // As a workaround, we now explicitly close the connection associated with each shard's
    // ResultSet once we are done reading through it in MultiShardDataReader.
    // Please refer to the bug to find a sample app with a repro, dump and symbols.
    //
    // behavior |= CommandBehavior.CloseConnection;

    log.info("MultiShardCommand.GetLabeledDbDataReaderTask; Starting command execution for"
        + "Shard: {}; Behavior: {}; Retry Policy: {}", shard, behavior, this.getRetryPolicy());

    FutureTask<Pair<ResultSet, Statement>> commandExecutionTask = commandRetryPolicy.
        <Pair<ResultSet, Statement>>executeAsync(
            () -> {
              // Execute command in the Threadpool
              //TODO: return FutureTask.Run(async() ->{
              Statement commandToExecute = command.getConnection().createStatement();

              // Open the connection if it isn't already
              this.openConnectionWithRetryAsync(commandToExecute, /*cmdCancellationMgr.Token,*/
                  connectionRetryPolicy);

              // The connection to the shard has been successfully opened and the per-shard command
              // is about to execute. Raise the ShardExecutionBegan event.
              this.onShardExecutionBegan(shard);

              ResultSet perShardReader = commandToExecute.executeQuery(commandText);

              return new ImmutablePair<>(perShardReader, commandToExecute);
              //});
            }/*, cmdCancellationMgr.Token*/);

    //TODO: return commandExecutionTask.<FutureTask<LabeledDbDataReader>>ContinueWith((t) -> {
    return new FutureTask<>(() -> {
      stopwatch.stop();

      String traceMsg = String.format(
          "Completed command execution for Shard: %1$s; Execution Time: %2$s; Task Status: %3$s",
          shard, stopwatch.elapsed(TimeUnit.MILLISECONDS), "" /*t.Status*/);

      /*switch (t.Status) {
        case TaskStatus.Faulted:*/
      MultiShardException exception = new MultiShardException(shard, new RuntimeException()
          /*t.Exception.InnerException*/);

      // Close the connection
      command.getConnection().close();

      // Workaround: SqlCommand sets the task status to Faulted if the token was
      // canceled while ExecuteReaderAsync was in progress. Interpret it and raise a canceled event instead.
      if (/*cmdCancellationMgr.Token.IsCancellationRequested*/true) {
        log.error("MultiShardCommand.GetLabeledDbDataReaderTask", exception,
            "Command was canceled. {0}", traceMsg);

        currentCompletion.cancel(false);

        // Raise the ShardExecutionCanceled event.
        this.onShardExecutionCanceled(shard);
      } else {
        log.error("MultiShardCommand.GetLabeledDbDataReaderTask", exception,
            "Command failed. {0}", traceMsg);

        if (executionPolicy == MultiShardExecutionPolicy.CompleteResults) {
          //TODO:
          // currentCompletion.SetException(exception);

          // Cancel any other tasks in-progress
          //cmdCancellationMgr.CompleteResultsCts.Cancel();
        } else {
          LabeledDbDataReader failedLabeledReader = new LabeledDbDataReader(exception, shard,
              command);

          //currentCompletion.SetResult(failedLabeledReader);
        }

        // Raise the ShardExecutionFaulted event.
        this.onShardExecutionFaulted(shard, new RuntimeException()/*t.Exception.InnerException*/);
      }
          /*break;
        case TaskStatus.Canceled:*/
      log.info("MultiShardCommand.GetLabeledDbDataReaderTask",
          "Command was canceled. {0}", traceMsg);

      command.getConnection().close();

      //TODO: currentCompletion.SetCanceled();

      // Raise the ShardExecutionCanceled event.
      this.onShardExecutionCanceled(shard);
          /*break;
        case TaskStatus.RanToCompletion:*/
      log.info("MultiShardCommand.GetLabeledDbDataReaderTask", traceMsg);

      LabeledDbDataReader labeledReader = new LabeledDbDataReader(
          new MultiShardException(), shard, command
          /*t.Result.Item1, shard, t.Result.Item2*/);

      // Raise the ShardExecutionReaderReturned event.
      this.onShardExecutionReaderReturned(shard, labeledReader);

      //TODO: currentCompletion.SetResult(labeledReader);

      // Raise the ShardExecutionSucceeded event.
      this.onShardExecutionSucceeded(shard, labeledReader);
          /*break;
        default:
          currentCompletion.SetException(new IllegalStateException("Unexpected task status.."));
          break;
      }*/

      return null; //TODO: currentCompletion.Task;
    }/*, TaskContinuationOptions.ExecuteSynchronously).Unwrap(*/);
  }

  ///#endregion ExecuteReader Methods

  private FutureTask openConnectionWithRetryAsync(Statement shardCommand,
      /*CancellationToken cancellationToken,*/
      RetryPolicy connectionRetryPolicy) throws SQLException {

    Connection shardConnection = shardCommand.getConnection();

    return connectionRetryPolicy.executeAsync(() ->
        MultiShardUtils.openShardConnectionAsync(shardConnection
          /*, cancellationToken), cancellationToken*/
        ));
  }

  /**
   * Attempts to cancel an in progress <see cref="MultiShardCommand"/> and any ongoing work that is
   * performed at the shards on behalf of the command.
   * We don't want cancel throwing any exceptions. Just cancel.
   */
  public void cancel() {
    synchronized (cancellationLock) {
      try {
        FutureTask<MultiShardDataReader> currentTask = currentCommandTask;

        if (currentTask != null) {
          if (isExecutionInProgress()) {
            // Call could've been made from a worker thread
            try (ActivityIdScope activityIdScope = new ActivityIdScope(activityId)) {
              log.info("MultiShardCommand.Cancel Command was canceled; Current task status: {}",
                  currentTask);

              //TODO: innerCts.Cancel(); currentTask.Wait();
            }
          }

          //Debug.Assert(currentTask.IsCompleted, "Current task should be complete.");

          // For tasks that failed or were cancelled we assume that they are already cleaned up.
          //if (currentTask.Status == TaskStatus.RanToCompletion) {
          FutureTask<MultiShardDataReader> executeReaderTask = ((currentTask instanceof FutureTask)
              ? currentTask : null);

          if (currentTask != null) {
            // Cancel all the active readers on MultiShardDataReader.
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

        log.info("MultiShardCommand.Dispose", "Command disposed");
      }
    }
  }

  /**
   * Resets the <see cref="commandTimeout"/> property to its default value.
   */
  public void resetCommandTimeout() {
    this.commandTimeout = DEFAULT_COMMAND_TIMEOUT;
  }

  ///#endregion APIs

  ///#region Helpers

  /**
   * Resets the <see cref="CommandTimeoutPerShard"/> property to its default value.
   */
  public void ResetCommandTimeoutPerShard() {
    this.commandTimeoutPerShard = DEFAULT_COMMAND_TIMEOUT_PER_SHARD;
  }

  private void validateCommand(CommandBehavior behavior) {
    // Enforce only one async invocation at a time
    if (isExecutionInProgress()) {
      IllegalStateException ex = new IllegalStateException("The command execution cannot proceed"
          + "due to a pending asynchronous operation already in progress.");

      log.error("MultiShardCommand.ValidateCommand; Exception {}; Current Task Status: {}", ex,
          currentCommandTask);

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

  /**
   * Whether execution is already in progress against this command instance.
   *
   * @return True if execution is in progress
   */
  private boolean isExecutionInProgress() {
    FutureTask<MultiShardDataReader> currentTask = currentCommandTask;
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
        return new ImmutablePair<>(sc.getLeft(),
            (Statement) sc.getRight().prepareStatement(commandText));
      } catch (SQLException e) {
        e.printStackTrace();
        return null;
      }
    }).collect(Collectors.toList());
  }

  ///#region Handle Exceptions
  /*private <TResult> void HandleCommandExecutionException(TaskCompletionSource<TResult> tcs,
      RuntimeException ex) {
    HandleCommandExecutionException(tcs, ex, "");
  }

  private <TResult> void HandleCommandExecutionException(TaskCompletionSource<TResult> tcs,
      RuntimeException ex, String trace) {
    // Close any open connections
    this.connection.Close();
    log.error("MultiShardCommand.ExecuteReaderAsync; Exception {}; Trace: {}", ex, trace);
    tcs.SetException(ex);
  }

  private <TResult> void HandleCommandExecutionCanceled(TaskCompletionSource<TResult> tcs,
      CommandCancellationManager cancellationMgr) {
    HandleCommandExecutionCanceled(tcs, cancellationMgr, "");
  }

  private <TResult> void HandleCommandExecutionCanceled(TaskCompletionSource<TResult> tcs,
      CommandCancellationManager cancellationMgr, String trace) {
    // Close any open connections
    this.connection.Close();

    s_tracer
        .TraceWarning("MultiShardCommand.ExecuteReaderAsync", "Command was canceled; {0}", trace);

    if (cancellationMgr.getHasTimeoutExpired()) {
      // The ConnectionTimeout elapsed
      tcs.SetException(
          new TimeoutException(String.format("Command timeout of %1$s elapsed.", commandTimeout)));
    } else {
      tcs.SetCanceled();
    }
  }*/
  ///#endregion

  ///#region Event Raisers

  /**
   * Raise the ShardExecutionBegan event.
   *
   * @param shardLocation The shard for which this event is raised.
   */
  private void onShardExecutionBegan(ShardLocation shardLocation) {
    if (ShardExecutionBegan != null) {
      ShardExecutionEventArgs args = new ShardExecutionEventArgs();
      args.setShardLocation(shardLocation);
      args.setException(null);

      try {
        //TODO: ShardExecutionBegan(this, args);
      } catch (RuntimeException e) {
        throw new MultiShardException(shardLocation, e);
      }
    }
  }

  /**
   * Raise the ShardExecutionSucceeded event.
   *
   * @param shardLocation The shard for which this event is raised.
   * @param reader The reader to pass in the associated eventArgs.
   */
  private void onShardExecutionSucceeded(ShardLocation shardLocation, LabeledDbDataReader reader) {
    if (ShardExecutionSucceeded != null) {
      ShardExecutionEventArgs args = new ShardExecutionEventArgs();
      args.setShardLocation(shardLocation);
      args.setException(null);
      args.setReader(reader);

      try {
        //TODO: ShardExecutionSucceeded(this, args);
      } catch (RuntimeException e) {
        throw new MultiShardException(shardLocation, e);
      }
    }
  }

  /**
   * Raise the ShardExecutionReaderReturned event.
   *
   * @param shardLocation The shard for which this event is raised.
   * @param reader The reader to pass in the associated eventArgs.
   */
  private void onShardExecutionReaderReturned(ShardLocation shardLocation,
      LabeledDbDataReader reader) {
    if (ShardExecutionReaderReturned != null) {
      ShardExecutionEventArgs args = new ShardExecutionEventArgs();
      args.setShardLocation(shardLocation);
      args.setException(null);
      args.setReader(reader);

      try {
        //TODO: ShardExecutionReaderReturned(this, args);
      } catch (RuntimeException e) {
        throw new MultiShardException(shardLocation, e);
      }
    }
  }

  /**
   * Raise the ShardExecutionFaulted event.
   *
   * @param shardLocation The shard for which this event is raised.
   * @param executionException The exception causing the execution on this shard to fault.
   */
  private void onShardExecutionFaulted(ShardLocation shardLocation,
      RuntimeException executionException) {
    if (ShardExecutionFaulted != null) {
      ShardExecutionEventArgs args = new ShardExecutionEventArgs();
      args.setShardLocation(shardLocation);
      args.setException(executionException);

      try {
        //TODO: ShardExecutionFaulted(this, args);
      } catch (RuntimeException e) {
        throw new MultiShardException(shardLocation, e);
      }
    }
  }

  /**
   * Raise the ShardExecutionCanceled event.
   *
   * @param shardLocation The shard for which this event is raised.
   */
  private void onShardExecutionCanceled(ShardLocation shardLocation) {
    if (ShardExecutionCanceled != null) {
      ShardExecutionEventArgs args = new ShardExecutionEventArgs();
      args.setShardLocation(shardLocation);
      args.setException(null);

      try {
        //TODO: ShardExecutionCanceled(this, args);
      } catch (RuntimeException e) {
        throw new MultiShardException(shardLocation, e);
      }
    }
  }

  ///#endregion Event Raisers

  @Override
  public void close() throws Exception {
    dispose(true);
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

    public final FutureTask<List<LabeledDbDataReader>> getOuterTask() {
      return outerTask;
    }

    public final void setOuterTask(FutureTask<List<LabeledDbDataReader>> value) {
      outerTask = value;
    }

    public final List<FutureTask<LabeledDbDataReader>> getInnerTasks() {
      return innerTasks;
    }

    public final void setInnerTasks(List<FutureTask<LabeledDbDataReader>> value) {
      innerTasks = value;
    }
  }

  ///#endregion Helpers
}
