package com.microsoft.azure.elasticdb.query.multishard;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.google.common.base.Stopwatch;
import com.microsoft.azure.elasticdb.core.commons.helpers.Event;
import com.microsoft.azure.elasticdb.core.commons.helpers.EventHandler;
import com.microsoft.azure.elasticdb.core.commons.logging.ActivityIdScope;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryBehavior;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.query.exception.MultiShardAggregateException;
import com.microsoft.azure.elasticdb.query.exception.MultiShardException;
import com.microsoft.azure.elasticdb.query.exception.MultiShardResultSetClosedException;
import com.microsoft.azure.elasticdb.query.exception.MultiShardResultSetInternalException;
import com.microsoft.azure.elasticdb.query.exception.MultiShardSchemaMismatchException;
import com.microsoft.azure.elasticdb.query.logging.CommandBehavior;
import com.microsoft.azure.elasticdb.query.logging.MultiShardExecutionOptions;
import com.microsoft.azure.elasticdb.query.logging.MultiShardExecutionPolicy;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlConnectionStringBuilder;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;
import com.microsoft.sqlserver.jdbc.SQLServerDataTable;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;

/**
 * Complements the <see cref="MultiShardConnection"/> with a command object similar to the triad of <see cref="Connection"/>, <see cref="Statement"/>,
 * and <see cref="ResultSet"/>. The <see cref="MultiShardStatement"/> takes a T-SQL command statement as its input and executes the command across its
 * collection of shards specified by its corresponding <see cref="MultiShardConnection"/>. The results from processing the
 * <see cref="MultiShardStatement"/> are made available through the execute methods and the <see cref="MultiShardResultSet"/>. Purpose: Complements
 * the MultiShardConnection and abstracts away the work of running a given command against multiple shards Notes: This class is NOT thread-safe. Since
 * the Sync API internally invokes the async API, connections to shards with connection string property "context connection = true" are not supported.
 * Transaction semantics are not supported.
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
     * The event handler invoked when execution has begun on a given shard.
     */
    public Event<EventHandler<ShardExecutionEventArgs>> shardExecutionBegan = new Event<>();

    /**
     * The event handler invoked when execution has successfully completed on a given shard or its shard-specific ResultSet has been returned.
     */
    public Event<EventHandler<ShardExecutionEventArgs>> shardExecutionSucceeded = new Event<>();

    /**
     * The event handler invoked when execution on a given shard has faulted. This handler is only invoked on exceptions for which execution could not
     * be retried further as a result of the exception's non-transience or as a result of the chosen <see cref="RetryBehavior"/>.
     */
    public Event<EventHandler<ShardExecutionEventArgs>> shardExecutionFaulted = new Event<>();

    /**
     * The event handler invoked when execution on a given shard is canceled, either explicitly via the provided <see cref="CancellationToken"/> or
     * implicitly as a result of the chosen <see cref="MultiShardExecutionPolicy"/>.
     */
    public Event<EventHandler<ShardExecutionEventArgs>> shardExecutionCanceled = new Event<>();

    /**
     * The event handler invoked when executeQuery on a certain shard has successfully returned a reader. This is an internal-only method, and differs
     * from shardExecutionSucceeded in that it is invoked BEFORE the reader is added to the MultiShardResultSet; this adding is rife with side effects
     * that are difficult to isolate.
     */
    public Event<EventHandler<ShardExecutionEventArgs>> shardExecutionReaderReturned = new Event<>();

    /**
     * The sql command to be executed against shards.
     */
    private String commandText;

    /**
     * Task associated with current command invocation.
     */
    private Future<LabeledResultSet> currentTask = null;

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
     * The retry behavior for detecting transient faults that could occur when connecting to and executing commands against individual shards. The
     * <see cref="Microsoft.Azure.SqlDatabase.ElasticScale.RetryBehavior.DefaultRetryBehavior"/> is the default.
     */
    private RetryBehavior retryBehavior;

    /**
     * The execution policy to use when executing commands against shards. Through this policy, users can control whether complete results are
     * required, or whether partial results are acceptable.
     */
    private MultiShardExecutionPolicy executionPolicy;

    /**
     * Gets the current instance of the <see cref="MultiShardConnection"/> associated with this command.
     */
    private MultiShardConnection connection;

    /**
     * Gets or sets options that control how the command is executed. For instance, you can use this to include the shard name as an additional column
     * into the result.
     */
    private MultiShardExecutionOptions executionOptions;

    /**
     * The retry policy to use when connecting to and executing commands against individual shards.
     */
    private RetryPolicy retryPolicy;

    /**
     * ResultSetMetaData stored as a template to compare schema.
     */
    private ResultSetMetaData schemaComparisonTemplate;

    /**
     * List of objects required to set parameters to statements before execution of command text.
     */
    private List<Triple<Integer, Integer, Object[]>> parameters;

    /**
     * Creates an instance of this class.
     *
     * @param connection
     *            The connection to shards
     * @param commandText
     *            The sql command to execute against the shards
     * @param commandTimeout
     *            Command timeout for given commandText to be run against ALL shards
     */
    private MultiShardStatement(MultiShardConnection connection,
            String commandText,
            int commandTimeout) {
        this.setConnection(connection);
        this.commandTimeout = commandTimeout;
        this.commandText = commandText;

        // Set defaults
        this.setRetryPolicy(RetryPolicy.getDefaultRetryPolicy());
        this.setRetryBehavior(RetryBehavior.getDefaultRetryBehavior());
        this.setExecutionPolicy(MultiShardExecutionPolicy.CompleteResults);
        this.setExecutionOptions(MultiShardExecutionOptions.None);
    }

    /**
     * Instance constructor of this class. Default command timeout of 300 seconds is used.
     *
     * @param connection
     *            The connection to shards
     * @param commandText
     *            The command text to execute against shards
     * @return An Instance of this class.
     */
    public static MultiShardStatement create(MultiShardConnection connection,
            String commandText) {
        return MultiShardStatement.create(connection, commandText, MultiShardStatement.DEFAULT_COMMAND_TIMEOUT);
    }

    /**
     * Instance constructor of this class. Default command type is text.
     *
     * @param connection
     *            The connection to shards
     * @param commandText
     *            The command text to execute against shards
     * @param commandTimeout
     *            Command timeout for given commandText to be run against ALL shards
     * @return An Instance of this class.
     */
    public static MultiShardStatement create(MultiShardConnection connection,
            String commandText,
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
     * Time in seconds to wait for the command to be executed on ALL shards. A value of 0 indicates no wait time limit. The default is 300 seconds.
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
        return this.commandTimeoutPerShard <= 0 ? DEFAULT_COMMAND_TIMEOUT_PER_SHARD : this.commandTimeoutPerShard;
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

    private void setConnection(MultiShardConnection connection) {
        if (connection.isClosed()) {
            List<Shard> shards = connection.getShards();
            if (shards == null || shards.size() <= 0) {
                List<ShardLocation> locations = connection.getShardLocations();
                connection = new MultiShardConnection(connection.getConnectionString(), locations.toArray(new ShardLocation[locations.size()]));
            }
            else {
                connection = new MultiShardConnection(connection.getConnectionString(), shards.toArray(new Shard[shards.size()]));
            }
        }
        this.connection = connection;
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
     * Set query parameters. Currently only Table type parameter is supported.
     *
     * @param index
     *            Index of the parameter
     * @param type
     *            SQL Type of the parameter
     * @param objects
     *            An array of objects to add as parameter
     */
    public void setParameters(int index,
            int type,
            Object... objects) {
        if (this.parameters == null) {
            this.parameters = new ArrayList<>();
        }

        this.parameters.add(new ImmutableTriple<>(index, type, objects));
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

    /**
     * The ExecuteReader methods of the MultiShardStatement execute the given command statement on each shard and return the concatenation (i.e. UNION
     * ALL) of the individual results from the shards in a <see cref="MultiShardResultSet"/>. The execution policy regarding result completeness can
     * be controlled by setting the <see cref="MultiShardExecutionPolicy"/>. The default execution policy is to return complete results.
     *
     * @return the <see cref="MultiShardResultSet"/> instance with the overall concatenated result set.
     * @throws IllegalStateException
     *             thrown if the commandText is null or empty
     * @throws TimeoutException
     *             thrown if the CommandTimeout elapsed prior to completion
     */
    public MultiShardResultSet executeQuery() throws MultiShardAggregateException {
        // We want to return exceptions via the task so that they can be dealt with on the main thread.
        // Gotta catch 'em all. We are returning the sharded ResultSet variable via the task. We don't
        // want to dispose it. This method is part of the defined API.
        // We can't move it to a different class.
        return executeQuery(CommandBehavior.Default);
    }

    /**
     * The ExecuteReader methods of the MultiShardStatement execute the given command statement on each shard and return the concatenation (i.e. UNION
     * ALL) of the individual results from the shards in a <see cref="MultiShardResultSet"/>. The execution policy regarding result completeness can
     * be controlled by setting the <see cref="MultiShardExecutionPolicy"/>. The default execution policy is to return complete results.
     *
     * @param behavior
     *            specifies the <see cref="CommandBehavior"/> to use.
     * @return the <see cref="MultiShardResultSet"/> instance with the overall concatenated ResultSet.
     * @throws IllegalStateException
     *             thrown if the commandText is null or empty
     * @throws TimeoutException
     *             thrown if the CommandTimeout elapsed prior to completion
     */
    public MultiShardResultSet executeQuery(CommandBehavior behavior) throws MultiShardAggregateException {
        return executeQuery(behavior, MultiShardUtils.getSqlCommandRetryPolicy(this.retryPolicy, this.retryBehavior), this.executionPolicy);
    }

    /**
     * Runs the given query against all shards and returns a reader that encompasses results from them. Design Principles - Commands are executed in a
     * parallel, non-blocking manner. - Only the calling thread is blocked until the command is complete against all shards.
     *
     * @param behavior
     *            Command behavior to use
     * @param commandRetryPolicy
     *            The retry policy to use when executing commands against the shards
     * @param executionPolicy
     *            The execution policy to use
     * @return MultiShardResultSet instance that encompasses results from all shards
     * @throws IllegalStateException
     *             If the commandText is null or empty
     * @throws TimeoutException
     *             If the commandTimeout elapsed prior to completion
     * @throws MultiShardAggregateException
     *             If one or more errors occurred while executing the query
     */
    public MultiShardResultSet executeQuery(CommandBehavior behavior,
            RetryPolicy commandRetryPolicy,
            MultiShardExecutionPolicy executionPolicy) throws MultiShardAggregateException {
        MultiShardResultSet result;
        try {
            result = this.executeQueryAsync(behavior, commandRetryPolicy, executionPolicy).call();
        }
        catch (Exception e) {
            throw e instanceof MultiShardAggregateException ? (MultiShardAggregateException) e : new MultiShardAggregateException(e);
        }
        return result;
    }

    /**
     * The ExecuteReader methods of the MultiShardStatement execute the given command statement on each shard and return the concatenation (i.e. UNION
     * ALL) of the individual results from the shards in a <see cref="MultiShardResultSet"/>. The execution policy regarding result completeness can
     * be controlled by setting the <see cref="MultiShardExecutionPolicy"/>. The default execution policy is to return complete results.
     *
     * @return a task wrapping the <see cref="MultiShardResultSet"/> instance with the overall concatenated result set.
     * @throws IllegalStateException
     *             thrown if the commandText is null or empty, or if the specified command behavior is not supported such as CloseConnection or
     *             SingleRow.
     * @throws TimeoutException
     *             thrown if the commandTimeout elapsed prior to completion.
     */
    public Callable<MultiShardResultSet> executeQueryAsync() {
        return this.executeQueryAsync(CommandBehavior.Default);
    }

    /**
     * The ExecuteReader methods of the MultiShardStatement execute the given command statement on each shard and return the concatenation (i.e. UNION
     * ALL) of the individual results from the shards in a <see cref="MultiShardResultSet"/>. The execution policy regarding result completeness can
     * be controlled by setting the <see cref="MultiShardExecutionPolicy"/>. The default execution policy is to return complete results.
     *
     * @param behavior
     *            Command behavior to use
     * @return a task wrapping the <see cref="MultiShardResultSet"/> instance with the overall concatenated result set.
     * @throws IllegalStateException
     *             thrown if the commandText is null or empty, or if the specified command behavior is not supported such as CloseConnection or
     *             SingleRow.
     * @throws TimeoutException
     *             thrown if the commandTimeout elapsed prior to completion.
     */
    public Callable<MultiShardResultSet> executeQueryAsync(CommandBehavior behavior) {
        return this.executeQueryAsync(behavior, MultiShardUtils.getSqlCommandRetryPolicy(this.getRetryPolicy(), this.getRetryBehavior()),
                this.getExecutionPolicy());
    }

    /**
     * Executes the given query against all shards asynchronously.
     *
     * @param behavior
     *            Command behavior to use
     * @param commandRetryPolicy
     *            The retry policy to use when executing commands against the shards
     * @param executionPolicy
     *            The execution policy to use
     * @return A task with a ResultT that encompasses results from all shards Any exceptions during command execution are conveyed via the returned
     *         Task
     * @throws IllegalStateException
     *             If the commandText is null or empty
     */
    public Callable<MultiShardResultSet> executeQueryAsync(CommandBehavior behavior,
            RetryPolicy commandRetryPolicy,
            MultiShardExecutionPolicy executionPolicy) {
        this.validateCommand(behavior);

        // Create a list of sql commands to run against each of the shards
        List<Pair<ShardLocation, Statement>> shardCommands = this.getShardCommands();

        // Set the parameters, if any.
        if (this.parameters != null && this.parameters.size() > 0) {
            this.parameters.forEach(p -> shardCommands.forEach(c -> new Parameter(c.getRight(), p.getLeft(), p.getMiddle(), p.getRight()).run()));
        }

        // Don't allow a new invocation if a Cancel() is already in progress
        synchronized (cancellationLock) {
            // Set the activity id
            activityId = UUID.randomUUID();
            try (ActivityIdScope activityIdScope = new ActivityIdScope(activityId)) {
                Stopwatch stopwatch = Stopwatch.createStarted();

                log.info("MultiShardStatement.ExecuteReaderAsync; Start; Command Timeout: {};" + "Command Text: {}; Execution Policy: {}",
                        this.getCommandTimeout(), this.getCommandText(), this.getExecutionPolicy());

                List<Callable<LabeledResultSet>> tasks = this.getLabeledResultSetCallableList(behavior, shardCommands, executionPolicy,
                        commandRetryPolicy);

                return () -> {
                    List<LabeledResultSet> resultSets = executeAsync(tasks.size(), tasks.stream(), executionPolicy).collect(Collectors.toList());
                    stopwatch.stop();

                    log.info("Complete; Execution Time: {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));

                    if ((this.getExecutionOptions().getValue() & MultiShardExecutionOptions.IncludeShardNameColumn.getValue()) != 0) {
                        resultSets.forEach(r -> r.setShardLabel(r.getShardLocation().getDatabase()));
                    }

                    // Hand-off the responsibility of cleanup to the MultiShardResultSet.
                    MultiShardResultSet resultSet = new MultiShardResultSet(resultSets);

                    // Clean up schema comparison template
                    this.schemaComparisonTemplate = null;

                    // Throw exception if all result sets has exceptions
                    List<MultiShardException> exceptions = resultSet.getMultiShardExceptions();
                    if (exceptions.size() == connection.getShards().size()) {
                        throw new MultiShardAggregateException(new ArrayList<>(exceptions));
                    }

                    return resultSet;
                };
            }
        }
    }

    private List<Callable<LabeledResultSet>> getLabeledResultSetCallableList(CommandBehavior behavior,
            List<Pair<ShardLocation, Statement>> commands,
            MultiShardExecutionPolicy executionPolicy,
            RetryPolicy commandRetryPolicy) {
        List<Callable<LabeledResultSet>> shardCommandTasks = new ArrayList<>();

        commands.forEach(cmd -> shardCommandTasks.add(this.getLabeledResultSetTask(behavior, cmd, executionPolicy, commandRetryPolicy)));

        return shardCommandTasks;
    }

    /**
     * Helper that generates a Task to return a LabeledResultSet rather than just a plain ResultSet so that we can affiliate the shard label with the
     * Task returned from a call to Statement.ExecuteReaderAsync. We are returning the LabeledResultSet via the task. We don't want to dispose it.
     *
     * @param behavior
     *            Command behavior to use
     * @param shardStatements
     *            A tuple of the Shard and the command to be executed //@param cmdCancellationMgr Manages the cancellation tokens
     * @param commandRetryPolicy
     *            The retry policy to use when executing commands against the shards
     * @return A Task that will return a LabeledResultSet.
     *
     *         We should be able to tap into this code to trap and gracefully deal with command execution errors as well.
     */
    private Callable<LabeledResultSet> getLabeledResultSetTask(CommandBehavior behavior,
            Pair<ShardLocation, Statement> shardStatements,
            MultiShardExecutionPolicy executionPolicy,
            RetryPolicy commandRetryPolicy) {
        ShardLocation shard = shardStatements.getLeft();
        AtomicReference<PreparedStatement> statement = new AtomicReference<>((PreparedStatement) shardStatements.getRight());
        return () -> {
            Stopwatch stopwatch = Stopwatch.createStarted();

            log.info("MultiShardStatement.GetLabeledDbDataReaderTask; Starting command execution for" + "Shard: {}; Behavior: {}; Retry Policy: {}",
                    shard, behavior, this.getRetryPolicy());

            // The per-shard command is about to execute.
            // Raise the shardExecutionBegan event.
            this.onShardExecutionBegan(shard);

            LabeledResultSet resultSet = commandRetryPolicy.executeAction(() -> {
                try {
                    LabeledResultSet labeledReader;
                    if (statement.get().execute()) {
                        ResultSet res = statement.get().getResultSet();

                        // Validate the result set
                        MultiShardException ex = validateResultSet(res, shard);
                        if (ex != null) {
                            if (executionPolicy.equals(MultiShardExecutionPolicy.CompleteResults)) {
                                throw ex;
                            }
                            labeledReader = new LabeledResultSet(ex, shard, statement.get());
                        }
                        else {
                            labeledReader = new LabeledResultSet(res, shard, statement.get());
                        }
                        // Raise the ShardExecutionReaderReturned event.
                        this.onShardExecutionReaderReturned(shard, labeledReader);
                    }
                    else {
                        labeledReader = new LabeledResultSet(shard, statement.get());
                    }

                    return labeledReader;
                }
                catch (SQLException ex) {
                    stopwatch.stop();
                    log.info("MultiShardStatement.GetLabeledDbDataReaderTask; Command Execution Failed; " + "Execution Time: {} ",
                            stopwatch.elapsed(TimeUnit.MILLISECONDS));

                    throw new MultiShardException(shard, ex);
                }
            });

            stopwatch.stop();

            log.info("MultiShardStatement.GetLabeledDbDataReaderTask; Completed command execution for" + "Shard: {}; Execution Time: {} ", shard,
                    stopwatch.elapsed(TimeUnit.MILLISECONDS));

            // Raise the ShardExecutionSucceeded event.
            this.onShardExecutionSucceeded(shard, resultSet);

            return resultSet;
        };
    }

    private Stream<LabeledResultSet> executeAsync(int numberOfThreads,
            Stream<Callable<LabeledResultSet>> callables,
            MultiShardExecutionPolicy executionPolicy) throws SQLException, MultiShardException {
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);

        try {
            // CompletionService allows to terminate the parallel execution if one of the treads throws
            // an exception
            CompletionService<LabeledResultSet> completionService = new ExecutorCompletionService<>(executorService);

            List<Future<LabeledResultSet>> futures = callables.map(completionService::submit).collect(Collectors.toList());

            // Looping over the futures in order of completion: the first future to
            // complete (or fail) is returned first by .take()
            List<LabeledResultSet> resultSets = new ArrayList<>();
            for (int i = 0; i < futures.size(); ++i) {
                try {
                    this.currentTask = completionService.take();
                    resultSets.add(this.currentTask.get());
                }
                catch (Exception e) {
                    if (e.getCause() instanceof MultiShardException) {
                        MultiShardException ex = (MultiShardException) e.getCause();
                        ShardLocation loc = ex.getShardLocation();
                        if (this.currentTask.isCancelled()) {
                            log.info("MultiShardStatement.GetLabeledDbDataReaderTask; Command Cancelled;");

                            // Raise the shardExecutionCanceled event.
                            this.onShardExecutionCanceled(loc);
                        }
                        else {
                            log.info("MultiShardStatement.GetLabeledDbDataReaderTask; Command Failed");

                            // Raise the shardExecutionFaulted event.
                            this.onShardExecutionFaulted(loc, (Exception) e.getCause());
                        }

                        if (executionPolicy.equals(MultiShardExecutionPolicy.CompleteResults)) {
                            // In case one callable fails, cancel all pending and executing operations.
                            futures.forEach(f -> f.cancel(true));
                            throw ex;
                        }
                        resultSets.add(new LabeledResultSet(ex, loc, getConnectionForLocation(loc).prepareStatement(this.commandText)));
                    }
                }
            }

            return resultSets.stream();
        }
        finally {
            executorService.shutdown();
        }
    }

    private Connection getConnectionForLocation(ShardLocation loc) throws SQLException {
        SqlConnectionStringBuilder str = new SqlConnectionStringBuilder(this.connection.getConnectionString());
        str.setDataSource(loc.getDataSource());
        str.setDatabaseName(loc.getDatabase());
        return DriverManager.getConnection(str.toString());
    }

    private MultiShardException validateResultSet(ResultSet r,
            ShardLocation loc) throws SQLException {
        if (r.isClosed()) {
            // ResultSet is already closed. Hence adding an exception in its place.
            return new MultiShardException(loc, new MultiShardResultSetClosedException(
                    String.format("The result set for '%1$s' was closed and could not be added.", loc.getDatabase())));
        }
        ResultSetMetaData m = r.getMetaData();
        if (m == null || m.getColumnCount() == 0) {
            // ResultSet does not have proper metadata to read.
            return new MultiShardException(loc, new MultiShardResultSetInternalException(
                    String.format("The result set for '%1$s' does not have proper metadata to read and could not be added.", loc.getDatabase())));
        }
        if (this.schemaComparisonTemplate == null) {
            this.schemaComparisonTemplate = r.getMetaData();
            return null;
        }
        for (int i = 1; i <= m.getColumnCount(); i++) {
            // Get the designated column's name.
            String expectedName = this.schemaComparisonTemplate.getColumnName(i);
            String actualName = m.getColumnName(i);
            if (!Objects.equals(expectedName, actualName)) {
                return new MultiShardException(loc, new MultiShardSchemaMismatchException(loc,
                        String.format("Expected schema column name %1$s, but encountered schema column name %2$s.", expectedName, actualName)));
            }

            // Retrieves the designated column's SQL type.
            if (!Objects.equals(this.schemaComparisonTemplate.getColumnType(i), m.getColumnType(i))) {
                return new MultiShardException(loc,
                        new MultiShardSchemaMismatchException(loc,
                                String.format("Mismatched SQL type values for column %1$s. Expected: %2$s. Actual: %3$s", actualName,
                                        this.schemaComparisonTemplate.getColumnTypeName(i), m.getColumnTypeName(i))));
            }

            // Get the designated column's specified column size.
            int expectedPrecision = this.schemaComparisonTemplate.getPrecision(i);
            int actualPrecision = m.getPrecision(i);
            if (!Objects.equals(expectedPrecision, actualPrecision)) {
                return new MultiShardException(loc,
                        new MultiShardSchemaMismatchException(loc,
                                String.format("Mismatched nullability values for column %1$s. Expected: %2$s. Actual: %3$s", actualName,
                                        expectedPrecision, actualPrecision)));
            }

            // Indicates the nullability of values in the designated column.
            int expectedNullableValue = this.schemaComparisonTemplate.isNullable(i);
            int actualNullableValue = m.isNullable(i);
            if (!Objects.equals(expectedNullableValue, actualNullableValue)) {
                return new MultiShardException(loc,
                        new MultiShardSchemaMismatchException(loc,
                                String.format("Mismatched nullability values for column %1$s. Expected: %2$s. Actual: %3$s", actualName,
                                        NullableValue.forValue(expectedNullableValue), NullableValue.forValue(actualNullableValue))));
            }
        }
        return null;
    }

    private void validateCommand(CommandBehavior behavior) {
        // Enforce only one async invocation at a time
        if (isExecutionInProgress()) {
            IllegalStateException ex = new IllegalStateException(
                    "The command execution cannot proceed" + "due to a pending asynchronous operation already in progress.");

            log.error("MultiShardStatement.ValidateCommand; Exception {}; Current Task Status: {}", ex, currentTask);

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
        if (((value & CommandBehavior.CloseConnection.getValue()) != 0) || ((value & CommandBehavior.SingleResult.getValue()) != 0)
                || ((value & CommandBehavior.SingleRow.getValue()) != 0)) {
            throw new UnsupportedOperationException(String.format("CommandBehavior %1$s is not supported", cmdBehavior));
        }
    }

    /**
     * Whether execution is already in progress against this command instance.
     *
     * @return True if execution is in progress
     */
    private boolean isExecutionInProgress() {
        Future<LabeledResultSet> currentTask = this.currentTask;
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
                Connection conn = sc.getRight();
                if (conn.isClosed()) {
                    // TODO: This hack needs to be perfected. Reopening of connection is not straight forward.
                    conn = getConnectionForLocation(sc.getLeft());
                }
                Statement statement = conn.prepareStatement(this.commandText, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                statement.setQueryTimeout(this.getCommandTimeoutPerShard());
                return new ImmutablePair<>(sc.getLeft(), statement);
            }
            catch (SQLException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }).collect(Collectors.toList());
    }

    /**
     * Attempts to cancel an in progress <see cref="MultiShardStatement"/> and any ongoing work that is performed at the shards on behalf of the
     * command. We don't want cancel throwing any exceptions. Just cancel.
     */
    public void cancel() throws MultiShardException {
        synchronized (cancellationLock) {
            try {
                Future<LabeledResultSet> currentTask = this.currentTask;

                if (currentTask != null) {
                    if (isExecutionInProgress()) {
                        // Call could've been made from a worker thread
                        try (ActivityIdScope activityIdScope = new ActivityIdScope(activityId)) {
                            log.info("MultiShardStatement.Cancel Command was canceled; Current task status: {}", currentTask);

                            currentTask.cancel(true);
                        }
                    }

                    // For tasks that failed or were cancelled we assume that they are already cleaned up.
                    if (currentTask.isDone()) {
                        // Cancel all the active readers on MultiShardResultSet.
                        currentTask.get().close();
                    }
                }
            }
            catch (Exception e) {
                // Cancel doesn't throw any exceptions
            }
            finally {
                // Raise the shardExecutionCanceled event.
                this.onShardExecutionCanceled(null);
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
                }
                catch (MultiShardException | RuntimeException | IOException e) {
                    // Ignore any exceptions
                    e.printStackTrace();
                }

                isDisposed = true;

                log.info("MultiShardStatement.Dispose", "Command disposed");
            }
        }
    }

    @Override
    public void close() throws Exception {
        dispose(true);
    }

    /**
     * Raise the shardExecutionBegan event.
     *
     * @param shardLocation
     *            The shard for which this event is raised.
     */
    private void onShardExecutionBegan(ShardLocation shardLocation) throws MultiShardException {
        if (shardExecutionBegan != null) {
            ShardExecutionEventArgs args = new ShardExecutionEventArgs();
            args.setShardLocation(shardLocation);
            args.setException(null);

            try {
                shardExecutionBegan.listeners().forEach(l -> l.invoke(this, args));
            }
            catch (RuntimeException e) {
                throw new MultiShardException(shardLocation, e);
            }
        }
    }

    /**
     * Raise the shardExecutionSucceeded event.
     *
     * @param shardLocation
     *            The shard for which this event is raised.
     * @param reader
     *            The reader to pass in the associated eventArgs.
     */
    private void onShardExecutionSucceeded(ShardLocation shardLocation,
            LabeledResultSet reader) throws MultiShardException {
        if (shardExecutionSucceeded != null) {
            ShardExecutionEventArgs args = new ShardExecutionEventArgs();
            args.setShardLocation(shardLocation);
            args.setException(null);
            args.setReader(reader);

            try {
                shardExecutionSucceeded.listeners().forEach(l -> l.invoke(this, args));
            }
            catch (Exception e) {
                throw new MultiShardException(shardLocation, e);
            }
        }
    }

    /**
     * Raise the shardExecutionReaderReturned event.
     *
     * @param shardLocation
     *            The shard for which this event is raised.
     * @param reader
     *            The reader to pass in the associated eventArgs.
     */
    private void onShardExecutionReaderReturned(ShardLocation shardLocation,
            LabeledResultSet reader) throws MultiShardException {
        if (shardExecutionReaderReturned != null) {
            ShardExecutionEventArgs args = new ShardExecutionEventArgs();
            args.setShardLocation(shardLocation);
            args.setException(null);
            args.setReader(reader);

            try {
                shardExecutionReaderReturned.listeners().forEach(l -> l.invoke(this, args));
            }
            catch (Exception e) {
                throw new MultiShardException(shardLocation, e);
            }
        }
    }

    /**
     * Raise the shardExecutionFaulted event.
     *
     * @param shardLocation
     *            The shard for which this event is raised.
     * @param executionException
     *            The exception causing the execution on this shard to fault.
     */
    private void onShardExecutionFaulted(ShardLocation shardLocation,
            Exception executionException) throws MultiShardException {
        if (shardExecutionFaulted != null) {
            ShardExecutionEventArgs args = new ShardExecutionEventArgs();
            args.setShardLocation(shardLocation);
            args.setException(executionException);

            try {
                shardExecutionFaulted.listeners().forEach(l -> l.invoke(this, args));
            }
            catch (Exception e) {
                throw new MultiShardException(shardLocation, e);
            }
        }
    }

    /**
     * Raise the shardExecutionCanceled event.
     *
     * @param shardLocation
     *            The shard for which this event is raised.
     */
    private void onShardExecutionCanceled(ShardLocation shardLocation) throws MultiShardException {
        if (shardExecutionCanceled != null) {
            ShardExecutionEventArgs args = new ShardExecutionEventArgs();
            args.setShardLocation(shardLocation);
            args.setException(null);

            try {
                shardExecutionCanceled.listeners().forEach(l -> l.invoke(this, args));
            }
            catch (Exception e) {
                throw new MultiShardException(shardLocation, e);
            }
        }
    }

    private enum NullableValue {
        // The constant indicating that a column does not allow NULL values.
        columnNoNulls(0),

        // The constant indicating that a column allows NULL values.
        columnNullable(1),

        // The constant indicating that a column allows NULL values.
        columnNullableUnknown(2);

        public static final int SIZE = Integer.SIZE;
        private static java.util.HashMap<Integer, NullableValue> mappings;
        private int intValue;

        NullableValue(int value) {
            intValue = value;
            getMappings().put(value, this);
        }

        private static java.util.HashMap<Integer, NullableValue> getMappings() {
            if (mappings == null) {
                synchronized (NullableValue.class) {
                    if (mappings == null) {
                        mappings = new java.util.HashMap<>();
                    }
                }
            }
            return mappings;
        }

        public static NullableValue forValue(int value) {
            return getMappings().get(value);
        }

        public int getValue() {
            return intValue;
        }
    }

    private final class Parameter implements Runnable {

        private final Statement statement;
        private final int index;
        private final int type;
        private final Object[] objects;

        Parameter(Statement statement,
                int index,
                int type,
                Object... objects) {
            this.statement = statement;
            this.index = index;
            this.type = type;
            this.objects = objects;
        }

        @Override
        public void run() {
            SQLServerPreparedStatement stmt = (SQLServerPreparedStatement) statement;
            try {
                switch (type) {
                    // TODO: Add all types
                    case Types.STRUCT:
                        if (objects.length == 2) {
                            stmt.setStructured(index, (String) objects[0], (SQLServerDataTable) objects[1]);
                        }
                        break;
                    default:
                        throw new RuntimeException("Not Supported yet!",
                                new UnsupportedOperationException(String.format("This SQL Type (%1$s) cannot be added to the statement using this"
                                        + " method. Please add the same as an inline parameter at %2$s index.", type, index)));
                }
            }
            catch (SQLServerException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
