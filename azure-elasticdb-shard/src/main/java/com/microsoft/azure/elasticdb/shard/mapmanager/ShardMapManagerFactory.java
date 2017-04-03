package com.microsoft.azure.elasticdb.shard.mapmanager;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.logging.ILogger;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryBehavior;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import com.microsoft.azure.elasticdb.shard.utils.GlobalConstants;
import com.microsoft.azure.elasticdb.shard.utils.Version;

/**
 * Factory for <see cref="ShardMapManager"/>s facilitates the creation and management
 * of shard map manager persistent state. Use this class as the entry point to the library's
 * object hierarchy.
 */
public final class ShardMapManagerFactory {
    /**
     * The Tracer
     */
    private static ILogger getTracer() {
        return TraceHelper.Tracer;
    }

    /**
     * Creates a <see cref="ShardMapManager"/> and its corresponding storage structures in the specified SQL Server database,
     * with <see cref="ShardMapManagerCreateMode.KeepExisting"/> and <see cref="RetryBehavior.DefaultRetryBehavior"/>.
     *
     * @param connectionString Connection parameters used for creating shard map manager database.
     * @return A shard map manager object used for performing management and read operations for
     * shard maps, shards and shard mappings.
     */
    public static ShardMapManager CreateSqlShardMapManager(String connectionString) {
        return CreateSqlShardMapManager(connectionString, ShardMapManagerCreateMode.KeepExisting, RetryBehavior.DefaultRetryBehavior);
    }

    /**
     * Creates a <see cref="ShardMapManager"/> and its corresponding storage structures in the specified SQL Server database,
     * with <see cref="RetryBehavior.DefaultRetryBehavior"/>.
     *
     * @param connectionString Connection parameters used for creating shard map manager database.
     * @param createMode       Describes the option selected by the user for creating shard map manager database.
     * @return A shard map manager object used for performing management and read operations for
     * shard maps, shards and shard mappings.
     */
    public static ShardMapManager CreateSqlShardMapManager(String connectionString, ShardMapManagerCreateMode createMode) {
        return CreateSqlShardMapManager(connectionString, createMode, RetryBehavior.DefaultRetryBehavior);
    }

    /**
     * Creates a <see cref="ShardMapManager"/> and its corresponding storage structures in the specified SQL Server database,
     * with <see cref="RetryPolicy.DefaultRetryPolicy"/>.
     *
     * @param connectionString Connection parameters used for creating shard map manager database.
     * @param createMode       Describes the option selected by the user for creating shard map manager database.
     * @param targetVersion    Target version of store to create.
     */
    public static ShardMapManager CreateSqlShardMapManager(String connectionString, ShardMapManagerCreateMode createMode, Version targetVersion) {
        return CreateSqlShardMapManagerImpl(connectionString, createMode, RetryBehavior.DefaultRetryBehavior, null, targetVersion);
    }

    /**
     * Creates a <see cref="ShardMapManager"/> and its corresponding storage structures in the specified SQL Server database,
     * with <see cref="ShardMapManagerCreateMode.KeepExisting"/>.
     *
     * @param connectionString Connection parameters used for creating shard map manager database.
     * @param retryBehavior    Behavior for detecting transient exceptions in the store.
     * @return A shard map manager object used for performing management and read operations for
     * shard maps, shards and shard mappings.
     */
    public static ShardMapManager CreateSqlShardMapManager(String connectionString, RetryBehavior retryBehavior) {
        return CreateSqlShardMapManager(connectionString, ShardMapManagerCreateMode.KeepExisting, retryBehavior);
    }

    /**
     * Creates a <see cref="ShardMapManager"/> and its corresponding storage structures in the specified SQL Server database.
     *
     * @param connectionString Connection parameters used for creating shard map manager database.
     * @param createMode       Describes the option selected by the user for creating shard map manager database.
     * @param retryBehavior    Behavior for detecting transient exceptions in the store.
     * @return A shard map manager object used for performing management and read operations for
     * shard maps, shards and shard mappings.
     */
    public static ShardMapManager CreateSqlShardMapManager(String connectionString, ShardMapManagerCreateMode createMode, RetryBehavior retryBehavior) {
        return CreateSqlShardMapManagerImpl(connectionString, createMode, retryBehavior, null, GlobalConstants.GsmVersionClient);
    }

    /**
     * Creates a <see cref="ShardMapManager"/> and its corresponding storage structures in the specified SQL Server database.
     *
     * @param connectionString  Connection parameters used for creating shard map manager database.
     * @param createMode        Describes the option selected by the user for creating shard map manager database.
     * @param retryBehavior     Behavior for detecting transient exceptions in the store.
     * @param retryEventHandler Event handler for store operation retry events.
     * @return A shard map manager object used for performing management and read operations for
     * shard maps, shards and shard mappings.
     */
    public static ShardMapManager CreateSqlShardMapManager(String connectionString, ShardMapManagerCreateMode createMode, RetryBehavior retryBehavior, EventHandler<RetryingEventArgs> retryEventHandler) {
        return CreateSqlShardMapManagerImpl(connectionString, createMode, retryBehavior, retryEventHandler, GlobalConstants.GsmVersionClient);
    }

    /**
     * Creates a <see cref="ShardMapManager"/> and its corresponding storage structures in the specified SQL Server database.
     *
     * @param connectionString  Connection parameters used for creating shard map manager database.
     * @param createMode        Describes the option selected by the user for creating shard map manager database.
     * @param retryBehavior     Behavior for performing retries on connections to shard map manager database.
     * @param targetVersion     Target version of Store to deploy, this is mainly used for upgrade testing.
     * @param retryEventHandler Event handler for store operation retry events.
     * @return A shard map manager object used for performing management and read operations for
     * shard maps, shards and shard mappings.
     */
    private static ShardMapManager CreateSqlShardMapManagerImpl(String connectionString, ShardMapManagerCreateMode createMode, RetryBehavior retryBehavior, tangible.EventHandler<RetryingEventArgs> retryEventHandler, Version targetVersion) {
        ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");
        ExceptionUtils.DisallowNullArgument(retryBehavior, "retryBehavior");

        if (createMode != ShardMapManagerCreateMode.KeepExisting && createMode != ShardMapManagerCreateMode.ReplaceExisting) {
            throw new IllegalArgumentException(StringUtils.FormatInvariant(Errors._General_InvalidArgumentValue, createMode, "createMode"), "createMode");
        }

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.NewGuid())) {
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManagerFactory, "CreateSqlShardMapManager", "Start; ");

            Stopwatch stopwatch = Stopwatch.StartNew();

            SqlShardMapManagerCredentials credentials = new SqlShardMapManagerCredentials(connectionString);

            TransientFaultHandling.RetryPolicy retryPolicy = new TransientFaultHandling.RetryPolicy(new ShardManagementTransientErrorDetectionStrategy(retryBehavior), RetryPolicy.DefaultRetryPolicy.GetRetryStrategy());

            tangible.EventHandler<TransientFaultHandling.RetryingEventArgs> handler = (sender, args) -> {
                if (retryEventHandler != null) {
                    retryEventHandler.invoke(sender, new RetryingEventArgs(args));
                }
            };

            try {
                retryPolicy.Retrying += handler;

                // specifying targetVersion as GlobalConstants.GsmVersionClient to deploy latest store by default.
                try (IStoreOperationGlobal op = (new StoreOperationFactory()).CreateCreateShardMapManagerGlobalOperation(credentials, retryPolicy, "CreateSqlShardMapManager", createMode, targetVersion)) {
                    op.Do();
                }

                stopwatch.Stop();

                getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManagerFactory, "CreateSqlShardMapManager", "Complete; Duration: {0}", stopwatch.Elapsed);
            } finally {
                retryPolicy.Retrying -= handler;
            }

            return new ShardMapManager(credentials, new SqlStoreConnectionFactory(), new StoreOperationFactory(), new CacheStore(), ShardMapManagerLoadPolicy.Lazy, RetryPolicy.DefaultRetryPolicy, retryBehavior, retryEventHandler);
        }
    }

    /**
     * Gets <see cref="ShardMapManager"/> from persisted state in a SQL Server database.
     *
     * @param connectionString Connection parameters used for performing operations against shard map manager database(s).
     * @param loadPolicy       Initialization policy.
     * @param shardMapManager  Shard map manager object used for performing management and read operations for shard maps,
     *                         shards and shard mappings or <c>null</c> in case shard map manager does not exist.
     * @return <c>true</c> if a shard map manager object was created, <c>false</c> otherwise.
     */
    public static boolean TryGetSqlShardMapManager(String connectionString, ShardMapManagerLoadPolicy loadPolicy, ReferenceObjectHelper<ShardMapManager> shardMapManager) {
        return TryGetSqlShardMapManager(connectionString, loadPolicy, RetryBehavior.DefaultRetryBehavior, shardMapManager);
    }

    /**
     * Gets <see cref="ShardMapManager"/> from persisted state in a SQL Server database.
     *
     * @param connectionString Connection parameters used for performing operations against shard map manager database(s).
     * @param loadPolicy       Initialization policy.
     * @param shardMapManager  Shard map manager object used for performing management and read operations for shard maps,
     *                         shards and shard mappings or <c>null</c> in case shard map manager does not exist.
     * @param retryBehavior    Behavior for detecting transient exceptions in the store.
     * @return <c>true</c> if a shard map manager object was created, <c>false</c> otherwise.
     */
//TODO TASK: Java annotations will not correspond to .NET attributes:
//ORIGINAL LINE: [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1021:AvoidOutParameters", MessageId = "2#"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "2")] public static bool TryGetSqlShardMapManager(string connectionString, ShardMapManagerLoadPolicy loadPolicy, RetryBehavior retryBehavior, out ShardMapManager shardMapManager)
    public static boolean TryGetSqlShardMapManager(String connectionString, ShardMapManagerLoadPolicy loadPolicy, RetryBehavior retryBehavior, ReferenceObjectHelper<ShardMapManager> shardMapManager) {
        ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");
        ExceptionUtils.DisallowNullArgument(retryBehavior, "retryBehavior");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.NewGuid())) {
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManagerFactory, "TryGetSqlShardMapManager", "Start; ");

            Stopwatch stopwatch = Stopwatch.StartNew();

            shardMapManager.argValue = ShardMapManagerFactory.GetSqlShardMapManager(connectionString, loadPolicy, retryBehavior, null, false);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManagerFactory, "TryGetSqlShardMapManager", "Complete; Duration: {0}", stopwatch.Elapsed);

            return shardMapManager.argValue != null;
        }
    }

    /**
     * Gets <see cref="ShardMapManager"/> from persisted state in a SQL Server database.
     *
     * @param connectionString  Connection parameters used for performing operations against shard map manager database(s).
     * @param loadPolicy        Initialization policy.
     * @param shardMapManager   Shard map manager object used for performing management and read operations for shard maps,
     *                          shards and shard mappings or <c>null</c> in case shard map manager does not exist.
     * @param retryBehavior     Behavior for detecting transient exceptions in the store.
     * @param retryEventHandler Event handler for store operation retry events.
     * @return <c>true</c> if a shard map manager object was created, <c>false</c> otherwise.
     */
//TODO TASK: Java annotations will not correspond to .NET attributes:
//ORIGINAL LINE: [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1021:AvoidOutParameters", MessageId = "2#"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "2")] internal static bool TryGetSqlShardMapManager(string connectionString, ShardMapManagerLoadPolicy loadPolicy, RetryBehavior retryBehavior, EventHandler<RetryingEventArgs> retryEventHandler, out ShardMapManager shardMapManager)
    public static boolean TryGetSqlShardMapManager(String connectionString, ShardMapManagerLoadPolicy loadPolicy, RetryBehavior retryBehavior, tangible.EventHandler<RetryingEventArgs> retryEventHandler, ReferenceObjectHelper<ShardMapManager> shardMapManager) {
        ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");
        ExceptionUtils.DisallowNullArgument(retryBehavior, "retryBehavior");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.NewGuid())) {
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManagerFactory, "TryGetSqlShardMapManager", "Start; ");

            Stopwatch stopwatch = Stopwatch.StartNew();

            shardMapManager.argValue = ShardMapManagerFactory.GetSqlShardMapManager(connectionString, loadPolicy, retryBehavior, retryEventHandler, false);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManagerFactory, "TryGetSqlShardMapManager", "Complete; Duration: {0}", stopwatch.Elapsed);

            return shardMapManager.argValue != null;
        }
    }

    /**
     * Gets <see cref="ShardMapManager"/> from persisted state in a SQL Server database, with <see cref="RetryBehavior.DefaultRetryBehavior"/>.
     *
     * @param connectionString Connection parameters used for performing operations against shard map manager database(s).
     * @param loadPolicy       Initialization policy.
     * @return A shard map manager object used for performing management and read operations for
     * shard maps, shards and shard mappings.
     */
//TODO TASK: Java annotations will not correspond to .NET attributes:
//ORIGINAL LINE: [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")] public static ShardMapManager GetSqlShardMapManager(string connectionString, ShardMapManagerLoadPolicy loadPolicy)
    public static ShardMapManager GetSqlShardMapManager(String connectionString, ShardMapManagerLoadPolicy loadPolicy) {
        return GetSqlShardMapManager(connectionString, loadPolicy, RetryBehavior.DefaultRetryBehavior);
    }

    /**
     * Gets <see cref="ShardMapManager"/> from persisted state in a SQL Server database.
     *
     * @param connectionString Connection parameters used for performing operations against shard map manager database(s).
     * @param loadPolicy       Initialization policy.
     * @param retryBehavior    Behavior for detecting transient exceptions in the store.
     * @return A shard map manager object used for performing management and read operations for
     * shard maps, shards and shard mappings.
     */
    public static ShardMapManager GetSqlShardMapManager(String connectionString, ShardMapManagerLoadPolicy loadPolicy, RetryBehavior retryBehavior) {
        return GetSqlShardMapManager(connectionString, loadPolicy, retryBehavior, null);
    }

    /**
     * Create shard management performance counter category and counters
     */
    public static void CreatePerformanceCategoryAndCounters() {
        PerfCounterInstance.CreatePerformanceCategoryAndCounters();
    }

    /**
     * Gets <see cref="ShardMapManager"/> from persisted state in a SQL Server database.
     *
     * @param connectionString  Connection parameters used for performing operations against shard map manager database(s).
     * @param loadPolicy        Initialization policy.
     * @param retryBehavior     Behavior for detecting transient exceptions in the store.
     * @param retryEventHandler Event handler for store operation retry events.
     * @return A shard map manager object used for performing management and read operations for
     * shard maps, shards and shard mappings.
     */
//TODO TASK: Java annotations will not correspond to .NET attributes:
//ORIGINAL LINE: [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")] internal static ShardMapManager GetSqlShardMapManager(string connectionString, ShardMapManagerLoadPolicy loadPolicy, RetryBehavior retryBehavior, EventHandler<RetryingEventArgs> retryEventHandler)
    public static ShardMapManager GetSqlShardMapManager(String connectionString, ShardMapManagerLoadPolicy loadPolicy, RetryBehavior retryBehavior, tangible.EventHandler<RetryingEventArgs> retryEventHandler) {
        ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");
        ExceptionUtils.DisallowNullArgument(retryBehavior, "retryBehavior");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.NewGuid())) {
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManagerFactory, "GetSqlShardMapManager", "Start; ");

            Stopwatch stopwatch = Stopwatch.StartNew();

            ShardMapManager shardMapManager = ShardMapManagerFactory.GetSqlShardMapManager(connectionString, loadPolicy, retryBehavior, retryEventHandler, true);

            stopwatch.Stop();

            assert shardMapManager != null;

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManagerFactory, "GetSqlShardMapManager", "Complete; Duration: {0}", stopwatch.Elapsed);

            return shardMapManager;
        }
    }

    /**
     * Gets <see cref="ShardMapManager"/> from persisted state in a SQL Server database.
     *
     * @param connectionString  Connection parameters used for performing operations against shard map manager database(s).
     * @param loadPolicy        Initialization policy.
     * @param retryBehavior     Behavior for detecting transient exceptions in the store.
     * @param retryEventHandler Event handler for store operation retry events.
     * @param throwOnFailure    Whether to raise exception on failure.
     * @return A shard map manager object used for performing management and read operations for
     * shard maps, shards and shard mappings or <c>null</c> if the object could not be created.
     */
//TODO TASK: Java annotations will not correspond to .NET attributes:
//ORIGINAL LINE: [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "We need to hand of the ShardMapManager to the user.")] private static ShardMapManager GetSqlShardMapManager(string connectionString, ShardMapManagerLoadPolicy loadPolicy, RetryBehavior retryBehavior, EventHandler<RetryingEventArgs> retryEventHandler, bool throwOnFailure)
    private static ShardMapManager GetSqlShardMapManager(String connectionString, ShardMapManagerLoadPolicy loadPolicy, RetryBehavior retryBehavior, tangible.EventHandler<RetryingEventArgs> retryEventHandler, boolean throwOnFailure) {
        assert connectionString != null;
        assert retryBehavior != null;

        SqlShardMapManagerCredentials credentials = new SqlShardMapManagerCredentials(connectionString);

        StoreOperationFactory storeOperationFactory = new StoreOperationFactory();

        IStoreResults result;

        TransientFaultHandling.RetryPolicy retryPolicy = new TransientFaultHandling.RetryPolicy(new ShardManagementTransientErrorDetectionStrategy(retryBehavior), RetryPolicy.DefaultRetryPolicy.GetRetryStrategy());

        tangible.EventHandler<TransientFaultHandling.RetryingEventArgs> handler = (sender, args) -> {
            if (retryEventHandler != null) {
                retryEventHandler.invoke(sender, new RetryingEventArgs(args));
            }
        };

        try {
            retryPolicy.Retrying += handler;

            try (IStoreOperationGlobal op = storeOperationFactory.CreateGetShardMapManagerGlobalOperation(credentials, retryPolicy, throwOnFailure ? "GetSqlShardMapManager" : "TryGetSqlShardMapManager", throwOnFailure)) {
                result = op.Do();
            }
        } finally {
            retryPolicy.Retrying -= handler;
        }

        return result.Result == StoreResult.Success ? new ShardMapManager(credentials, new SqlStoreConnectionFactory(), storeOperationFactory, new CacheStore(), loadPolicy, RetryPolicy.DefaultRetryPolicy, retryBehavior, retryEventHandler) : null;
    }
}
