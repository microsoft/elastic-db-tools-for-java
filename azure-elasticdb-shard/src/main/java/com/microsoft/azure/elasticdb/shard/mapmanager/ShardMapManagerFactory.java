package com.microsoft.azure.elasticdb.shard.mapmanager;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.helpers.EventHandler;
import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.core.commons.logging.ActivityIdScope;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryBehavior;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryingEventArgs;
import com.microsoft.azure.elasticdb.shard.cache.CacheStore;
import com.microsoft.azure.elasticdb.shard.cache.PerfCounterInstance;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlShardMapManagerCredentials;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlStoreConnectionFactory;
import com.microsoft.azure.elasticdb.shard.store.IStoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreResult;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationFactory;
import com.microsoft.azure.elasticdb.shard.utils.*;

import java.io.IOException;
import java.util.UUID;

/**
 * Factory for <see cref="ShardMapManager"/>s facilitates the creation and management
 * of shard map manager persistent state. Use this class as the entry point to the library's
 * object hierarchy.
 */
public final class ShardMapManagerFactory {

    /**
     * Creates a <see cref="ShardMapManager"/> and its corresponding storage structures in the specified SQL Server database,
     * with <see cref="ShardMapManagerCreateMode.KeepExisting"/> and <see cref="RetryBehavior.DefaultRetryBehavior"/>.
     *
     * @param connectionString Connection parameters used for creating shard map manager database.
     * @return A shard map manager object used for performing management and read operations for
     * shard maps, shards and shard mappings.
     */
    public static ShardMapManager CreateSqlShardMapManager(String connectionString) {
        return CreateSqlShardMapManager(connectionString, ShardMapManagerCreateMode.KeepExisting, RetryBehavior.getDefaultRetryBehavior());
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
        return CreateSqlShardMapManager(connectionString, createMode, RetryBehavior.getDefaultRetryBehavior());
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
        return CreateSqlShardMapManagerImpl(connectionString, createMode, RetryBehavior.getDefaultRetryBehavior(), null, targetVersion);
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
    private static ShardMapManager CreateSqlShardMapManagerImpl(String connectionString, ShardMapManagerCreateMode createMode, RetryBehavior retryBehavior, EventHandler<RetryingEventArgs> retryEventHandler, Version targetVersion) {
        ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");
        ExceptionUtils.DisallowNullArgument(retryBehavior, "retryBehavior");

        if (createMode != ShardMapManagerCreateMode.KeepExisting && createMode != ShardMapManagerCreateMode.ReplaceExisting) {
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._General_InvalidArgumentValue, createMode, "createMode"), new Throwable("createMode"));
        }

        //try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
        //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManagerFactory, "CreateSqlShardMapManager", "Start; ");

        //Stopwatch stopwatch = Stopwatch.createStarted();

        SqlShardMapManagerCredentials credentials = new SqlShardMapManagerCredentials(connectionString);

        RetryPolicy retryPolicy = new RetryPolicy(new ShardManagementTransientErrorDetectionStrategy(retryBehavior), RetryPolicy.DefaultRetryPolicy.GetRetryStrategy());

        EventHandler<RetryingEventArgs> handler = (sender, args) -> {
            if (retryEventHandler != null) {
                retryEventHandler.invoke(sender, new RetryingEventArgs(args));
            }
        };

        try {
            //retryPolicy.Retrying += handler;

            // specifying targetVersion as GlobalConstants.GsmVersionClient to deploy latest store by default.
            try (IStoreOperationGlobal op = (new StoreOperationFactory()).CreateCreateShardMapManagerGlobalOperation(credentials, retryPolicy, "CreateSqlShardMapManager", createMode, targetVersion)) {
                op.Do();
            } catch (IOException e) {
                e.printStackTrace();
            }

            //stopwatch.stop();

            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManagerFactory, "CreateSqlShardMapManager", "Complete; Duration: {0}", stopwatch.Elapsed);
        } finally {
            //TODO: retryPolicy.Retrying -= handler;
        }

        return new ShardMapManager(credentials, new SqlStoreConnectionFactory(), new StoreOperationFactory(), new CacheStore(), ShardMapManagerLoadPolicy.Lazy, RetryPolicy.DefaultRetryPolicy, retryBehavior, retryEventHandler);
        //}
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
        return TryGetSqlShardMapManager(connectionString, loadPolicy, RetryBehavior.getDefaultRetryBehavior(), shardMapManager);
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
    public static boolean TryGetSqlShardMapManager(String connectionString, ShardMapManagerLoadPolicy loadPolicy, RetryBehavior retryBehavior, ReferenceObjectHelper<ShardMapManager> shardMapManager) {
        ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");
        ExceptionUtils.DisallowNullArgument(retryBehavior, "retryBehavior");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManagerFactory, "TryGetSqlShardMapManager", "Start; ");

            //Stopwatch stopwatch = Stopwatch.createStarted();

            shardMapManager.argValue = ShardMapManagerFactory.GetSqlShardMapManager(connectionString, loadPolicy, retryBehavior, null, false);

            //stopwatch.stop();

            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManagerFactory, "TryGetSqlShardMapManager", "Complete; Duration: {0}", stopwatch.Elapsed);

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
    public static boolean TryGetSqlShardMapManager(String connectionString, ShardMapManagerLoadPolicy loadPolicy, RetryBehavior retryBehavior, EventHandler<RetryingEventArgs> retryEventHandler, ReferenceObjectHelper<ShardMapManager> shardMapManager) {
        ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");
        ExceptionUtils.DisallowNullArgument(retryBehavior, "retryBehavior");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManagerFactory, "TryGetSqlShardMapManager", "Start; ");

            //Stopwatch stopwatch = Stopwatch.createStarted();

            shardMapManager.argValue = ShardMapManagerFactory.GetSqlShardMapManager(connectionString, loadPolicy, retryBehavior, retryEventHandler, false);

            //stopwatch.stop();

            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManagerFactory, "TryGetSqlShardMapManager", "Complete; Duration: {0}", stopwatch.Elapsed);

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
    public static ShardMapManager GetSqlShardMapManager(String connectionString, ShardMapManagerLoadPolicy loadPolicy) {
        return GetSqlShardMapManager(connectionString, loadPolicy, RetryBehavior.getDefaultRetryBehavior());
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
    public static ShardMapManager GetSqlShardMapManager(String connectionString, ShardMapManagerLoadPolicy loadPolicy, RetryBehavior retryBehavior, EventHandler<RetryingEventArgs> retryEventHandler) {
        ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");
        ExceptionUtils.DisallowNullArgument(retryBehavior, "retryBehavior");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManagerFactory, "GetSqlShardMapManager", "Start; ");

            //Stopwatch stopwatch = Stopwatch.createStarted();

            ShardMapManager shardMapManager = ShardMapManagerFactory.GetSqlShardMapManager(connectionString, loadPolicy, retryBehavior, retryEventHandler, true);

            //stopwatch.stop();

            assert shardMapManager != null;

            //getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManagerFactory, "GetSqlShardMapManager", "Complete; Duration: {0}", stopwatch.Elapsed);

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
    private static ShardMapManager GetSqlShardMapManager(String connectionString, ShardMapManagerLoadPolicy loadPolicy, RetryBehavior retryBehavior, EventHandler<RetryingEventArgs> retryEventHandler, boolean throwOnFailure) {
        assert connectionString != null;
        assert retryBehavior != null;

        SqlShardMapManagerCredentials credentials = new SqlShardMapManagerCredentials(connectionString);

        StoreOperationFactory storeOperationFactory = new StoreOperationFactory();

        IStoreResults result = null;

        RetryPolicy retryPolicy = new RetryPolicy(new ShardManagementTransientErrorDetectionStrategy(retryBehavior), RetryPolicy.DefaultRetryPolicy.GetRetryStrategy());

        EventHandler<RetryingEventArgs> handler = (sender, args) -> {
            if (retryEventHandler != null) {
                retryEventHandler.invoke(sender, new RetryingEventArgs(args));
            }
        };

        try {
            //TODO: retryPolicy.Retrying += handler;

            try (IStoreOperationGlobal op = storeOperationFactory.CreateGetShardMapManagerGlobalOperation(credentials, retryPolicy, throwOnFailure ? "GetSqlShardMapManager" : "TryGetSqlShardMapManager", throwOnFailure)) {
                result = op.Do();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } finally {
            //TODO: retryPolicy.Retrying -= handler;
        }

        return result.getResult() == StoreResult.Success ? new ShardMapManager(credentials, new SqlStoreConnectionFactory(), storeOperationFactory, new CacheStore(), loadPolicy, RetryPolicy.DefaultRetryPolicy, retryBehavior, retryEventHandler) : null;
    }
}
