package com.microsoft.azure.elasticdb.shard.mapmanager;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.google.common.base.Stopwatch;
import com.microsoft.azure.elasticdb.core.commons.helpers.EventHandler;
import com.microsoft.azure.elasticdb.core.commons.logging.ActivityIdScope;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryBehavior;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryingEventArgs;
import com.microsoft.azure.elasticdb.shard.cache.ICacheStore;
import com.microsoft.azure.elasticdb.shard.map.RangeShardMap;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.map.ShardMapExtensions;
import com.microsoft.azure.elasticdb.shard.map.ShardMapUtils;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlShardMapManagerCredentials;
import com.microsoft.azure.elasticdb.shard.store.IStoreConnectionFactory;
import com.microsoft.azure.elasticdb.shard.store.IStoreResults;
import com.microsoft.azure.elasticdb.shard.store.IStoreShardMap;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationFactory;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import com.microsoft.azure.elasticdb.shard.utils.GlobalConstants;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Serves as the entry point for creation, management and lookup operations over shard maps.
 */
public final class ShardMapManager {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * credentials for performing ShardMapManager operations.
     */
    private SqlShardMapManagerCredentials credentials;
    /**
     * Factory for store connections.
     */
    private IStoreConnectionFactory StoreConnectionFactory;
    /**
     * Factory for store operations.
     */
    private IStoreOperationFactory StoreOperationFactory;
    /**
     * Policy for performing retries on connections to shard map manager database.
     */
    private RetryPolicy RetryPolicy;
    /**
     * Local cache.
     */
    private ICacheStore Cache;

    /**
     * Given the connection string, opens up the corresponding data source and obtains the ShardMapManager.
     *
     * @param credentials            credentials for performing ShardMapManager operations.
     * @param storeConnectionFactory Factory for store connections.
     * @param storeOperationFactory  Factory for store operations.
     * @param cacheStore             Cache store.
     * @param loadPolicy             Initialization policy.
     * @param retryPolicy            Policy for performing retries on connections to shard map manager database.
     * @param retryBehavior          Policy for detecting transient errors.
     */
    public ShardMapManager(SqlShardMapManagerCredentials credentials, IStoreConnectionFactory storeConnectionFactory, IStoreOperationFactory storeOperationFactory, ICacheStore cacheStore, ShardMapManagerLoadPolicy loadPolicy, RetryPolicy retryPolicy, RetryBehavior retryBehavior) {
        this(credentials, storeConnectionFactory, storeOperationFactory, cacheStore, loadPolicy, retryPolicy, retryBehavior, null);
    }

    /**
     * Given the connection string, opens up the corresponding data source and obtains the ShardMapManager.
     *
     * @param credentials            credentials for performing ShardMapManager operations.
     * @param storeConnectionFactory Factory for store connections.
     * @param storeOperationFactory  Factory for store operations.
     * @param cacheStore             Cache store.
     * @param loadPolicy             Initialization policy.
     * @param retryPolicy            Policy for performing retries on connections to shard map manager database.
     * @param retryBehavior          Policy for detecting transient errors.
     * @param retryEventHandler      Event handler for store operation retry events.
     */
    public ShardMapManager(SqlShardMapManagerCredentials credentials, IStoreConnectionFactory storeConnectionFactory, IStoreOperationFactory storeOperationFactory
            , ICacheStore cacheStore, ShardMapManagerLoadPolicy loadPolicy, RetryPolicy retryPolicy
            , RetryBehavior retryBehavior, EventHandler<RetryingEventArgs> retryEventHandler) {
        this.credentials = credentials;
        StoreConnectionFactory = storeConnectionFactory;
        StoreOperationFactory = storeOperationFactory;
        Cache = cacheStore;
        RetryPolicy = retryPolicy;
    }

    public SqlShardMapManagerCredentials getCredentials() {
        return credentials;
    }

    public void setCredentials(SqlShardMapManagerCredentials value) {
        credentials = value;
    }

    public IStoreConnectionFactory getStoreConnectionFactory() {
        return StoreConnectionFactory;
    }

    public IStoreOperationFactory getStoreOperationFactory() {
        return StoreOperationFactory;
    }

    public RetryPolicy getRetryPolicy() {
        return RetryPolicy;
    }

    public ICacheStore getCache() {
        return Cache;
    }

    public void setCache(ICacheStore value) {
        Cache = value;
    }

    public <T> RangeShardMap<T> TryGetRangeShardMap(String shardMapName) {
        ValidateShardMapName(shardMapName);

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID()))
        {
            log.debug("Start; ShardMap: {}", shardMapName);

            ShardMap shardMap = this.LookupAndConvertShardMapHelper(
                    "TryGetRangeShardMap",
                    shardMapName, false);
            log.info("Complete; ShardMap: {}", shardMapName);
            return ShardMapExtensions.AsRangeShardMap(shardMap, false);
        }
    }

    public <T> RangeShardMap<T> CreateRangeShardMap(String shardMapName) {
        return null;
    }


    /// <summary>
    /// Ensures that the given shard map name is valid.
    /// </summary>
    /// <param name="shardMapName">Input shard map name.</param>
    private static void ValidateShardMapName(String shardMapName)
    {
        ExceptionUtils.DisallowNullOrEmptyStringArgument(shardMapName, "shardMapName");

        // Disallow non-alpha-numeric characters.
        if (!StringUtils.isAlphanumeric(shardMapName)) {
            throw new IllegalArgumentException(String.format(Errors._ShardMapManager_UnsupportedShardMapName,
                shardMapName));
        }

        // Ensure that length is within bounds.
        if (shardMapName.length() > GlobalConstants.MaximumShardMapNameLength) {
            throw new IllegalArgumentException(String.format(
                Errors._ShardMapManager_UnsupportedShardMapNameLength,
                shardMapName,
                GlobalConstants.MaximumShardMapNameLength));
        }
    }

    /// <summary>
    /// Performs lookup and conversion operation for shard map with given name.
    /// </summary>
    /// <typeparam name="TShardMap">Type to convert shard map to.</typeparam>
    /// <param name="operationName">Operation name, useful for diagnostics.</param>
    /// <param name="shardMapName">Shard map name.</param>
    /// <param name="converter">Function to downcast a shard map to List/Range/Hash.</param>
    /// <param name="throwOnFailure">Whether to throw exception or return null on failure.</param>
    /// <returns>The converted shard map.</returns>
    private ShardMap LookupAndConvertShardMapHelper(String operationName
        , String shardMapName
        , boolean throwOnFailure)  {
        ShardMap sm = this.LookupShardMapByName(operationName, shardMapName, true);

        if (sm == null && throwOnFailure) {
            throw new ShardManagementException(
                ShardManagementErrorCategory.ShardMapManager,
                ShardManagementErrorCode.ShardMapLookupFailure,
                Errors._ShardMapManager_ShardMapLookupFailed,
                shardMapName,
                this.credentials.getShardMapManagerLocation());
        }
        return sm;
    }


    /// <summary>
    /// Finds a shard map from cache if requested and if necessary from global shard map.
    /// </summary>
    /// <param name="operationName">Operation name, useful for diagnostics.</param>
    /// <param name="shardMapName">Name of shard map.</param>
    /// <param name="lookInCacheFirst">Whether to skip first lookup in cache.</param>
    /// <returns>Shard map object corresponding to one being searched.</returns>
    private ShardMap LookupShardMapByName(String operationName, String shardMapName, boolean lookInCacheFirst)
    {
        IStoreShardMap ssm = null;

        if (lookInCacheFirst)
        {
            // Typical scenario will result in immediate lookup succeeding.
            ssm = this.Cache.LookupShardMapByName(shardMapName);
        }

        ShardMap shardMap;

        // Cache miss. Go to store and add entry to cache.
        if (ssm == null)
        {
            Stopwatch stopwatch = Stopwatch.createStarted();

            shardMap = this.LookupShardMapByNameInStore(operationName, shardMapName);

            stopwatch.stop();

            log.info("Lookup ShardMap: {} in store complete; Duration: {}",
                    shardMapName, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }
        else
        {
            shardMap = ShardMapUtils.CreateShardMapFromStoreShardMap(this, ssm);
        }

        return shardMap;
    }

    /// <summary>
    /// Finds shard map with given name in global shard map.
    /// </summary>
    /// <param name="operationName">Operation name, useful for diagnostics.</param>
    /// <param name="shardMapName">Name of shard map to search.</param>
    /// <returns>Shard map corresponding to given Id.</returns>
    private ShardMap LookupShardMapByNameInStore(String operationName, String shardMapName)
    {
        IStoreResults result;

        try (IStoreOperationGlobal op = this.StoreOperationFactory.CreateFindShardMapByNameGlobalOperation(this, operationName, shardMapName))
        {
            result = op.Do();
            return result.getStoreShardMaps()
                    .stream().map(ssm -> ShardMapUtils.CreateShardMapFromStoreShardMap(this, ssm))
                         .findFirst().get();
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }
}
