package com.microsoft.azure.elasticdb.shard.mapmanager;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.helpers.EventHandler;
import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.core.commons.logging.ILogger;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryBehavior;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryingEventArgs;
import com.microsoft.azure.elasticdb.shard.cache.ICacheStore;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlShardMapManagerCredentials;
import com.microsoft.azure.elasticdb.shard.store.IStoreConnectionFactory;
import com.microsoft.azure.elasticdb.shard.store.IStoreShardMap;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationFactory;
import com.microsoft.azure.elasticdb.shard.storeops.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.utils.Version;

import java.util.*;

/**
 * Serves as the entry point for creation, management and lookup operations over shard maps.
 */
public final class ShardMapManager {
    /**
     * Credentials for performing ShardMapManager operations.
     */
    private SqlShardMapManagerCredentials Credentials;
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
     * @param credentials            Credentials for performing ShardMapManager operations.
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
     * @param credentials            Credentials for performing ShardMapManager operations.
     * @param storeConnectionFactory Factory for store connections.
     * @param storeOperationFactory  Factory for store operations.
     * @param cacheStore             Cache store.
     * @param loadPolicy             Initialization policy.
     * @param retryPolicy            Policy for performing retries on connections to shard map manager database.
     * @param retryBehavior          Policy for detecting transient errors.
     * @param retryEventHandler      Event handler for store operation retry events.
     */
    public ShardMapManager(SqlShardMapManagerCredentials credentials, IStoreConnectionFactory storeConnectionFactory, IStoreOperationFactory storeOperationFactory, ICacheStore cacheStore, ShardMapManagerLoadPolicy loadPolicy, RetryPolicy retryPolicy, RetryBehavior retryBehavior, EventHandler<RetryingEventArgs> retryEventHandler) {
        //TODO: Implement
    }

    public SqlShardMapManagerCredentials getCredentials() {
        return Credentials;
    }

    public void setCredentials(SqlShardMapManagerCredentials value) {
        Credentials = value;
    }

    public IStoreConnectionFactory getStoreConnectionFactory() {
        return StoreConnectionFactory;
    }

    public void setStoreConnectionFactory(IStoreConnectionFactory value) {
        StoreConnectionFactory = value;
    }

    public IStoreOperationFactory getStoreOperationFactory() {
        return StoreOperationFactory;
    }

    public void setStoreOperationFactory(IStoreOperationFactory value) {
        StoreOperationFactory = value;
    }

    public RetryPolicy getRetryPolicy() {
        return RetryPolicy;
    }

    public void setRetryPolicy(RetryPolicy value) {
        RetryPolicy = value;
    }

    public ICacheStore getCache() {
        return Cache;
    }

    public void setCache(ICacheStore value) {
        Cache = value;
    }
}
