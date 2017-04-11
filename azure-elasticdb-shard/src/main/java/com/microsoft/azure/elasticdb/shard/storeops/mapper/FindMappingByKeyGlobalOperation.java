package com.microsoft.azure.elasticdb.shard.storeops.mapper;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.cache.CacheStoreMappingUpdatePolicy;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.*;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationErrorHandler;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Obtains the mapping from the GSM based on given key.
 */
public class FindMappingByKeyGlobalOperation extends StoreOperationGlobal {
    /**
     * Shard map manager instance.
     */
    private ShardMapManager _manager;

    /**
     * Shard map for which mappings are requested.
     */
    private StoreShardMap _shardMap;

    /**
     * Key being searched.
     */
    private ShardKey _key;

    /**
     * Policy for cache update.
     */
    private CacheStoreMappingUpdatePolicy _policy;

    /**
     * Error category to use.
     */
    private ShardManagementErrorCategory _errorCategory;

    /**
     * Whether to cache the results.
     */
    private boolean _cacheResults;

    /**
     * Ignore ShardMapNotFound error.
     */
    private boolean _ignoreFailure;

    /**
     * Constructs request for obtaining mapping from GSM based on given key.
     *
     * @param shardMapManager Shard map manager.
     * @param operationName   Operation being executed.
     * @param shardMap        Local shard map.
     * @param key             Key for lookup operation.
     * @param policy          Policy for cache update.
     * @param errorCategory   Error category.
     * @param cacheResults    Whether to cache the results of the operation.
     * @param ignoreFailure   Ignore shard map not found error.
     */
    public FindMappingByKeyGlobalOperation(ShardMapManager shardMapManager, String operationName, StoreShardMap shardMap, ShardKey key, CacheStoreMappingUpdatePolicy policy, ShardManagementErrorCategory errorCategory, boolean cacheResults, boolean ignoreFailure) {
        super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), operationName);
        _manager = shardMapManager;
        _shardMap = shardMap;
        _key = key;
        _policy = policy;
        _errorCategory = errorCategory;
        _cacheResults = cacheResults;
        _ignoreFailure = ignoreFailure;
    }

    /**
     * Whether this is a read-only operation.
     */
    @Override
    public boolean getReadOnly() {
        return true;
    }

    /**
     * Execute the operation against GSM in the current transaction scope.
     *
     * @param ts Transaction scope.
     * @return Results of the operation.
     */
    @Override
    public IStoreResults DoGlobalExecute(IStoreTransactionScope ts) {
        // If no ranges are specified, blindly mark everything for deletion.
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpFindShardMappingByKeyGlobal, StoreOperationRequestBuilder.FindShardMappingByKeyGlobal(_shardMap, _key));
    }

    /**
     * Asynchronously execute the operation against GSM in the current transaction scope.
     *
     * @param ts Transaction scope.
     * @return Task encapsulating results of the operation.
     */
    @Override
    public Callable<IStoreResults> DoGlobalExecuteAsync(IStoreTransactionScope ts) {
        // If no ranges are specified, blindly mark everything for deletion.
        return ts.ExecuteOperationAsync(StoreOperationRequestBuilder.SpFindShardMappingByKeyGlobal, StoreOperationRequestBuilder.FindShardMappingByKeyGlobal(_shardMap, _key));
    }

    /**
     * Invalidates the cache on unsuccessful commit of the GSM operation.
     *
     * @param result Operation result.
     */
    @Override
    public void DoGlobalUpdateCachePre(IStoreResults result) {
        if (result.getResult() == StoreResult.ShardMapDoesNotExist) {
            // Remove shard map from cache.
            _manager.getCache().DeleteShardMap(_shardMap);
        }
    }

    /**
     * Handles errors from the GSM operation after the LSM operations.
     *
     * @param result Operation result.
     */
    @Override
    public void HandleDoGlobalExecuteError(IStoreResults result) {
        // MappingNotFound for key is supposed to be handled in the calling layers
        // so that TryLookup vs Lookup have proper behavior.
        if (result.getResult() != StoreResult.MappingNotFoundForKey) {
            // Recovery manager handles the ShardMapDoesNotExist error properly, so we don't interfere.
            if (!_ignoreFailure || result.getResult() != StoreResult.ShardMapDoesNotExist) {
                // Possible errors are:
                // StoreResult.ShardMapDoesNotExist
                // StoreResult.StoreVersionMismatch
                // StoreResult.MissingParametersForStoredProcedure
                throw StoreOperationErrorHandler.OnShardMapperErrorGlobal(result, _shardMap, null, _errorCategory, this.getOperationName(), StoreOperationRequestBuilder.SpFindShardMappingByKeyGlobal); // shard
            }
        }
    }

    /**
     * Refreshes the cache on successful commit of the GSM operation.
     *
     * @param result Operation result.
     */
    @Override
    public void DoGlobalUpdateCachePost(IStoreResults result) {
        assert result.getResult() == StoreResult.Success || result.getResult() == StoreResult.MappingNotFoundForKey || result.getResult() == StoreResult.ShardMapDoesNotExist;

        if (result.getResult() == StoreResult.Success && _cacheResults) {
            for (StoreMapping sm : result.getStoreMappings()) {
                _manager.getCache().AddOrUpdateMapping(sm, _policy);
            }
        }
    }

    /**
     * Error category for store exception.
     */
    @Override
    protected ShardManagementErrorCategory getErrorCategory() {
        return _errorCategory;
    }

    @Override
    public void close() throws IOException {

    }
}