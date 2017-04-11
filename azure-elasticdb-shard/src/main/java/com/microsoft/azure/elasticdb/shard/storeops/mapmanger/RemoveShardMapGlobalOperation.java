package com.microsoft.azure.elasticdb.shard.storeops.mapmanger;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreResult;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationErrorHandler;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;

import java.io.IOException;

/**
 * Removes given shard map from GSM.
 */
public class RemoveShardMapGlobalOperation extends StoreOperationGlobal {
    /**
     * Shard map manager object.
     */
    private ShardMapManager _shardMapManager;

    /**
     * Shard map to remove.
     */
    private StoreShardMap _shardMap;

    /**
     * Constructs request to remove given shard map from GSM.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationName   Operation name, useful for diagnostics.
     * @param shardMap        Shard map to remove.
     */
    public RemoveShardMapGlobalOperation(ShardMapManager shardMapManager, String operationName, StoreShardMap shardMap) {
        super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), operationName);
        _shardMapManager = shardMapManager;
        _shardMap = shardMap;
    }

    /**
     * Whether this is a read-only operation.
     */
    @Override
    public boolean getReadOnly() {
        return false;
    }

    /**
     * Execute the operation against GSM in the current transaction scope.
     *
     * @param ts Transaction scope.
     * @return Results of the operation.
     */
    @Override
    public StoreResults DoGlobalExecute(IStoreTransactionScope ts) {
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpRemoveShardMapGlobal, StoreOperationRequestBuilder.RemoveShardMapGlobal(_shardMap));
    }

    /**
     * Invalidates the cache on unsuccessful commit of the GSM operation.
     *
     * @param result Operation result.
     */
    @Override
    public void DoGlobalUpdateCachePre(StoreResults result) {
        if (result.getResult() == StoreResult.ShardMapDoesNotExist) {
            // Remove cache entry.
            _shardMapManager.getCache().DeleteShardMap(_shardMap);
        }
    }

    /**
     * Handles errors from the GSM operation after the LSM operations.
     *
     * @param result Operation result.
     */
    @Override
    public void HandleDoGlobalExecuteError(StoreResults result) {
        if (result.getResult() != StoreResult.ShardMapDoesNotExist) {
            // Possible errors are:
            // StoreResult.ShardMapHasShards
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnShardMapManagerErrorGlobal(result, _shardMap, this.getOperationName(), StoreOperationRequestBuilder.SpRemoveShardMapGlobal);
        }
    }

    /**
     * Refreshes the cache on successful commit of the GSM operation.
     *
     * @param result Operation result.
     */
    @Override
    public void DoGlobalUpdateCachePost(StoreResults result) {
        assert result.getResult() == StoreResult.Success || result.getResult() == StoreResult.ShardMapDoesNotExist;

        if (result.getResult() == StoreResult.Success) {
            // Remove cache entry.
            _shardMapManager.getCache().DeleteShardMap(_shardMap);
        }
    }

    /**
     * Error category for store exception.
     */
    @Override
    protected ShardManagementErrorCategory getErrorCategory() {
        return ShardManagementErrorCategory.ShardMapManager;
    }

    @Override
    public void close() throws IOException {

    }
}