package com.microsoft.azure.elasticdb.shard.storeops.map;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationGlobal;

/**
 * Gets all shards from given shard map from GSM.
 */
public class GetShardsGlobalOperation extends StoreOperationGlobal {
    /**
     * Shard map manager object.
     */
    private ShardMapManager _shardMapManager;

    /**
     * Shard map for which shards are being requested.
     */
    private IStoreShardMap _shardMap;

    /**
     * Constructs request to get all shards for given shard map from GSM.
     *
     * @param operationName   Operation name, useful for diagnostics.
     * @param shardMapManager Shard map manager object.
     * @param shardMap        Shard map for which shards are being requested.
     */
    public GetShardsGlobalOperation(String operationName, ShardMapManager shardMapManager, IStoreShardMap shardMap) {
        super(shardMapManager.Credentials, shardMapManager.RetryPolicy, operationName);
        _shardMapManager = shardMapManager;
        _shardMap = shardMap;
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
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpGetAllShardsGlobal, StoreOperationRequestBuilder.GetAllShardsGlobal(_shardMap));
    }

    /**
     * Handles errors from the GSM operation after the LSM operations.
     *
     * @param result Operation result.
     */
    @Override
    public void HandleDoGlobalExecuteError(IStoreResults result) {
        if (result.Result == StoreResult.ShardMapDoesNotExist) {
            // Remove shard map from cache.
            _shardMapManager.Cache.DeleteShardMap(_shardMap);
        }

        // Possible errors are:
        // StoreResult.ShardMapDoesNotExist
        // StoreResult.StoreVersionMismatch
        // StoreResult.MissingParametersForStoredProcedure
        throw StoreOperationErrorHandler.OnShardMapErrorGlobal(result, _shardMap, null, ShardManagementErrorCategory.ShardMap, this.OperationName, StoreOperationRequestBuilder.SpGetAllShardsGlobal); // shard
    }

    /**
     * Error category for store exception.
     */
    @Override
    protected ShardManagementErrorCategory getErrorCategory() {
        return ShardManagementErrorCategory.ShardMap;
    }
}