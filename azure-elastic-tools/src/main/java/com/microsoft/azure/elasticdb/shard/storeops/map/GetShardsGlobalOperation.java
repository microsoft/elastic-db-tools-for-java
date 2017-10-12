package com.microsoft.azure.elasticdb.shard.storeops.map;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreResult;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationErrorHandler;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;

/**
 * Gets all shards from given shard map from GSM.
 */
public class GetShardsGlobalOperation extends StoreOperationGlobal {

    /**
     * Shard map manager object.
     */
    private ShardMapManager shardMapManager;

    /**
     * Shard map for which shards are being requested.
     */
    private StoreShardMap shardMap;

    /**
     * Constructs request to get all shards for given shard map from GSM.
     *
     * @param operationName
     *            Operation name, useful for diagnostics.
     * @param shardMapManager
     *            Shard map manager object.
     * @param shardMap
     *            Shard map for which shards are being requested.
     */
    public GetShardsGlobalOperation(String operationName,
            ShardMapManager shardMapManager,
            StoreShardMap shardMap) {
        super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), operationName);
        this.shardMapManager = shardMapManager;
        this.shardMap = shardMap;
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
     * @param ts
     *            Transaction scope.
     * @return Results of the operation.
     */
    @Override
    public StoreResults doGlobalExecute(IStoreTransactionScope ts) {
        return ts.executeOperation(StoreOperationRequestBuilder.SP_GET_ALL_SHARDS_GLOBAL, StoreOperationRequestBuilder.getAllShardsGlobal(shardMap));
    }

    /**
     * Handles errors from the GSM operation after the LSM operations.
     *
     * @param result
     *            Operation result.
     */
    @Override
    public void handleDoGlobalExecuteError(StoreResults result) {
        if (result.getResult() == StoreResult.ShardMapDoesNotExist) {
            // Remove shard map from cache.
            shardMapManager.getCache().deleteShardMap(shardMap);
        }

        // Possible errors are:
        // StoreResult.ShardMapDoesNotExist
        // StoreResult.StoreVersionMismatch
        // StoreResult.MissingParametersForStoredProcedure
        throw StoreOperationErrorHandler.onShardMapErrorGlobal(result, shardMap, null, ShardManagementErrorCategory.ShardMap, this.getOperationName(),
                StoreOperationRequestBuilder.SP_GET_ALL_SHARDS_GLOBAL); // shard
    }

    /**
     * Error category for store exception.
     */
    @Override
    protected ShardManagementErrorCategory getErrorCategory() {
        return ShardManagementErrorCategory.ShardMap;
    }
}