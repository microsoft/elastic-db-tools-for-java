package com.microsoft.azure.elasticdb.shard.storeops.mapmanger;

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
 * Removes given shard map from GSM.
 */
public class RemoveShardMapGlobalOperation extends StoreOperationGlobal {

    /**
     * Shard map manager object.
     */
    private ShardMapManager shardMapManager;

    /**
     * Shard map to remove.
     */
    private StoreShardMap shardMap;

    /**
     * Constructs request to remove given shard map from GSM.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationName
     *            Operation name, useful for diagnostics.
     * @param shardMap
     *            Shard map to remove.
     */
    public RemoveShardMapGlobalOperation(ShardMapManager shardMapManager,
            String operationName,
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
        return false;
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
        return ts.executeOperation(StoreOperationRequestBuilder.SP_REMOVE_SHARD_MAP_GLOBAL,
                StoreOperationRequestBuilder.removeShardMapGlobal(shardMap));
    }

    /**
     * Invalidates the cache on unsuccessful commit of the GSM operation.
     *
     * @param result
     *            Operation result.
     */
    @Override
    public void doGlobalUpdateCachePre(StoreResults result) {
        if (result.getResult() == StoreResult.ShardMapDoesNotExist) {
            // Remove cache entry.
            shardMapManager.getCache().deleteShardMap(shardMap);
        }
    }

    /**
     * Handles errors from the GSM operation after the LSM operations.
     *
     * @param result
     *            Operation result.
     */
    @Override
    public void handleDoGlobalExecuteError(StoreResults result) {
        if (result.getResult() != StoreResult.ShardMapDoesNotExist) {
            // Possible errors are:
            // StoreResult.ShardMapHasShards
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.onShardMapManagerErrorGlobal(result, shardMap, this.getOperationName(),
                    StoreOperationRequestBuilder.SP_REMOVE_SHARD_MAP_GLOBAL);
        }
    }

    /**
     * Refreshes the cache on successful commit of the GSM operation.
     *
     * @param result
     *            Operation result.
     */
    @Override
    public void doGlobalUpdateCachePost(StoreResults result) {
        assert result.getResult() == StoreResult.Success || result.getResult() == StoreResult.ShardMapDoesNotExist;

        if (result.getResult() == StoreResult.Success) {
            // Remove cache entry.
            shardMapManager.getCache().deleteShardMap(shardMap);
        }
    }

    /**
     * Error category for store exception.
     */
    @Override
    protected ShardManagementErrorCategory getErrorCategory() {
        return ShardManagementErrorCategory.ShardMapManager;
    }
}