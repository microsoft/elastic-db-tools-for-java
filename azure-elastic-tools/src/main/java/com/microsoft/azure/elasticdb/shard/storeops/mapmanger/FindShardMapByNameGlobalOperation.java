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
 * Finds shard map with given name from GSM.
 */
public class FindShardMapByNameGlobalOperation extends StoreOperationGlobal {

    /**
     * Shard map manager object.
     */
    private ShardMapManager shardMapManager;

    /**
     * Name of shard map being searched.
     */
    private String shardMapName;

    /**
     * Constructs request to find shard map with given name from GSM.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationName
     *            Operation name, useful for diagnostics.
     * @param shardMapName
     *            Name of the shard map being searched.
     */
    public FindShardMapByNameGlobalOperation(ShardMapManager shardMapManager,
            String operationName,
            String shardMapName) {
        super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), operationName);
        this.shardMapManager = shardMapManager;
        this.shardMapName = shardMapName;
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
        return ts.executeOperation(StoreOperationRequestBuilder.SP_FIND_SHARD_MAP_BY_NAME_GLOBAL,
                StoreOperationRequestBuilder.findShardMapByNameGlobal(shardMapName));
    }

    /**
     * Handles errors from the GSM operation after the LSM operations.
     *
     * @param result
     *            Operation result.
     */
    @Override
    public void handleDoGlobalExecuteError(StoreResults result) {
        // Possible errors are:
        // StoreResult.StoreVersionMismatch
        // StoreResult.MissingParametersForStoredProcedure
        throw StoreOperationErrorHandler.onShardMapManagerErrorGlobal(result, null, this.getOperationName(),
                StoreOperationRequestBuilder.SP_FIND_SHARD_MAP_BY_NAME_GLOBAL);
    }

    /**
     * Refreshes the cache on successful commit of the GSM operation.
     *
     * @param result
     *            Result of the operation.
     */
    @Override
    public void doGlobalUpdateCachePost(StoreResults result) {
        assert result.getResult() == StoreResult.Success;

        // Add cache entry.
        for (StoreShardMap ssm : result.getStoreShardMaps()) {
            shardMapManager.getCache().addOrUpdateShardMap(ssm);
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
    public void close() {

    }
}