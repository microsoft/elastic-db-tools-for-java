package com.microsoft.azure.elasticdb.shard.storeops.mapmanger;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreResults;
import com.microsoft.azure.elasticdb.shard.store.IStoreShardMap;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreResult;
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
    private ShardMapManager _shardMapManager;

    /**
     * Name of shard map being searched.
     */
    private String _shardMapName;

    /**
     * Constructs request to find shard map with given name from GSM.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationName   Operation name, useful for diagnostics.
     * @param shardMapName    Name of the shard map being searched.
     */
    public FindShardMapByNameGlobalOperation(ShardMapManager shardMapManager, String operationName, String shardMapName) {
        super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), operationName);
        _shardMapManager = shardMapManager;
        _shardMapName = shardMapName;
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
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpFindShardMapByNameGlobal
                , StoreOperationRequestBuilder.FindShardMapByNameGlobal(_shardMapName));
    }

    /**
     * Handles errors from the GSM operation after the LSM operations.
     *
     * @param result Operation result.
     */
    @Override
    public void HandleDoGlobalExecuteError(IStoreResults result) {
        // Possible errors are:
        // StoreResult.StoreVersionMismatch
        // StoreResult.MissingParametersForStoredProcedure
        throw StoreOperationErrorHandler.OnShardMapManagerErrorGlobal(result, null, this.getOperationName(), StoreOperationRequestBuilder.SpFindShardMapByNameGlobal);
    }

    /**
     * Refreshes the cache on successful commit of the GSM operation.
     *
     * @param result Result of the operation.
     */
    @Override
    public void DoGlobalUpdateCachePost(IStoreResults result) {
        assert result.getResult() == StoreResult.Success;

        // Add cache entry.
        for (IStoreShardMap ssm : result.getStoreShardMaps()) {
            _shardMapManager.getCache().AddOrUpdateShardMap(ssm);
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