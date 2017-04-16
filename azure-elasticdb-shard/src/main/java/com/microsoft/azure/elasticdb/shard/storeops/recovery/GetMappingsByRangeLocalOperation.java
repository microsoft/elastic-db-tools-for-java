package com.microsoft.azure.elasticdb.shard.storeops.recovery;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.*;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationErrorHandler;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationLocal;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;

import java.io.IOException;

/**
 * Obtains all the shard maps and shards from an LSM.
 */
public class GetMappingsByRangeLocalOperation extends StoreOperationLocal {
    /**
     * Local shard map.
     */
    private StoreShardMap _shardMap;

    /**
     * Local shard.
     */
    private StoreShard _shard;

    /**
     * Range to get mappings from.
     */
    private ShardRange _range;

    /**
     * Ignore ShardMapNotFound error.
     */
    private boolean _ignoreFailure;

    /**
     * Constructs request for obtaining all the shard maps and shards from an LSM.
     *
     * @param shardMapManager Shard map manager.
     * @param location        Location of the LSM.
     * @param operationName   Operation name.
     * @param shardMap        Local shard map.
     * @param shard           Local shard.
     * @param range           Optional range to get mappings from.
     * @param ignoreFailure   Ignore shard map not found error.
     */
    public GetMappingsByRangeLocalOperation(ShardMapManager shardMapManager, ShardLocation location, String operationName, StoreShardMap shardMap, StoreShard shard, ShardRange range, boolean ignoreFailure) {
        super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), location, operationName);
        assert shard != null;

        _shardMap = shardMap;
        _shard = shard;
        _range = range;
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
     * Execute the operation against LSM in the current transaction scope.
     *
     * @param ts Transaction scope.
     * @return Results of the operation.
     */
    @Override
    public StoreResults DoLocalExecute(IStoreTransactionScope ts) {
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpGetAllShardMappingsLocal, StoreOperationRequestBuilder.GetAllShardMappingsLocal(_shardMap, _shard, _range));
    }

    /**
     * Handles errors from the LSM operation.
     *
     * @param result Operation result.
     */
    @Override
    public void HandleDoLocalExecuteError(StoreResults result) {
        if (!_ignoreFailure || result.getResult() != StoreResult.ShardMapDoesNotExist) {
            // Possible errors are:
            // StoreResult.ShardMapDoesNotExist
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnRecoveryErrorLocal(result, _shardMap, this.getLocation(), ShardManagementErrorCategory.Recovery, this.getOperationName(), StoreOperationRequestBuilder.SpGetAllShardMappingsLocal);
        }
    }

    @Override
    public void close() throws IOException {

    }
}