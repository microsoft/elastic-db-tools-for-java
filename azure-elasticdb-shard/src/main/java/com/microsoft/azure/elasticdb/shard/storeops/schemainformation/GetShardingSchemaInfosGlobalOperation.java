package com.microsoft.azure.elasticdb.shard.storeops.schemainformation;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreResults;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationErrorHandler;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;

import java.io.IOException;

/**
 * Gets all schema infos from GSM.
 */
public class GetShardingSchemaInfosGlobalOperation extends StoreOperationGlobal {
    /**
     * Constructs a request to get all schema info objects from GSM.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationName   Operation name, useful for diagnostics.
     */
    public GetShardingSchemaInfosGlobalOperation(ShardMapManager shardMapManager, String operationName) {
        super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), operationName);
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
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpGetAllShardingSchemaInfosGlobal, StoreOperationRequestBuilder.GetAllShardingSchemaInfosGlobal());
    }

    /**
     * Handles errors from the GSM operation after the LSM operations.
     *
     * @param result Operation result.
     */
    @Override
    public void HandleDoGlobalExecuteError(IStoreResults result) {
        // Expected errors are:
        // StoreResult.MissingParametersForStoredProcedure:
        // StoreResult.StoreVersionMismatch:
        throw StoreOperationErrorHandler.OnShardSchemaInfoErrorGlobal(result, "*", this.getOperationName(), StoreOperationRequestBuilder.SpGetAllShardingSchemaInfosGlobal);
    }

    /**
     * Error category for store exception.
     */
    @Override
    protected ShardManagementErrorCategory getErrorCategory() {
        return ShardManagementErrorCategory.SchemaInfoCollection;
    }

    @Override
    public void close() throws IOException {

    }
}