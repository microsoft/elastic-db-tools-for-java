package com.microsoft.azure.elasticdb.shard.storeops.schemainformation;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreResults;
import com.microsoft.azure.elasticdb.shard.store.IStoreSchemaInfo;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationErrorHandler;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;

import java.io.IOException;

/**
 * Updates schema info in GSM.
 */
public class UpdateShardingSchemaInfoGlobalOperation extends StoreOperationGlobal {
    /**
     * Schema info to update.
     */
    private IStoreSchemaInfo _schemaInfo;

    /**
     * Constructs a request to update schema info to GSM.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationName   Operation name, useful for diagnostics.
     * @param schemaInfo      Schema info to update.
     */
    public UpdateShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreSchemaInfo schemaInfo) {
        super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), operationName);
        _schemaInfo = schemaInfo;
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
    public IStoreResults DoGlobalExecute(IStoreTransactionScope ts) {
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpUpdateShardingSchemaInfoGlobal, StoreOperationRequestBuilder.UpdateShardingSchemaInfoGlobal(_schemaInfo));
    }

    /**
     * Handles errors from the GSM operation after the LSM operations.
     *
     * @param result Operation result.
     */
    @Override
    public void HandleDoGlobalExecuteError(IStoreResults result) {
        // Expected errors are:
        // StoreResult.SchemaInfoNameDoesNotExist:
        // StoreResult.MissingParametersForStoredProcedure:
        // StoreResult.StoreVersionMismatch:
        throw StoreOperationErrorHandler.OnShardSchemaInfoErrorGlobal(result, _schemaInfo.getName(), this.getOperationName(), StoreOperationRequestBuilder.SpUpdateShardingSchemaInfoGlobal);
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