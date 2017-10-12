package com.microsoft.azure.elasticdb.shard.storeops.schemainformation;

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
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationErrorHandler;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;

/**
 * Delete schema info from GSM.
 */
public class RemoveShardingSchemaInfoGlobalOperation extends StoreOperationGlobal {

    /**
     * Name of schema info to remove.
     */
    private String schemaInfoName;

    /**
     * Constructs a request to delete schema info from GSM.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationName
     *            Operation name, useful for diagnostics.
     * @param schemaInfoName
     *            Name of schema info to delete.
     */
    public RemoveShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager,
            String operationName,
            String schemaInfoName) {
        super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), operationName);
        this.schemaInfoName = schemaInfoName;
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
        return ts.executeOperation(StoreOperationRequestBuilder.SP_REMOVE_SHARDING_SCHEMA_INFO_GLOBAL,
                StoreOperationRequestBuilder.removeShardingSchemaInfoGlobal(schemaInfoName));
    }

    /**
     * Handles errors from the GSM operation after the LSM operations.
     *
     * @param result
     *            Operation result.
     */
    @Override
    public void handleDoGlobalExecuteError(StoreResults result) {
        // Expected errors are:
        // StoreResult.SchemaInfoNameDoesNotExist:
        // StoreResult.MissingParametersForStoredProcedure:
        // StoreResult.StoreVersionMismatch:
        throw StoreOperationErrorHandler.onShardSchemaInfoErrorGlobal(result, schemaInfoName, this.getOperationName(),
                StoreOperationRequestBuilder.SP_REMOVE_SHARDING_SCHEMA_INFO_GLOBAL);
    }

    /**
     * Error category for store exception.
     */
    @Override
    protected ShardManagementErrorCategory getErrorCategory() {
        return ShardManagementErrorCategory.SchemaInfoCollection;
    }
}