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
 * Gets all schema infos from GSM.
 */
public class GetShardingSchemaInfosGlobalOperation extends StoreOperationGlobal {

    /**
     * Constructs a request to get all schema info objects from GSM.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationName
     *            Operation name, useful for diagnostics.
     */
    public GetShardingSchemaInfosGlobalOperation(ShardMapManager shardMapManager,
            String operationName) {
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
     * @param ts
     *            Transaction scope.
     * @return Results of the operation.
     */
    @Override
    public StoreResults doGlobalExecute(IStoreTransactionScope ts) {
        return ts.executeOperation(StoreOperationRequestBuilder.SP_GET_ALL_SHARDING_SCHEMA_INFOS_GLOBAL,
                StoreOperationRequestBuilder.getAllShardingSchemaInfosGlobal());
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
        // StoreResult.MissingParametersForStoredProcedure:
        // StoreResult.StoreVersionMismatch:
        throw StoreOperationErrorHandler.onShardSchemaInfoErrorGlobal(result, "*", this.getOperationName(),
                StoreOperationRequestBuilder.SP_GET_ALL_SHARDING_SCHEMA_INFOS_GLOBAL);
    }

    /**
     * Error category for store exception.
     */
    @Override
    protected ShardManagementErrorCategory getErrorCategory() {
        return ShardManagementErrorCategory.SchemaInfoCollection;
    }
}