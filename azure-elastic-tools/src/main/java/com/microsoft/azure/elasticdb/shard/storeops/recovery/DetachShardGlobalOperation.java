package com.microsoft.azure.elasticdb.shard.storeops.recovery;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationErrorHandler;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;

/**
 * Detaches the given shard and corresponding mapping information to the GSM database.
 */
public class DetachShardGlobalOperation extends StoreOperationGlobal {

    /**
     * Location to be detached.
     */
    private ShardLocation location;

    /**
     * Shard map from which shard is being detached.
     */
    private String shardMapName;

    /**
     * Constructs request for Detaching the given shard and mapping information to the GSM database.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationName
     *            Operation name.
     * @param location
     *            Location to be detached.
     * @param shardMapName
     *            Shard map from which shard is being detached.
     */
    public DetachShardGlobalOperation(ShardMapManager shardMapManager,
            String operationName,
            ShardLocation location,
            String shardMapName) {
        super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), operationName);
        this.shardMapName = shardMapName;
        this.location = location;
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
        return ts.executeOperation(StoreOperationRequestBuilder.SP_DETACH_SHARD_GLOBAL,
                StoreOperationRequestBuilder.detachShardGlobal(shardMapName, location));
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
        throw StoreOperationErrorHandler.onRecoveryErrorGlobal(result, null, null, ShardManagementErrorCategory.Recovery, this.getOperationName(),
                StoreOperationRequestBuilder.SP_DETACH_SHARD_GLOBAL);
    }

    /**
     * Error category for store exception.
     */
    @Override
    protected ShardManagementErrorCategory getErrorCategory() {
        return ShardManagementErrorCategory.Recovery;
    }
}