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
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreResult;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationLocal;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.SqlUtils;

/**
 * Obtains all the shard maps and shards from an LSM.
 */
public class CheckShardLocalOperation extends StoreOperationLocal {

    /**
     * Constructs request for obtaining all the shard maps and shards from an LSM.
     *
     * @param operationName
     *            Operation name.
     * @param shardMapManager
     *            Shard map manager.
     * @param location
     *            Location of the LSM.
     */
    public CheckShardLocalOperation(String operationName,
            ShardMapManager shardMapManager,
            ShardLocation location) {
        super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), location, operationName);
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
     * @param ts
     *            Transaction scope.
     * @return Results of the operation.
     */
    @Override
    public StoreResults doLocalExecute(IStoreTransactionScope ts) {
        StoreResults result = ts.executeCommandSingle(SqlUtils.getCheckIfExistsLocalScript().get(0));

        if (result.getStoreVersion() == null) {
            // Shard not deployed, which is an error condition.
            throw new ShardManagementException(ShardManagementErrorCategory.Recovery, ShardManagementErrorCode.ShardNotValid,
                    Errors._Recovery_ShardNotValid, this.getLocation(), this.getOperationName());
        }

        assert result.getResult() == StoreResult.Success;

        return result;
    }

    /**
     * Handles errors from the LSM operation.
     *
     * @param result
     *            Operation result.
     */
    @Override
    public void handleDoLocalExecuteError(StoreResults result) {
        // Debug.Fail("Not expecting call because failure handled in the DoLocalExecute method.");
    }
}