package com.microsoft.azure.elasticdb.shard.storeops.mapmanagerfactory;

import java.util.List;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlShardMapManagerCredentials;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreResult;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.SqlUtils;

/**
 * Obtains the shard map manager object if the GSM has the SMM objects in it.
 */
public class GetShardMapManagerGlobalOperation extends StoreOperationGlobal {

    /**
     * Whether to throw exception on failure.
     */
    private boolean throwOnFailure;

    /**
     * Constructs request for obtaining shard map manager object if the GSM has the SMM objects in it.
     *
     * @param credentials
     *            Credentials for connection.
     * @param retryPolicy
     *            Retry policy.
     * @param operationName
     *            Operation name, useful for diagnostics.
     * @param throwOnFailure
     *            Whether to throw exception on failure or return error code.
     */
    public GetShardMapManagerGlobalOperation(SqlShardMapManagerCredentials credentials,
            RetryPolicy retryPolicy,
            String operationName,
            boolean throwOnFailure) {
        super(credentials, retryPolicy, operationName);
        this.throwOnFailure = throwOnFailure;
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
        StoreResults returnedResult;
        List<StringBuilder> globalScript = SqlUtils.getCheckIfExistsGlobalScript();
        StringBuilder command = globalScript.get(0);
        StoreResults result = ts.executeCommandSingle(command);

        returnedResult = new StoreResults();

        // If we did not find some store deployed.
        if (result.getStoreVersion() == null) {
            returnedResult.setResult(StoreResult.Failure);
        }
        else {
            // DevNote: We need to have a way to error out if versions do not match.
            // we can potentially call upgrade here to get to latest version.
            // Should this be exposed as a new parameter ?
            returnedResult.setResult(StoreResult.Success);
        }

        return returnedResult;
    }

    /**
     * Handles errors from the GSM operation after the LSM operations.
     *
     * @param result
     *            Operation result.
     */
    @Override
    public void handleDoGlobalExecuteError(StoreResults result) {
        if (throwOnFailure) {
            throw new ShardManagementException(ShardManagementErrorCategory.ShardMapManagerFactory,
                    ShardManagementErrorCode.ShardMapManagerStoreDoesNotExist, Errors._Store_ShardMapManager_DoesNotExistGlobal);
        }
    }

    /**
     * Error category for store exception.
     */
    @Override
    protected ShardManagementErrorCategory getErrorCategory() {
        return ShardManagementErrorCategory.ShardMapManagerFactory;
    }
}
