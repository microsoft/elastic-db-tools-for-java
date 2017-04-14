package com.microsoft.azure.elasticdb.shard.storeops.mapmanagerfactory;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.google.common.base.Stopwatch;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerCreateMode;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlShardMapManagerCredentials;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.Version;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.SqlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

/**
 * Deploys the SMM storage objects to the target GSM database.
 */
public class CreateShardMapManagerGlobalOperation extends StoreOperationGlobal {
    private final static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * Creation mode.
     */
    private ShardMapManagerCreateMode _createMode;

    /**
     * Target version of GSM to deploy, this will be used mainly for upgrade testing purpose.
     */
    private Version _targetVersion;

    /**
     * Constructs request for deploying SMM storage objects to target GSM database.
     *
     * @param credentials   Credentials for connection.
     * @param retryPolicy   Retry policy.
     * @param operationName Operation name, useful for diagnostics.
     * @param createMode    Creation mode.
     * @param targetVersion target version of store to deploy
     */
    public CreateShardMapManagerGlobalOperation(SqlShardMapManagerCredentials credentials, RetryPolicy retryPolicy, String operationName, ShardMapManagerCreateMode createMode, Version targetVersion) {
        super(credentials, retryPolicy, operationName);
        _createMode = createMode;
        _targetVersion = targetVersion;
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
    public StoreResults DoGlobalExecute(IStoreTransactionScope ts) {
        log.info("ShardMapManagerFactory {}, Started creating Global Shard Map structures.", this.getOperationName());

        Stopwatch stopwatch = Stopwatch.createStarted();

        StoreResults checkResult = ts.ExecuteCommandSingle(SqlUtils.getCheckIfExistsGlobalScript().get(0));

        // If we did find some store deployed.
        if (checkResult.getStoreVersion() != null) {
            // DevNote: We need to have a way to error out if versions do not match.
            if (_createMode == ShardMapManagerCreateMode.KeepExisting) {
                throw new ShardManagementException(ShardManagementErrorCategory.ShardMapManagerFactory, ShardManagementErrorCode.ShardMapManagerStoreAlreadyExists, Errors._Store_ShardMapManager_AlreadyExistsGlobal);
            }

            log.info("ShardMapManagerFactory {}, Dropping existing Global Shard Map structures.", this.getOperationName());

            ts.ExecuteCommandBatch(SqlUtils.getDropGlobalScript());
        }

        // Deploy initial version and run upgrade script to bring it to the specified version.
        ts.ExecuteCommandBatch(SqlUtils.getCreateGlobalScript());

        ts.ExecuteCommandBatch(SqlUtils.FilterUpgradeCommands(SqlUtils.getUpgradeGlobalScript(), _targetVersion));

        stopwatch.stop();

        log.info("ShardMapManagerFactory {}, Finished creating Global Shard Map structures. Duration:{}", this.getOperationName(), stopwatch.elapsed(TimeUnit.MILLISECONDS));

        return new StoreResults();
    }


    /**
     * Handles errors from the GSM operation after the LSM operations.
     *
     * @param result Operation result.
     */
    @Override
    public void HandleDoGlobalExecuteError(StoreResults result) {
        log.debug("Always expect Success or Exception from DoGlobalExecute.");
    }

    /**
     * Error category for store exception.
     */
    @Override
    protected ShardManagementErrorCategory getErrorCategory() {
        return ShardManagementErrorCategory.ShardMapManagerFactory;
    }

    @Override
    public void close() throws IOException {

    }
}