package com.microsoft.azure.elasticdb.shard.storeops.upgrade;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.google.common.base.Stopwatch;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.Version;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationLocal;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.SqlUtils;

import java.io.IOException;

/**
 * Upgrade store structures at specified location.
 */
public class UpgradeStoreLocalOperation extends StoreOperationLocal {
    /**
     * Target version of LSM to deploy, this will be used mainly for upgrade testing purpose.
     */
    private Version _targetVersion;

    /**
     * Constructs request to upgrade store hosting LSM.
     *
     * @param shardMapManager Shard map manager object.
     * @param location        Store location to upgrade.
     * @param operationName   Operation name, useful for diagnostics.
     * @param targetVersion   Target version to upgrade.
     */
    public UpgradeStoreLocalOperation(ShardMapManager shardMapManager, ShardLocation location, String operationName, Version targetVersion) {
        super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), location, operationName);
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
     * Execute the operation against LSM in the current transaction scope.
     *
     * @param ts Transaction scope.
     * @return Results of the operation.
     */
    @Override
    public StoreResults DoLocalExecute(IStoreTransactionScope ts) {
        //TODO: TraceHelper.Tracer.TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManagerFactory, this.getOperationName(), "Started upgrading Local Shard Map structures at location{}", super.getLocation());

        Stopwatch stopwatch = Stopwatch.createStarted();

        StoreResults checkResult = ts.ExecuteCommandSingle(SqlUtils.getCheckIfExistsLocalScript().get(0));
        if (checkResult.getStoreVersion() == null) {
            // DEVNOTE(apurvs): do we want to throw here if LSM is not already deployed?
            // deploy initial version of LSM, if not found.
            ts.ExecuteCommandBatch(SqlUtils.getCreateLocalScript());
        }

        if (checkResult.getStoreVersion() == null) {//TODO:  || checkResult.getStoreVersion().getVersion() < _targetVersion) {
            if (checkResult.getStoreVersion() == null) {
                ts.ExecuteCommandBatch(SqlUtils.FilterUpgradeCommands(SqlUtils.getUpgradeLocalScript(), _targetVersion));
            } else {
                ts.ExecuteCommandBatch(SqlUtils.FilterUpgradeCommands(SqlUtils.getUpgradeLocalScript(), _targetVersion, checkResult.getStoreVersion()));
            }

            // Read LSM version again after upgrade.
            checkResult = ts.ExecuteCommandSingle(SqlUtils.getCheckIfExistsLocalScript().get(0));

            stopwatch.stop();

            //TODO: TraceHelper.Tracer.TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManagerFactory, this.getOperationName(), "Finished upgrading store at location {0}. Duration:{}", super.getLocation(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } else {
            //TODO: TraceHelper.Tracer.TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManagerFactory, this.getOperationName(), "Local Shard Map at location {0} has version {1} equal to or higher than Client library version {2}, skipping upgrade.", super.getLocation(), checkResult.getStoreVersion(), GlobalConstants.GsmVersionClient);
        }
        return checkResult;
    }

    @Override
    public void HandleDoLocalExecuteError(StoreResults result) {
        throw new ShardManagementException(ShardManagementErrorCategory.ShardMapManager, ShardManagementErrorCode.StorageOperationFailure, Errors._Store_SqlExceptionLocal, getOperationName());
    }

    @Override
    public void close() throws IOException {

    }
}
