package com.microsoft.azure.elasticdb.shard.storeops.upgrade;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.Version;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.utils.SqlUtils;
import java.io.IOException;

/**
 * Upgrade store hosting GSM.
 */
public class UpgradeStoreGlobalOperation extends StoreOperationGlobal {

  /**
   * Target version of GSM to deploy, this will be used mainly for upgrade testing purpose.
   */
  private Version _targetVersion;

  /**
   * Constructs request to upgrade store hosting GSM.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationName Operation name, useful for diagnostics.
   * @param targetVersion Target version to upgrade.
   */
  public UpgradeStoreGlobalOperation(ShardMapManager shardMapManager, String operationName,
      Version targetVersion) {
    super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), operationName);
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
    //TODO: TraceHelper.Tracer.TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManagerFactory, this.getOperationName(), "Started upgrading Global Shard Map structures.");

    StoreResults checkResult = ts
        .ExecuteCommandSingle(SqlUtils.getCheckIfExistsGlobalScript().get(0));

    //Debug.Assert(checkResult.StoreVersion != null, "GSM store structures not found.");

        /*if (checkResult.getStoreVersion().getVersion() < _targetVersion) {
            Stopwatch stopwatch = Stopwatch.createStarted();

            ts.ExecuteCommandBatch(SqlUtils.FilterUpgradeCommands(SqlUtils.getUpgradeGlobalScript(), _targetVersion, checkResult.getStoreVersion().getVersion()));

            // read GSM version after upgrade.
            checkResult = ts.ExecuteCommandSingle(SqlUtils.getCheckIfExistsGlobalScript().get(0));

            // DEVNOTE(apurvs): verify (checkResult.StoreVersion == GlobalConstants.GsmVersionClient) and throw on failure.

            stopwatch.stop();

            //TODO: TraceHelper.Tracer.TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManagerFactory, this.getOperationName(), "Finished upgrading Global Shard Map. Duration:{}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } else {
            //TODO: TraceHelper.Tracer.TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManagerFactory, this.getOperationName(), "Global Shard Map is at a version {} equal to or higher than Client library version {1}, skipping upgrade.", (checkResult.getStoreVersion() == null) ? "" : checkResult.getStoreVersion().getVersion().toString(), GlobalConstants.GsmVersionClient);
        }*/

    return checkResult;
  }

  /**
   * Handles errors from the GSM operation after the LSM operations.
   *
   * @param result Operation result.
   */
  @Override
  public void HandleDoGlobalExecuteError(StoreResults result) {
    //Debug.Fail("Always expect Success or Exception from DoGlobalExecute.");
  }

  /**
   * Error category for store exception.
   */
  @Override
  protected ShardManagementErrorCategory getErrorCategory() {
    return ShardManagementErrorCategory.ShardMapManager;
  }

  @Override
  public void close() throws IOException {

  }
}
