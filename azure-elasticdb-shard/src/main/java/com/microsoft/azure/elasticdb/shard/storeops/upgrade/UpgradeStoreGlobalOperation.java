package com.microsoft.azure.elasticdb.shard.storeops.upgrade;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.google.common.base.Stopwatch;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.Version;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.utils.GlobalConstants;
import com.microsoft.azure.elasticdb.shard.utils.SqlUtils;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Upgrade store hosting GSM.
 */
public class UpgradeStoreGlobalOperation extends StoreOperationGlobal {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  /**
   * Target version of GSM to deploy, this will be used mainly for upgrade testing purpose.
   */
  private Version targetVersion;

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
    this.targetVersion = targetVersion;
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
  public StoreResults doGlobalExecute(IStoreTransactionScope ts) {
    log.info("ShardMapManagerFactory {} Started upgrading Global Shard Map structures.",
        this.getOperationName());

    StoreResults checkResult = ts.executeCommandSingle(
        SqlUtils.getCheckIfExistsGlobalScript().get(0));

    //Debug.Assert(checkResult.StoreVersion != null, "GSM store structures not found.");

    if (Version.isFirstGreaterThan(targetVersion, checkResult.getStoreVersion())) {
      Stopwatch stopwatch = Stopwatch.createStarted();

      ts.executeCommandBatch(SqlUtils.filterUpgradeCommands(SqlUtils.getUpgradeGlobalScript(),
          targetVersion, checkResult.getStoreVersion()));

      // read GSM version after upgrade.
      checkResult = ts.executeCommandSingle(SqlUtils.getCheckIfExistsGlobalScript().get(0));

      // DEVNOTE(apurvs): verify (checkResult.StoreVersion == GlobalConstants.GsmVersionClient)
      // and throw on failure.

      stopwatch.stop();

      log.info("ShardMapManagerFactory {} Finished upgrading Global Shard Map. Duration:{}",
          this.getOperationName(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
    } else {
      log.error(
          "ShardMapManagerFactory {} Global Shard Map is at a version {} equal to or higher than"
              + "Client library version {}, skipping upgrade.", this.getOperationName(),
          (checkResult.getStoreVersion() == null) ? "" : checkResult.getStoreVersion().toString(),
          GlobalConstants.GsmVersionClient);
    }

    return checkResult;
  }

  /**
   * Handles errors from the GSM operation after the LSM operations.
   *
   * @param result Operation result.
   */
  @Override
  public void handleDoGlobalExecuteError(StoreResults result) {
    //Debug.Fail("Always expect Success or Exception from DoGlobalExecute.");
  }

  /**
   * Error category for store exception.
   */
  @Override
  protected ShardManagementErrorCategory getErrorCategory() {
    return ShardManagementErrorCategory.ShardMapManager;
  }
}
