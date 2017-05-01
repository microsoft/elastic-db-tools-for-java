package com.microsoft.azure.elasticdb.shard.storeops.upgrade;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

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
import com.microsoft.azure.elasticdb.shard.utils.GlobalConstants;
import com.microsoft.azure.elasticdb.shard.utils.SqlUtils;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Upgrade store structures at specified location.
 */
public class UpgradeStoreLocalOperation extends StoreOperationLocal {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  /**
   * Target version of LSM to deploy, this will be used mainly for upgrade testing purpose.
   */
  private Version targetVersion;

  /**
   * Constructs request to upgrade store hosting LSM.
   *
   * @param shardMapManager Shard map manager object.
   * @param location Store location to upgrade.
   * @param operationName Operation name, useful for diagnostics.
   * @param targetVersion Target version to upgrade.
   */
  public UpgradeStoreLocalOperation(ShardMapManager shardMapManager, ShardLocation location,
      String operationName, Version targetVersion) {
    super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), location,
        operationName);
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
   * Execute the operation against LSM in the current transaction scope.
   *
   * @param ts Transaction scope.
   * @return Results of the operation.
   */
  @Override
  public StoreResults doLocalExecute(IStoreTransactionScope ts) {
    log.info("ShardMapManagerFactory {} Started upgrading Local Shard Map structures"
        + "at location {}", this.getOperationName(), super.getLocation());

    Stopwatch stopwatch = Stopwatch.createStarted();

    StoreResults checkResult = ts
        .executeCommandSingle(SqlUtils.getCheckIfExistsLocalScript().get(0));
    if (checkResult.getStoreVersion() == null) {
      // DEVNOTE(apurvs): do we want to throw here if LSM is not already deployed?
      // deploy initial version of LSM, if not found.
      ts.executeCommandBatch(SqlUtils.getCreateLocalScript());
    }

    if (checkResult.getStoreVersion() == null
        || Version.isFirstGreaterThan(targetVersion, checkResult.getStoreVersion())) {
      if (checkResult.getStoreVersion() == null) {
        ts.executeCommandBatch(
            SqlUtils.filterUpgradeCommands(SqlUtils.getUpgradeLocalScript(), targetVersion));
      } else {
        ts.executeCommandBatch(SqlUtils
            .filterUpgradeCommands(SqlUtils.getUpgradeLocalScript(), targetVersion,
                checkResult.getStoreVersion()));
      }

      // Read LSM version again after upgrade.
      checkResult = ts.executeCommandSingle(SqlUtils.getCheckIfExistsLocalScript().get(0));

      stopwatch.stop();

      log.info("ShardMapManagerFactory {} Finished upgrading store at location {}. Duration:{}",
          this.getOperationName(), super.getLocation(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
    } else {
      log.error("ShardMapManagerFactory {} Local Shard Map at location {} has version {} equal to"
              + "or higher than Client library version {}, skipping upgrade.",
          this.getOperationName(), super.getLocation(), checkResult.getStoreVersion(),
          GlobalConstants.GsmVersionClient);
    }
    return checkResult;
  }

  @Override
  public void handleDoLocalExecuteError(StoreResults result) {
    throw new ShardManagementException(ShardManagementErrorCategory.ShardMapManager,
        ShardManagementErrorCode.StorageOperationFailure, Errors._Store_SqlExceptionLocal,
        getOperationName());
  }

  @Override
  public void close() throws IOException {

  }
}
