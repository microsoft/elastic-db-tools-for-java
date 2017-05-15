package com.microsoft.azure.elasticdb.shard.storeops.mapmanger;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreResult;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationErrorHandler;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;

/**
 * Gets all shard maps from GSM.
 */
public class GetShardMapsGlobalOperation extends StoreOperationGlobal {

  /**
   * Shard map manager object.
   */
  private ShardMapManager shardMapManager;

  /**
   * Constructs request to get all shard maps from GSM.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationName Operation name, useful for diagnostics.
   */
  public GetShardMapsGlobalOperation(ShardMapManager shardMapManager, String operationName) {
    super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), operationName);
    this.shardMapManager = shardMapManager;
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
   * @param ts Transaction scope.
   * @return Results of the operation.
   */
  @Override
  public StoreResults doGlobalExecute(IStoreTransactionScope ts) {
    return ts.executeOperation(StoreOperationRequestBuilder.SP_GET_ALL_SHARD_MAPS_GLOBAL,
        StoreOperationRequestBuilder.getAllShardMapsGlobal());
  }

  /**
   * Handles errors from the GSM operation after the LSM operations.
   *
   * @param result Operation result.
   */
  @Override
  public void handleDoGlobalExecuteError(StoreResults result) {
    // Possible errors are:
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler
        .onShardMapManagerErrorGlobal(result, null, this.getOperationName(),
            StoreOperationRequestBuilder.SP_GET_ALL_SHARD_MAPS_GLOBAL);
  }

  /**
   * Refreshes the cache on successful commit of the GSM operation.
   *
   * @param result Operation result.
   */
  @Override
  public void doGlobalUpdateCachePost(StoreResults result) {
    assert result.getResult() == StoreResult.Success;

    // Add cache entry.
    for (StoreShardMap ssm : result.getStoreShardMaps()) {
      shardMapManager.getCache().addOrUpdateShardMap(ssm);
    }
  }

  /**
   * Error category for store exception.
   */
  @Override
  protected ShardManagementErrorCategory getErrorCategory() {
    return ShardManagementErrorCategory.ShardMapManager;
  }
}