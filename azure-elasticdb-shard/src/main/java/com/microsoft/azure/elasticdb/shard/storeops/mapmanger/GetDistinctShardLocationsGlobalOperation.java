package com.microsoft.azure.elasticdb.shard.storeops.mapmanger;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationErrorHandler;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;

/**
 * Gets all distinct shard locations from GSM.
 */
public class GetDistinctShardLocationsGlobalOperation extends StoreOperationGlobal {

  /**
   * Constructs request to get distinct shard locations from GSM.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationName Operation name, useful for diagnostics.
   */
  public GetDistinctShardLocationsGlobalOperation(ShardMapManager shardMapManager,
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
   * @param ts Transaction scope.
   * @return Results of the operation.
   */
  @Override
  public StoreResults doGlobalExecute(IStoreTransactionScope ts) {
    return ts.executeOperation(
        StoreOperationRequestBuilder.SP_GET_ALL_DISTINCT_SHARD_LOCATIONS_GLOBAL,
        StoreOperationRequestBuilder.getAllDistinctShardLocationsGlobal());
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
    throw StoreOperationErrorHandler.onShardMapManagerErrorGlobal(result, null,
        this.getOperationName(),
        StoreOperationRequestBuilder.SP_GET_ALL_DISTINCT_SHARD_LOCATIONS_GLOBAL);
  }

  /**
   * Error category for store exception.
   */
  @Override
  protected ShardManagementErrorCategory getErrorCategory() {
    return ShardManagementErrorCategory.ShardMapManager;
  }
}