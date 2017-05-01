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
import java.io.IOException;

/**
 * Adds given shard map to GSM.
 */
public class AddShardMapGlobalOperation extends StoreOperationGlobal {

  /**
   * Shard map manager object.
   */
  private ShardMapManager shardMapManager;

  /**
   * Shard map to add.
   */
  private StoreShardMap shardMap;

  /**
   * Constructs request to add given shard map to GSM.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationName Operation name, useful for diagnostics.
   * @param shardMap Shard map to add.
   */
  public AddShardMapGlobalOperation(ShardMapManager shardMapManager, String operationName,
      StoreShardMap shardMap) {
    super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), operationName);
    this.shardMapManager = shardMapManager;
    this.shardMap = shardMap;
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
    return ts.executeOperation(StoreOperationRequestBuilder.SP_ADD_SHARD_MAP_GLOBAL,
        StoreOperationRequestBuilder.addShardMapGlobal(shardMap));
  }

  /**
   * Handles errors from the GSM operation after the LSM operations.
   *
   * @param result Operation result.
   */
  @Override
  public void handleDoGlobalExecuteError(StoreResults result) {
    // Possible errors are:
    // StoreResult.ShardMapExists
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler
        .onShardMapManagerErrorGlobal(result, shardMap, this.getOperationName(),
            StoreOperationRequestBuilder.SP_ADD_SHARD_MAP_GLOBAL);
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
    shardMapManager.getCache().addOrUpdateShardMap(shardMap);
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