package com.microsoft.azure.elasticdb.shard.storeops.map;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
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
 * Gets shard with specific location from given shard map from GSM.
 */
public class FindShardByLocationGlobalOperation extends StoreOperationGlobal {

  /**
   * Shard map manager object.
   */
  private ShardMapManager _shardMapManager;

  /**
   * Shard map for which shard is being requested.
   */
  private StoreShardMap _shardMap;

  /**
   * Location of the shard being searched.
   */
  private ShardLocation _location;

  /**
   * Constructs request to get shard with specific location for given shard map from GSM.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationName Operation name, useful for diagnostics.
   * @param shardMap Shard map for which shard is being requested.
   * @param location Location of shard being searched.
   */
  public FindShardByLocationGlobalOperation(ShardMapManager shardMapManager, String operationName,
      StoreShardMap shardMap, ShardLocation location) {
    super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), operationName);
    _shardMapManager = shardMapManager;
    _shardMap = shardMap;
    _location = location;
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
  public StoreResults DoGlobalExecute(IStoreTransactionScope ts) {
    return ts.ExecuteOperation(StoreOperationRequestBuilder.SpFindShardByLocationGlobal,
        StoreOperationRequestBuilder.FindShardByLocationGlobal(_shardMap, _location));
  }

  /**
   * Handles errors from the GSM operation after the LSM operations.
   *
   * @param result Operation result.
   */
  @Override
  public void HandleDoGlobalExecuteError(StoreResults result) {
    if (result.getResult() == StoreResult.ShardMapDoesNotExist) {
      // Remove shard map from cache.
      _shardMapManager.getCache().deleteShardMap(_shardMap);
    }

    // Possible errors are:
    // StoreResult.ShardMapDoesNotExist
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler
        .OnShardMapErrorGlobal(result, _shardMap, null, ShardManagementErrorCategory.ShardMap,
            this.getOperationName(),
            StoreOperationRequestBuilder.SpFindShardByLocationGlobal); // shard
  }

  /**
   * Error category for store exception.
   */
  @Override
  protected ShardManagementErrorCategory getErrorCategory() {
    return ShardManagementErrorCategory.ShardMap;
  }

  @Override
  public void close() throws IOException {

  }
}