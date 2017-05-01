package com.microsoft.azure.elasticdb.shard.storeops.recovery;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationErrorHandler;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;
import java.io.IOException;

/**
 * Detaches the given shard and corresponding mapping information to the GSM database.
 */
public class DetachShardGlobalOperation extends StoreOperationGlobal {

  /**
   * Location to be detached.
   */
  private ShardLocation _location;

  /**
   * Shard map from which shard is being detached.
   */
  private String _shardMapName;

  /**
   * Constructs request for Detaching the given shard and mapping information to the GSM database.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationName Operation name.
   * @param location Location to be detached.
   * @param shardMapName Shard map from which shard is being detached.
   */
  public DetachShardGlobalOperation(ShardMapManager shardMapManager, String operationName,
      ShardLocation location, String shardMapName) {
    super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), operationName);
    _shardMapName = shardMapName;
    _location = location;
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
    return ts.ExecuteOperation(StoreOperationRequestBuilder.SP_DETACH_SHARD_GLOBAL,
        StoreOperationRequestBuilder.detachShardGlobal(_shardMapName, _location));
  }

  /**
   * Handles errors from the GSM operation after the LSM operations.
   *
   * @param result Operation result.
   */
  @Override
  public void HandleDoGlobalExecuteError(StoreResults result) {
    // Possible errors are:
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler
        .OnRecoveryErrorGlobal(result, null, null, ShardManagementErrorCategory.Recovery,
            this.getOperationName(), StoreOperationRequestBuilder.SP_DETACH_SHARD_GLOBAL);
  }

  /**
   * Error category for store exception.
   */
  @Override
  protected ShardManagementErrorCategory getErrorCategory() {
    return ShardManagementErrorCategory.Recovery;
  }

  @Override
  public void close() throws IOException {

  }
}