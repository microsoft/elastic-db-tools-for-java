package com.microsoft.azure.elasticdb.shard.storeops.recovery;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreResult;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreShard;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationErrorHandler;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationLocal;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;
import java.io.IOException;

/**
 * Obtains all the shard maps and shards from an LSM.
 */
public class GetMappingsByRangeLocalOperation extends StoreOperationLocal {

  /**
   * Local shard map.
   */
  private StoreShardMap shardMap;

  /**
   * Local shard.
   */
  private StoreShard shard;

  /**
   * Range to get mappings from.
   */
  private ShardRange range;

  /**
   * Ignore ShardMapNotFound error.
   */
  private boolean ignoreFailure;

  /**
   * Constructs request for obtaining all the shard maps and shards from an LSM.
   *
   * @param shardMapManager Shard map manager.
   * @param location Location of the LSM.
   * @param operationName Operation name.
   * @param shardMap Local shard map.
   * @param shard Local shard.
   * @param range Optional range to get mappings from.
   * @param ignoreFailure Ignore shard map not found error.
   */
  public GetMappingsByRangeLocalOperation(ShardMapManager shardMapManager, ShardLocation location,
      String operationName, StoreShardMap shardMap, StoreShard shard, ShardRange range,
      boolean ignoreFailure) {
    super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), location,
        operationName);
    assert shard != null;

    this.shardMap = shardMap;
    this.shard = shard;
    this.range = range;
    this.ignoreFailure = ignoreFailure;
  }

  /**
   * Whether this is a read-only operation.
   */
  @Override
  public boolean getReadOnly() {
    return true;
  }

  /**
   * Execute the operation against LSM in the current transaction scope.
   *
   * @param ts Transaction scope.
   * @return Results of the operation.
   */
  @Override
  public StoreResults doLocalExecute(IStoreTransactionScope ts) {
    return ts.executeOperation(StoreOperationRequestBuilder.SP_GET_ALL_SHARD_MAPPINGS_LOCAL,
        StoreOperationRequestBuilder.getAllShardMappingsLocal(shardMap, shard, range));
  }

  /**
   * Handles errors from the LSM operation.
   *
   * @param result Operation result.
   */
  @Override
  public void handleDoLocalExecuteError(StoreResults result) {
    if (!ignoreFailure || result.getResult() != StoreResult.ShardMapDoesNotExist) {
      // Possible errors are:
      // StoreResult.ShardMapDoesNotExist
      // StoreResult.StoreVersionMismatch
      // StoreResult.MissingParametersForStoredProcedure
      throw StoreOperationErrorHandler.onRecoveryErrorLocal(result, shardMap, this.getLocation(),
          ShardManagementErrorCategory.Recovery, this.getOperationName(),
          StoreOperationRequestBuilder.SP_GET_ALL_SHARD_MAPPINGS_LOCAL);
    }
  }

  @Override
  public void close() throws IOException {

  }
}