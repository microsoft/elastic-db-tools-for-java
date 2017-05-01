package com.microsoft.azure.elasticdb.shard.storeops.mapper;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.cache.CacheStoreMappingUpdatePolicy;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreResult;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationErrorHandler;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;
import java.io.IOException;

/**
 * Obtains the mapping by Id from the GSM.
 */
public class FindMappingByIdGlobalOperation extends StoreOperationGlobal {

  /**
   * Shard map manager instance.
   */
  private ShardMapManager shardMapManager;

  /**
   * Shard map for which mappings are requested.
   */
  private StoreShardMap shardMap;

  /**
   * Mapping whose Id will be used.
   */
  private StoreMapping mapping;

  /**
   * Error category to use.
   */
  private ShardManagementErrorCategory errorCategory;

  /**
   * Constructs request for obtaining mapping from GSM based on given key.
   *
   * @param shardMapManager Shard map manager.
   * @param operationName Operation being executed.
   * @param shardMap Local shard map.
   * @param mapping Mapping whose Id will be used.
   * @param errorCategory Error category.
   */
  public FindMappingByIdGlobalOperation(ShardMapManager shardMapManager, String operationName,
      StoreShardMap shardMap, StoreMapping mapping, ShardManagementErrorCategory errorCategory) {
    super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), operationName);
    this.shardMapManager = shardMapManager;
    this.shardMap = shardMap;
    this.mapping = mapping;
    this.errorCategory = errorCategory;
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
    // If no ranges are specified, blindly mark everything for deletion.
    return ts.executeOperation(StoreOperationRequestBuilder.SP_FIND_SHARD_MAPPING_BY_ID_GLOBAL,
        StoreOperationRequestBuilder.findShardMappingByIdGlobal(shardMap, mapping));
  }

  /**
   * Invalidates the cache on unsuccessful commit of the GSM operation.
   *
   * @param result Operation result.
   */
  @Override
  public void doGlobalUpdateCachePre(StoreResults result) {
    if (result.getResult() == StoreResult.ShardMapDoesNotExist) {
      // Remove shard map from cache.
      shardMapManager.getCache().deleteShardMap(shardMap);
    }

    if (result.getResult() == StoreResult.MappingDoesNotExist) {
      // Remove mapping from cache.
      shardMapManager.getCache().deleteMapping(mapping);
    }
  }

  /**
   * Handles errors from the GSM operation after the LSM operations.
   *
   * @param result Operation result.
   */
  @Override
  public void handleDoGlobalExecuteError(StoreResults result) {
    // Possible errors are:
    // StoreResult.ShardMapDoesNotExist
    // StoreResult.MappingDoesNotExist
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler
        .onShardMapperErrorGlobal(result, shardMap, mapping.getStoreShard(), errorCategory,
            this.getOperationName(),
            StoreOperationRequestBuilder.SP_FIND_SHARD_MAPPING_BY_ID_GLOBAL); // shard
  }

  /**
   * Refreshes the cache on successful commit of the GSM operation.
   *
   * @param result Operation result.
   */
  @Override
  public void doGlobalUpdateCachePost(StoreResults result) {
    assert result.getResult() == StoreResult.Success;
    for (StoreMapping sm : result.getStoreMappings()) {
      shardMapManager.getCache().addOrUpdateMapping(sm,
          CacheStoreMappingUpdatePolicy.OverwriteExisting);
    }
  }

  /**
   * Error category for store exception.
   */
  @Override
  protected ShardManagementErrorCategory getErrorCategory() {
    return errorCategory;
  }

  @Override
  public void close() throws IOException {

  }
}