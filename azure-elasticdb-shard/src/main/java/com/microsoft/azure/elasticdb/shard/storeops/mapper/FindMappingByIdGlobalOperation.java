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
  private ShardMapManager _manager;

  /**
   * Shard map for which mappings are requested.
   */
  private StoreShardMap _shardMap;

  /**
   * Mapping whose Id will be used.
   */
  private StoreMapping _mapping;

  /**
   * Error category to use.
   */
  private ShardManagementErrorCategory _errorCategory;

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
    _manager = shardMapManager;
    _shardMap = shardMap;
    _mapping = mapping;
    _errorCategory = errorCategory;
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
    // If no ranges are specified, blindly mark everything for deletion.
    return ts.ExecuteOperation(StoreOperationRequestBuilder.SpFindShardMappingByIdGlobal,
        StoreOperationRequestBuilder.FindShardMappingByIdGlobal(_shardMap, _mapping));
  }

  /**
   * Invalidates the cache on unsuccessful commit of the GSM operation.
   *
   * @param result Operation result.
   */
  @Override
  public void DoGlobalUpdateCachePre(StoreResults result) {
    if (result.getResult() == StoreResult.ShardMapDoesNotExist) {
      // Remove shard map from cache.
      _manager.getCache().deleteShardMap(_shardMap);
    }

    if (result.getResult() == StoreResult.MappingDoesNotExist) {
      // Remove mapping from cache.
      _manager.getCache().deleteMapping(_mapping);
    }
  }

  /**
   * Handles errors from the GSM operation after the LSM operations.
   *
   * @param result Operation result.
   */
  @Override
  public void HandleDoGlobalExecuteError(StoreResults result) {
    // Possible errors are:
    // StoreResult.ShardMapDoesNotExist
    // StoreResult.MappingDoesNotExist
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler
        .OnShardMapperErrorGlobal(result, _shardMap, _mapping.getStoreShard(), _errorCategory,
            this.getOperationName(),
            StoreOperationRequestBuilder.SpFindShardMappingByIdGlobal); // shard
  }

  /**
   * Refreshes the cache on successful commit of the GSM operation.
   *
   * @param result Operation result.
   */
  @Override
  public void DoGlobalUpdateCachePost(StoreResults result) {
    assert result.getResult() == StoreResult.Success;
    for (StoreMapping sm : result.getStoreMappings()) {
      _manager.getCache().addOrUpdateMapping(sm, CacheStoreMappingUpdatePolicy.OverwriteExisting);
    }
  }

  /**
   * Error category for store exception.
   */
  @Override
  protected ShardManagementErrorCategory getErrorCategory() {
    return _errorCategory;
  }

  @Override
  public void close() throws IOException {

  }
}