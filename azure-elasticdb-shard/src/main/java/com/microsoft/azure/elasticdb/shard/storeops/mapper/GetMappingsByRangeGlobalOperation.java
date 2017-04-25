package com.microsoft.azure.elasticdb.shard.storeops.mapper;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.cache.CacheStoreMappingUpdatePolicy;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreResult;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreShard;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationErrorHandler;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;
import java.io.IOException;

/**
 * Obtains all the mappings from the GSM based on given shard and range.
 */
public class GetMappingsByRangeGlobalOperation extends StoreOperationGlobal {

  /**
   * Shard map manager instance.
   */
  private ShardMapManager _manager;

  /**
   * Shard map for which mappings are requested.
   */
  private StoreShardMap _shardMap;

  /**
   * Optional shard which has the mappings.
   */
  private StoreShard _shard;

  /**
   * Optional range to get mappings for.
   */
  private ShardRange _range;

  /**
   * Error category to use.
   */
  private ShardManagementErrorCategory _errorCategory;

  /**
   * Whether to cache the results.
   */
  private boolean _cacheResults;

  /**
   * Ignore ShardMapNotFound error.
   */
  private boolean _ignoreFailure;


  /**
   * Constructs request for obtaining all the mappings from GSM based on given shard and mappings.
   *
   * @param shardMapManager Shard map manager.
   * @param operationName Operation being executed.
   * @param shardMap Local shard map.
   * @param shard Local shard.
   * @param range Optional range to get mappings from.
   * @param errorCategory Error category.
   * @param cacheResults Whether to cache the results of the operation.
   * @param ignoreFailure Ignore shard map not found error.
   */
  public GetMappingsByRangeGlobalOperation(ShardMapManager shardMapManager, String operationName,
      StoreShardMap shardMap, StoreShard shard, ShardRange range,
      ShardManagementErrorCategory errorCategory, boolean cacheResults, boolean ignoreFailure) {
    super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), operationName);
    _manager = shardMapManager;
    _shardMap = shardMap;
    _shard = shard;
    _range = range;
    _errorCategory = errorCategory;
    _cacheResults = cacheResults;
    _ignoreFailure = ignoreFailure;
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
    return ts.ExecuteOperation(StoreOperationRequestBuilder.SpGetAllShardMappingsGlobal
        , StoreOperationRequestBuilder.GetAllShardMappingsGlobal(_shardMap, _shard, _range));
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
      _manager.getCache().DeleteShardMap(_shardMap);
    }
  }

  /**
   * Handles errors from the GSM operation after the LSM operations.
   *
   * @param result Operation result.
   */
  @Override
  public void HandleDoGlobalExecuteError(StoreResults result) {
    // Recovery manager handles the ShardMapDoesNotExist error properly, so we don't interfere.
    if (!_ignoreFailure || result.getResult() != StoreResult.ShardMapDoesNotExist) {
      // Possible errors are:
      // StoreResult.ShardMapDoesNotExist
      // StoreResult.ShardDoesNotExist
      // StoreResult.ShardVersionMismatch
      // StoreResult.StoreVersionMismatch
      // StoreResult.MissingParametersForStoredProcedure
      throw StoreOperationErrorHandler
          .OnShardMapperErrorGlobal(result, _shardMap, _shard, _errorCategory,
              this.getOperationName(), StoreOperationRequestBuilder.SpGetAllShardMappingsGlobal);
    }
  }

  /**
   * Refreshes the cache on successful commit of the GSM operation.
   *
   * @param result Operation result.
   */
  @Override
  public void DoGlobalUpdateCachePost(StoreResults result) {
    if (result.getResult() == StoreResult.Success && _cacheResults) {
      for (StoreMapping sm : result.getStoreMappings()) {
        _manager.getCache().AddOrUpdateMapping(sm, CacheStoreMappingUpdatePolicy.OverwriteExisting);
      }
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