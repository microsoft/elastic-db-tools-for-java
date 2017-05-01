package com.microsoft.azure.elasticdb.shard.storeops.mapmanger;

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
import java.util.ArrayList;
import java.util.List;

/**
 * Gets all shard maps from GSM.
 */
public class LoadShardMapManagerGlobalOperation extends StoreOperationGlobal {

  /**
   * Shard map manager object.
   */
  private ShardMapManager shardMapManager;

  private ArrayList<LoadResult> loadResults;

  private StoreShardMap ssmCurrent;

  /**
   * Constructs request to get all shard maps from GSM.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationName Operation name, useful for diagnostics.
   */
  public LoadShardMapManagerGlobalOperation(ShardMapManager shardMapManager, String operationName) {
    super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), operationName);
    this.shardMapManager = shardMapManager;
    loadResults = new ArrayList<LoadResult>();
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
    loadResults.clear();

    StoreResults result = ts
        .executeOperation(StoreOperationRequestBuilder.SP_GET_ALL_SHARD_MAPS_GLOBAL,
            StoreOperationRequestBuilder.getAllShardMapsGlobal());

    if (result.getResult() == StoreResult.Success) {
      for (StoreShardMap ssm : result.getStoreShardMaps()) {
        ssmCurrent = ssm;

        result = ts.executeOperation(StoreOperationRequestBuilder.SP_GET_ALL_SHARD_MAPPINGS_GLOBAL,
            StoreOperationRequestBuilder.getAllShardMappingsGlobal(ssm, null, null));

        if (result.getResult() == StoreResult.Success) {
          LoadResult tempVar = new LoadResult();
          tempVar.setShardMap(ssm);
          tempVar.setMappings(result.getStoreMappings());
          loadResults.add(tempVar);
        } else {
          if (result.getResult() != StoreResult.ShardMapDoesNotExist) {
            // Ignore some possible failures for Load operation and skip failed
            // shard map caching operations.
            break;
          }
        }
      }
    }

    return result;
  }

  /**
   * Handles errors from the GSM operation after the LSM operations.
   *
   * @param result Operation result.
   */
  @Override
  public void handleDoGlobalExecuteError(StoreResults result) {
    if (ssmCurrent == null) {
      // Possible errors are:
      // StoreResult.StoreVersionMismatch
      // StoreResult.MissingParametersForStoredProcedure
      throw StoreOperationErrorHandler
          .onShardMapManagerErrorGlobal(result, null, this.getOperationName(),
              StoreOperationRequestBuilder.SP_GET_ALL_SHARD_MAPS_GLOBAL);
    } else {
      if (result.getResult() != StoreResult.ShardMapDoesNotExist) {
        // Possible errors are:
        // StoreResult.StoreVersionMismatch
        // StoreResult.MissingParametersForStoredProcedure
        throw StoreOperationErrorHandler.onShardMapperErrorGlobal(result, ssmCurrent, null,
            ShardManagementErrorCategory.ShardMapManager, this.getOperationName(),
            StoreOperationRequestBuilder.SP_GET_ALL_SHARD_MAPPINGS_GLOBAL); // shard
      }
    }
  }

  /**
   * Refreshes the cache on successful commit of the GSM operation.
   *
   * @param result Operation result.
   */
  @Override
  public void doGlobalUpdateCachePost(StoreResults result) {
    assert result.getResult() == StoreResult.Success
        || result.getResult() == StoreResult.ShardMapDoesNotExist;

    // Add shard maps and mappings to cache.
    for (LoadResult loadResult : loadResults) {
      shardMapManager.getCache().addOrUpdateShardMap(loadResult.getShardMap());

      for (StoreMapping sm : loadResult.getMappings()) {
        shardMapManager.getCache()
            .addOrUpdateMapping(sm, CacheStoreMappingUpdatePolicy.OverwriteExisting);
      }
    }
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

  /**
   * Result of load operation.
   */
  private static class LoadResult {

    /**
     * Shard map from the store.
     */
    private StoreShardMap shardMap;
    /**
     * Mappings corresponding to the shard map.
     */
    private List<StoreMapping> mappings;

    public final StoreShardMap getShardMap() {
      return shardMap;
    }

    public final void setShardMap(StoreShardMap value) {
      shardMap = value;
    }

    public final List<StoreMapping> getMappings() {
      return mappings;
    }

    public final void setMappings(List<StoreMapping> value) {
      mappings = value;
    }
  }
}
