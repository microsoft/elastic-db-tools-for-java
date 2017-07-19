package com.microsoft.azure.elasticdb.shard.storeops.recovery;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.map.ShardMapType;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Replaces the GSM mappings for given shard map with the input mappings.
 */
public class ReplaceMappingsGlobalOperation extends StoreOperationGlobal {

  /**
   * Global shard map.
   */
  private StoreShardMap shardMap;

  /**
   * Global shard.
   */
  private StoreShard shard;

  /**
   * List of mappings to remove.
   */
  private List<StoreMapping> mappingsToRemove;

  /**
   * List of mappings to add.
   */
  private List<StoreMapping> mappingsToAdd;

  /**
   * Constructs request for replacing the GSM mappings for given shard map with the input mappings.
   *
   * @param shardMapManager Shard map manager.
   * @param operationName Operation name.
   * @param shardMap GSM Shard map.
   * @param shard GSM Shard.
   * @param mappingsToRemove Optional list of mappings to remove.
   * @param mappingsToAdd List of mappings to add.
   */
  public ReplaceMappingsGlobalOperation(ShardMapManager shardMapManager, String operationName,
      StoreShardMap shardMap, StoreShard shard, List<StoreMapping> mappingsToRemove,
      List<StoreMapping> mappingsToAdd) {
    super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), operationName);
    this.shardMap = shardMap;
    this.shard = shard;
    this.mappingsToRemove = mappingsToRemove;
    this.mappingsToAdd = mappingsToAdd;
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
    List<StoreMapping> mappingsToReplace = this.getMappingsToPurge(ts);

    return ts.executeOperation(StoreOperationRequestBuilder.SP_REPLACE_SHARD_MAPPINGS_GLOBAL,
        StoreOperationRequestBuilder.replaceShardMappingsGlobalWithoutLogging(shardMap,
            mappingsToReplace.toArray(new StoreMapping[0]),
            mappingsToAdd.toArray(new StoreMapping[0])));
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
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler
        .onRecoveryErrorGlobal(result, shardMap, shard, ShardManagementErrorCategory.Recovery,
            this.getOperationName(), StoreOperationRequestBuilder.SP_REPLACE_SHARD_MAPPINGS_GLOBAL);
  }

  /**
   * Error category for store exception.
   */
  @Override
  protected ShardManagementErrorCategory getErrorCategory() {
    return ShardManagementErrorCategory.Recovery;
  }

  /**
   * Finds all mappings to be purged based on the given input ranges.
   *
   * @param ts GSM transaction scope.
   * @return Mappings which are to be removed.
   */
  private List<StoreMapping> getMappingsToPurge(IStoreTransactionScope ts) {
    // Find all the mappings in GSM belonging to the shard
    StoreResults gsmMappingsByShard = ts
        .executeOperation(StoreOperationRequestBuilder.SP_GET_ALL_SHARD_MAPPINGS_GLOBAL,
            StoreOperationRequestBuilder.getAllShardMappingsGlobal(shardMap, shard, null));

    if (gsmMappingsByShard.getResult() != StoreResult.Success) {
      // Possible errors are:
      // StoreResult.ShardMapDoesNotExist
      // StoreResult.StoreVersionMismatch
      // StoreResult.MissingParametersForStoredProcedure
      throw StoreOperationErrorHandler.onRecoveryErrorGlobal(gsmMappingsByShard, shardMap, shard,
          ShardManagementErrorCategory.Recovery, this.getOperationName(),
          StoreOperationRequestBuilder.SP_GET_ALL_SHARD_MAPPINGS_GLOBAL);
    }

    Map<ShardRange, StoreMapping> intersectingMappings = new HashMap<>();

    for (StoreMapping gsmMappingByShard : gsmMappingsByShard.getStoreMappings()) {
      ShardKey min = ShardKey.fromRawValue(shardMap.getKeyType(), gsmMappingByShard.getMinValue());

      ShardKey max;

      switch (shardMap.getMapType()) {
        case Range:
          max = ShardKey.fromRawValue(shardMap.getKeyType(), gsmMappingByShard.getMaxValue());
          break;
        default:
          assert shardMap.getMapType() == ShardMapType.List;
          max = ShardKey.fromRawValue(shardMap.getKeyType(), gsmMappingByShard.getMinValue())
              .getNextKey();
          break;
      }

      intersectingMappings.put(new ShardRange(min, max), gsmMappingByShard);
    }

    // We need to discover, also, the range of intersecting mappings, so we can transitively detect
    // inconsistencies with other shards.
    for (StoreMapping lsmMapping : mappingsToRemove) {
      ShardKey min = ShardKey.fromRawValue(shardMap.getKeyType(), lsmMapping.getMinValue());

      StoreResults gsmMappingsByRange;

      switch (shardMap.getMapType()) {
        case Range:
          gsmMappingsByRange = ts
              .executeOperation(StoreOperationRequestBuilder.SP_GET_ALL_SHARD_MAPPINGS_GLOBAL,
                  StoreOperationRequestBuilder.getAllShardMappingsGlobal(shardMap, null,
                      new ShardRange(min, ShardKey
                          .fromRawValue(shardMap.getKeyType(), lsmMapping.getMaxValue()))));
          break;

        default:
          assert shardMap.getMapType() == ShardMapType.List;
          gsmMappingsByRange = ts
              .executeOperation(StoreOperationRequestBuilder.SP_FIND_SHARD_MAPPING_BY_KEY_GLOBAL,
                  StoreOperationRequestBuilder.findShardMappingByKeyGlobal(shardMap, min));
          break;
      }

      if (gsmMappingsByRange.getResult() != StoreResult.Success) {
        if (gsmMappingsByRange.getResult() != StoreResult.MappingNotFoundForKey) {
          // Possible errors are:
          // StoreResult.ShardMapDoesNotExist
          // StoreResult.StoreVersionMismatch
          // StoreResult.MissingParametersForStoredProcedure
          throw StoreOperationErrorHandler
              .onRecoveryErrorGlobal(gsmMappingsByRange, shardMap, shard,
                  ShardManagementErrorCategory.Recovery, this.getOperationName(),
                  shardMap.getMapType() == ShardMapType.Range
                      ? StoreOperationRequestBuilder.SP_GET_ALL_SHARD_MAPPINGS_GLOBAL
                      : StoreOperationRequestBuilder.SP_FIND_SHARD_MAPPING_BY_KEY_GLOBAL);
        } else {
          // No intersections being found is fine. Skip to the next mapping.
          assert shardMap.getMapType() == ShardMapType.List;
        }
      } else {
        for (StoreMapping gsmMappingByRange : gsmMappingsByRange.getStoreMappings()) {
          ShardKey minGlobal = ShardKey
              .fromRawValue(shardMap.getKeyType(), gsmMappingByRange.getMinValue());
          ShardKey maxGlobal;

          switch (shardMap.getMapType()) {
            case Range:
              maxGlobal = ShardKey
                  .fromRawValue(shardMap.getKeyType(), gsmMappingByRange.getMaxValue());
              break;
            default:
              assert shardMap.getMapType() == ShardMapType.List;
              maxGlobal = ShardKey
                  .fromRawValue(shardMap.getKeyType(), gsmMappingByRange.getMinValue())
                  .getNextKey();
              break;
          }

          intersectingMappings.put(new ShardRange(minGlobal, maxGlobal), gsmMappingByRange);
        }
      }
    }
    return new ArrayList<>(intersectingMappings.values());
  }
}
