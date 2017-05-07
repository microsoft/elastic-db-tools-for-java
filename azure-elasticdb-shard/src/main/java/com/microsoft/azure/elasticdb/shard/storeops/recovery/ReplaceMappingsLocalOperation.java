package com.microsoft.azure.elasticdb.shard.storeops.recovery;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
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
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationLocal;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Replaces the LSM mappings for given shard map with the input mappings.
 */
public class ReplaceMappingsLocalOperation extends StoreOperationLocal {

  /**
   * Local shard map.
   */
  private StoreShardMap shardMap;

  /**
   * Local shard.
   */
  private StoreShard shard;

  /**
   * List of ranges to be removed.
   */
  private List<ShardRange> rangesToRemove;

  /**
   * List of mappings to add.
   */
  private List<StoreMapping> mappingsToAdd;

  /**
   * Constructs request for replacing the LSM mappings for given shard map with the input mappings.
   *
   * @param shardMapManager Shard map manager.
   * @param location Location of the LSM.
   * @param operationName Operation name.
   * @param shardMap Local shard map.
   * @param shard Local shard.
   * @param rangesToRemove Optional list of ranges to minimize amount of deletions.
   * @param mappingsToAdd List of mappings to add.
   */
  public ReplaceMappingsLocalOperation(ShardMapManager shardMapManager, ShardLocation location,
      String operationName, StoreShardMap shardMap, StoreShard shard,
      List<ShardRange> rangesToRemove, List<StoreMapping> mappingsToAdd) {
    super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), location,
        operationName);
    this.shardMap = shardMap;
    this.shard = shard;
    this.rangesToRemove = rangesToRemove;
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
   * Execute the operation against LSM in the current transaction scope.
   *
   * @param ts Transaction scope.
   * @return Results of the operation.
   */
  @Override
  public StoreResults doLocalExecute(IStoreTransactionScope ts) {
    List<StoreMapping> mappingsToRemove = this.getMappingsToPurge(ts);

    // Create a new Guid so that this operation forces over-writes.
    return ts.executeOperation(StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARD_MAPPINGS_LOCAL,
        StoreOperationRequestBuilder.replaceShardMappingsLocal(UUID.randomUUID(), false, shardMap,
            mappingsToRemove.toArray(new StoreMapping[0]), mappingsToAdd.toArray(
                new StoreMapping[0])));
  }

  /**
   * Handles errors from the LSM operation.
   *
   * @param result Operation result.
   */
  @Override
  public void handleDoLocalExecuteError(StoreResults result) {
    // Possible errors are:
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler.onRecoveryErrorLocal(result, shardMap, this.getLocation(),
        ShardManagementErrorCategory.Recovery, this.getOperationName(),
        StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARD_MAPPINGS_LOCAL);
  }

  /**
   * Finds all mappings to be purged based on the given input ranges.
   *
   * @param ts LSM transaction scope.
   * @return Mappings which are to be removed.
   */
  private List<StoreMapping> getMappingsToPurge(IStoreTransactionScope ts) {
    List<StoreMapping> lsmMappings;

    StoreResults result;

    if (rangesToRemove == null) {
      // If no ranges are specified, get all the mappings for the shard.
      result = ts.executeOperation(StoreOperationRequestBuilder.SP_GET_ALL_SHARD_MAPPINGS_LOCAL,
          StoreOperationRequestBuilder.getAllShardMappingsLocal(shardMap, shard, null));

      if (result.getResult() != StoreResult.Success) {
        // Possible errors are:
        // StoreResult.ShardMapDoesNotExist
        // StoreResult.StoreVersionMismatch
        // StoreResult.MissingParametersForStoredProcedure
        throw StoreOperationErrorHandler.onRecoveryErrorLocal(result, shardMap, this.getLocation(),
            ShardManagementErrorCategory.Recovery, this.getOperationName(),
            StoreOperationRequestBuilder.SP_GET_ALL_SHARD_MAPPINGS_LOCAL);
      }

      lsmMappings = result.getStoreMappings();
    } else {
      // If any ranges are specified, only delete intersected ranges.
      Map<ShardRange, StoreMapping> mappingsToPurge = new HashMap<>();

      for (ShardRange range : rangesToRemove) {
        switch (shardMap.getMapType()) {
          case Range:
            result = ts
                .executeOperation(StoreOperationRequestBuilder.SP_GET_ALL_SHARD_MAPPINGS_LOCAL,
                    StoreOperationRequestBuilder
                        .getAllShardMappingsLocal(shardMap, shard, range));
            break;

          default:
            assert shardMap.getMapType() == ShardMapType.List;
            result = ts
                .executeOperation(StoreOperationRequestBuilder.SP_FIND_SHARD_MAPPING_BY_KEY_LOCAL,
                    StoreOperationRequestBuilder.findShardMappingByKeyLocal(shardMap,
                        ShardKey
                            .fromRawValue(shardMap.getKeyType(), range.getLow().getRawValue())));
            break;
        }

        if (result.getResult() != StoreResult.Success) {
          if (result.getResult() != StoreResult.MappingNotFoundForKey) {
            // Possible errors are:
            // StoreResult.ShardMapDoesNotExist
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler
                .onRecoveryErrorLocal(result, shardMap, this.getLocation(),
                    ShardManagementErrorCategory.Recovery, this.getOperationName(),
                    shardMap.getMapType() == ShardMapType.Range
                        ? StoreOperationRequestBuilder.SP_GET_ALL_SHARD_MAPPINGS_LOCAL
                        : StoreOperationRequestBuilder.SP_FIND_SHARD_MAPPING_BY_KEY_LOCAL);
          } else {
            // No intersections being found is fine. Skip to the next mapping.
            assert shardMap.getMapType() == ShardMapType.List;
          }
        } else {
          for (StoreMapping mapping : result.getStoreMappings()) {
            ShardRange intersectedRange = new ShardRange(
                ShardKey.fromRawValue(shardMap.getKeyType(), mapping.getMinValue()),
                ShardKey.fromRawValue(shardMap.getKeyType(), mapping.getMaxValue()));

            mappingsToPurge.put(intersectedRange, mapping);
          }
        }
      }
      lsmMappings = (List<StoreMapping>) mappingsToPurge.values();
    }

    return lsmMappings;
  }

  @Override
  public void close() throws IOException {

  }
}
