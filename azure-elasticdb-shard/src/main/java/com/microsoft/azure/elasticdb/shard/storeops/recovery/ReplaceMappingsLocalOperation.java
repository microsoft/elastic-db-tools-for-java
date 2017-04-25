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
  private StoreShardMap _shardMap;

  /**
   * Local shard.
   */
  private StoreShard _shard;

  /**
   * List of ranges to be removed.
   */
  private List<ShardRange> _rangesToRemove;

  /**
   * List of mappings to add.
   */
  private List<StoreMapping> _mappingsToAdd;

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
    _shardMap = shardMap;
    _shard = shard;
    _rangesToRemove = rangesToRemove;
    _mappingsToAdd = mappingsToAdd;
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
  public StoreResults DoLocalExecute(IStoreTransactionScope ts) {
    List<StoreMapping> mappingsToRemove = this.GetMappingsToPurge(ts);

    return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal,
        StoreOperationRequestBuilder.ReplaceShardMappingsLocal(UUID.randomUUID(), false, _shardMap,
            mappingsToRemove.toArray(new StoreMapping[0]), _mappingsToAdd.toArray(
                new StoreMapping[0]))); // Create a new Guid so that this operation forces over-writes.
  }

  /**
   * Handles errors from the LSM operation.
   *
   * @param result Operation result.
   */
  @Override
  public void HandleDoLocalExecuteError(StoreResults result) {
    // Possible errors are:
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler.OnRecoveryErrorLocal(result, _shardMap, this.getLocation(),
        ShardManagementErrorCategory.Recovery, this.getOperationName(),
        StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal);
  }

  /**
   * Finds all mappings to be purged based on the given input ranges.
   *
   * @param ts LSM transaction scope.
   * @return Mappings which are to be removed.
   */
  private List<StoreMapping> GetMappingsToPurge(IStoreTransactionScope ts) {
    List<StoreMapping> lsmMappings = null;

    StoreResults result;

    if (_rangesToRemove == null) {
      // If no ranges are specified, get all the mappings for the shard.
      result = ts.ExecuteOperation(StoreOperationRequestBuilder.SpGetAllShardMappingsLocal,
          StoreOperationRequestBuilder.GetAllShardMappingsLocal(_shardMap, _shard, null));

      if (result.getResult() != StoreResult.Success) {
        // Possible errors are:
        // StoreResult.ShardMapDoesNotExist
        // StoreResult.StoreVersionMismatch
        // StoreResult.MissingParametersForStoredProcedure
        throw StoreOperationErrorHandler.OnRecoveryErrorLocal(result, _shardMap, this.getLocation(),
            ShardManagementErrorCategory.Recovery, this.getOperationName(),
            StoreOperationRequestBuilder.SpGetAllShardMappingsLocal);
      }

      lsmMappings = result.getStoreMappings();
    } else {
      // If any ranges are specified, only delete intersected ranges.
      Map<ShardRange, StoreMapping> mappingsToPurge = new HashMap<ShardRange, StoreMapping>();

      for (ShardRange range : _rangesToRemove) {
        switch (_shardMap.getMapType()) {
          case Range:
            result = ts.ExecuteOperation(StoreOperationRequestBuilder.SpGetAllShardMappingsLocal,
                StoreOperationRequestBuilder.GetAllShardMappingsLocal(_shardMap, _shard, range));
            break;

          default:
            assert _shardMap.getMapType() == ShardMapType.List;
            result = ts.ExecuteOperation(StoreOperationRequestBuilder.SpFindShardMappingByKeyLocal,
                StoreOperationRequestBuilder.FindShardMappingByKeyLocal(_shardMap,
                    ShardKey.fromRawValue(_shardMap.getKeyType(), range.getLow().getRawValue())));
            break;
        }

        if (result.getResult() != StoreResult.Success) {
          if (result.getResult() != StoreResult.MappingNotFoundForKey) {
            // Possible errors are:
            // StoreResult.ShardMapDoesNotExist
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler
                .OnRecoveryErrorLocal(result, _shardMap, this.getLocation(),
                    ShardManagementErrorCategory.Recovery, this.getOperationName(),
                    _shardMap.getMapType() == ShardMapType.Range
                        ? StoreOperationRequestBuilder.SpGetAllShardMappingsLocal
                        : StoreOperationRequestBuilder.SpFindShardMappingByKeyLocal);
          } else {
            // No intersections being found is fine. Skip to the next mapping.
            assert _shardMap.getMapType() == ShardMapType.List;
          }
        } else {
          for (StoreMapping mapping : result.getStoreMappings()) {
            ShardRange intersectedRange = new ShardRange(
                ShardKey.fromRawValue(_shardMap.getKeyType(), mapping.getMinValue()),
                ShardKey.fromRawValue(_shardMap.getKeyType(), mapping.getMaxValue()));

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
