package com.microsoft.azure.elasticdb.shard.storeops.mapper;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.cache.CacheStoreMappingUpdatePolicy;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreResult;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreShard;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreConnectionInfo;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperation;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationCode;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationErrorHandler;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationState;
import java.util.UUID;

/**
 * Adds a mapping to given shard map.
 */
public class AddMappingOperation extends StoreOperation {

  /**
   * Shard map for which to add the mapping.
   */
  private StoreShardMap shardmap;

  /**
   * Mapping to add.
   */
  private StoreMapping mapping;

  /**
   * Error category to use.
   */
  private ShardManagementErrorCategory errorCategory;

  /**
   * Creates request to add shard to given shard map.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationCode Store operation code.
   * @param shardMap Shard map for which to add mapping.
   * @param mapping Mapping to add.
   */
  public AddMappingOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode,
      StoreShardMap shardMap, StoreMapping mapping) {
    this(shardMapManager, UUID.randomUUID(), StoreOperationState.UndoBegin, operationCode, shardMap,
        mapping, null);
  }

  /**
   * Creates request to add shard to given shard map.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationId Operation id.
   * @param undoStartState State from which Undo operation starts.
   * @param operationCode Store operation code.
   * @param shardMap Shard map for which to add mapping.
   * @param mapping Mapping to add.
   * @param originalShardVersionAdds Original shard version.
   */
  public AddMappingOperation(ShardMapManager shardMapManager, UUID operationId,
      StoreOperationState undoStartState, StoreOperationCode operationCode, StoreShardMap shardMap,
      StoreMapping mapping, UUID originalShardVersionAdds) {
    super(shardMapManager, operationId, undoStartState, operationCode, null,
        originalShardVersionAdds);
    shardmap = shardMap;
    this.mapping = mapping;
    errorCategory = operationCode == StoreOperationCode.AddRangeMapping
        ? ShardManagementErrorCategory.RangeShardMap : ShardManagementErrorCategory.ListShardMap;
  }

  /**
   * Requests the derived class to provide information regarding the connections
   * needed for the operation.
   *
   * @return Information about shards involved in the operation.
   */
  @Override
  public StoreConnectionInfo getStoreConnectionInfo() {
    StoreConnectionInfo tempVar = new StoreConnectionInfo();
    tempVar.setSourceLocation(
        this.getUndoStartState().getValue() <= StoreOperationState.UndoLocalSourceBeginTransaction
            .getValue() ? mapping.getStoreShard().getLocation() : null);
    return tempVar;
  }

  /**
   * Performs the initial GSM operation prior to LSM operations.
   *
   * @param ts Transaction scope.
   * @return Pending operations on the target objects if any.
   */
  @Override
  public StoreResults doGlobalPreLocalExecute(IStoreTransactionScope ts) {
    return ts.executeOperation(
        StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARD_MAPPINGS_GLOBAL_BEGIN,
        StoreOperationRequestBuilder
            .addShardMappingGlobal(this.getId(), this.getOperationCode(), false, shardmap,
                mapping)); // undo
  }

  /**
   * Handles errors from the initial GSM operation prior to LSM operations.
   *
   * @param result Operation result.
   */
  @Override
  public void handleDoGlobalPreLocalExecuteError(StoreResults result) {
    if (result.getResult() == StoreResult.ShardMapDoesNotExist) {
      // Remove shard map from cache.
      this.getShardMapManager().getCache().deleteShardMap(shardmap);
    }

    // Possible errors are:
    // StoreResult.ShardMapDoesNotExist
    // StoreResult.ShardDoesNotExist
    // StoreResult.MappingPointAlreadyMapped
    // StoreResult.MappingRangeAlreadyMapped
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler
        .onShardMapperErrorGlobal(result, shardmap, mapping.getStoreShard(), errorCategory,
            StoreOperationErrorHandler.operationNameFromStoreOperationCode(this.getOperationCode()),
            StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARD_MAPPINGS_GLOBAL_BEGIN);
  }

  /**
   * Performs the LSM operation on the source shard.
   *
   * @param ts Transaction scope.
   * @return Result of the operation.
   */
  @Override
  public StoreResults doLocalSourceExecute(IStoreTransactionScope ts) {
    return ts.executeOperation(StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARD_MAPPINGS_LOCAL,
        StoreOperationRequestBuilder
            .addShardMappingLocal(this.getId(), false, shardmap, mapping));
  }

  /**
   * Handles errors from the the LSM operation on the source shard.
   *
   * @param result Operation result.
   */
  @Override
  public void handleDoLocalSourceExecuteError(StoreResults result) {
    // Possible errors are:
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler
        .onShardMapperErrorLocal(result, mapping.getStoreShard().getLocation(),
            StoreOperationErrorHandler.operationNameFromStoreOperationCode(this.getOperationCode()),
            StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARD_MAPPINGS_LOCAL);
  }

  /**
   * Performs the final GSM operation after the LSM operations.
   *
   * @param ts Transaction scope.
   * @return Pending operations on the target objects if any.
   */
  @Override
  public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
    return ts
        .executeOperation(StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARD_MAPPINGS_GLOBAL_END,
            StoreOperationRequestBuilder
                .addShardMappingGlobal(this.getId(), this.getOperationCode(), false, shardmap,
                    mapping)); // undo
  }

  /**
   * Handles errors from the final GSM operation after the LSM operations.
   *
   * @param result Operation result.
   */
  @Override
  public void handleDoGlobalPostLocalExecuteError(StoreResults result) {
    if (result.getResult() == StoreResult.ShardMapDoesNotExist) {
      // Remove shard map from cache.
      this.getShardMapManager().getCache().deleteShardMap(shardmap);
    }

    // Possible errors are:
    // StoreResult.ShardMapDoesNotExist
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler
        .onShardMapperErrorGlobal(result, shardmap, mapping.getStoreShard(), errorCategory,
            StoreOperationErrorHandler.operationNameFromStoreOperationCode(this.getOperationCode()),
            StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARD_MAPPINGS_GLOBAL_END);
  }

  /**
   * Refreshes the cache on successful commit of the final GSM operation after the LSM operations.
   *
   * @param result Operation result.
   */
  @Override
  public void doGlobalPostLocalUpdateCache(StoreResults result) {
    // Add mapping to cache.
    this.getShardMapManager().getCache()
        .addOrUpdateMapping(mapping, CacheStoreMappingUpdatePolicy.OverwriteExisting);
  }

  /**
   * Performs the undo of LSM operation on the source shard.
   *
   * @param ts Transaction scope.
   * @return Result of the operation.
   */
  @Override
  public StoreResults undoLocalSourceExecute(IStoreTransactionScope ts) {
    StoreMapping dsm = new StoreMapping(mapping.getId(), shardmap.getId(), mapping.getMinValue(),
        mapping.getMaxValue(), mapping.getStatus(), null,
        new StoreShard(mapping.getStoreShard().getId(), this.getOriginalShardVersionAdds(),
            shardmap.getId(), mapping.getStoreShard().getLocation(),
            mapping.getStoreShard().getStatus()));

    return ts.executeOperation(StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARD_MAPPINGS_LOCAL,
        StoreOperationRequestBuilder.removeShardMappingLocal(this.getId(), true, shardmap, dsm));
  }

  /**
   * Handles errors from the undo of LSM operation on the source shard.
   *
   * @param result Operation result.
   */
  @Override
  public void handleUndoLocalSourceExecuteError(StoreResults result) {
    // Possible errors are:
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler
        .onShardMapperErrorLocal(result, mapping.getStoreShard().getLocation(),
            StoreOperationErrorHandler.operationNameFromStoreOperationCode(this.getOperationCode()),
            StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARD_MAPPINGS_LOCAL);
  }

  /**
   * Performs the undo of GSM operation after LSM operations.
   *
   * @param ts Transaction scope.
   * @return Pending operations on the target objects if any.
   */
  @Override
  public StoreResults undoGlobalPostLocalExecute(IStoreTransactionScope ts) {
    return ts
        .executeOperation(StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARD_MAPPINGS_GLOBAL_END,
            StoreOperationRequestBuilder
                .addShardMappingGlobal(this.getId(), this.getOperationCode(), true, shardmap,
                    mapping)); // undo
  }

  /**
   * Handles errors from the undo of GSM operation after LSM operations.
   *
   * @param result Operation result.
   */
  @Override
  public void handleUndoGlobalPostLocalExecuteError(StoreResults result) {
    if (result.getResult() == StoreResult.ShardMapDoesNotExist) {
      // Remove shard map from cache.
      this.getShardMapManager().getCache().deleteShardMap(shardmap);
    }

    // Possible errors are:
    // StoreResult.ShardMapDoesNotExist
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler
        .onShardMapperErrorGlobal(result, shardmap, mapping.getStoreShard(), errorCategory,
            StoreOperationErrorHandler.operationNameFromStoreOperationCode(this.getOperationCode()),
            StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARD_MAPPINGS_GLOBAL_END);
  }

  /**
   * Source location of error.
   */
  @Override
  protected ShardLocation getErrorSourceLocation() {
    return mapping.getStoreShard().getLocation();
  }

  /**
   * Target location of error.
   */
  @Override
  protected ShardLocation getErrorTargetLocation() {
    return mapping.getStoreShard().getLocation();
  }

  /**
   * Error category for error.
   */
  @Override
  protected ShardManagementErrorCategory getErrorCategory() {
    return errorCategory;
  }

  @Override
  public void close() throws Exception {

  }
}