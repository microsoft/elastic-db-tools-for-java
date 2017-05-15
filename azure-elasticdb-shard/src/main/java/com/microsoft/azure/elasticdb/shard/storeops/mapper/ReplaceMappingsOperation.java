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
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Replaces existing mappings with new mappings in given shard map.
 */
public class ReplaceMappingsOperation extends StoreOperation {

  /**
   * Shard map for which to perform operation.
   */
  private StoreShardMap shardMap;

  /**
   * Original mappings.
   */
  private List<Pair<StoreMapping, UUID>> mappingsSource;

  /**
   * New mappings.
   */
  private List<Pair<StoreMapping, UUID>> mappingsTarget;

  /**
   * Creates request to replace mappings within shard map.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationCode Store operation code.
   * @param shardMap Shard map for which to update mapping.
   * @param mappingsSource Original mappings.
   * @param mappingsTarget Target mappings mapping.
   */
  public ReplaceMappingsOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode,
      StoreShardMap shardMap, List<Pair<StoreMapping, UUID>> mappingsSource,
      List<Pair<StoreMapping, UUID>> mappingsTarget) {
    this(shardMapManager, UUID.randomUUID(), StoreOperationState.UndoBegin, operationCode, shardMap,
        mappingsSource, mappingsTarget, null);
  }

  /**
   * Creates request to replace mappings within shard map.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationId Operation id.
   * @param undoStartState State from which Undo operation starts.
   * @param operationCode Store operation code.
   * @param shardMap Shard map for which to update mapping.
   * @param mappingsSource Original mappings.
   * @param mappingsTarget Target mappings mapping.
   * @param originalShardVersionAdds Original shard version on source.
   */
  public ReplaceMappingsOperation(ShardMapManager shardMapManager, UUID operationId,
      StoreOperationState undoStartState, StoreOperationCode operationCode, StoreShardMap shardMap,
      List<Pair<StoreMapping, UUID>> mappingsSource, List<Pair<StoreMapping, UUID>> mappingsTarget,
      UUID originalShardVersionAdds) {
    super(shardMapManager, operationId, undoStartState, operationCode, originalShardVersionAdds,
        originalShardVersionAdds);
    this.shardMap = shardMap;
    this.mappingsSource = mappingsSource;
    this.mappingsTarget = mappingsTarget;
  }

  /**
   * Requests the derived class to provide information regarding the connections
   * needed for the operation.
   *
   * @return Information about shards involved in the operation.
   */
  @Override
  public StoreConnectionInfo getStoreConnectionInfo() {
    assert mappingsSource.size() > 0;
    StoreConnectionInfo tempVar = new StoreConnectionInfo();
    tempVar.setSourceLocation(
        this.getUndoStartState().getValue() <= StoreOperationState.UndoLocalSourceBeginTransaction
            .getValue() ? mappingsSource.get(0).getLeft().getStoreShard().getLocation() : null);
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
    // undo
    return ts.executeOperation(
        StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARD_MAPPINGS_GLOBAL_BEGIN,
        StoreOperationRequestBuilder.replaceShardMappingsGlobal(this.getId(),
            this.getOperationCode(), false, shardMap, mappingsSource, mappingsTarget));
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
      this.getShardMapManager().getCache().deleteShardMap(shardMap);
    }

    if (result.getResult() == StoreResult.MappingDoesNotExist) {
      for (StoreMapping mappingSource
          : mappingsSource.stream().map(Pair::getLeft).collect(Collectors.toList())) {
        // Remove mapping from cache.
        this.getShardMapManager().getCache().deleteMapping(mappingSource);
      }
    }

    // Possible errors are:
    // StoreResult.ShardMapDoesNotExist
    // StoreResult.ShardDoesNotExist
    // StoreResult.MappingRangeAlreadyMapped
    // StoreResult.MappingDoesNotExist
    // StoreResult.MappingLockOwnerIdDoesNotMatch
    // StoreResult.MappingIsNotOffline
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler
        .onShardMapperErrorGlobal(result, shardMap, mappingsSource.get(0).getLeft().getStoreShard(),
            ShardManagementErrorCategory.RangeShardMap,
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
        StoreOperationRequestBuilder.replaceShardMappingsLocal(this.getId(), false,
            shardMap, mappingsSource.stream().map(Pair::getLeft).toArray(StoreMapping[]::new),
            mappingsTarget.stream().map(Pair::getLeft).toArray(StoreMapping[]::new)));
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
        .onShardMapperErrorLocal(result,
            mappingsSource.get(0).getLeft().getStoreShard().getLocation(),
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
                .replaceShardMappingsGlobal(this.getId(), this.getOperationCode(), false, shardMap,
                    mappingsSource, mappingsTarget)); // undo
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
      this.getShardMapManager().getCache().deleteShardMap(shardMap);
    }

    // Possible errors are:
    // StoreResult.ShardMapDoesNotExist
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler
        .onShardMapperErrorGlobal(result, shardMap, mappingsSource.get(0).getLeft().getStoreShard(),
            ShardManagementErrorCategory.RangeShardMap,
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
    // Remove from cache.
    for (Pair<StoreMapping, UUID> ssm : mappingsSource) {
      this.getShardMapManager().getCache().deleteMapping(ssm.getLeft());
    }

    // Add to cache.
    for (Pair<StoreMapping, UUID> ssm : mappingsTarget) {
      this.getShardMapManager().getCache()
          .addOrUpdateMapping(ssm.getLeft(), CacheStoreMappingUpdatePolicy.OverwriteExisting);
    }
  }

  /**
   * Performs the undo of LSM operation on the source shard.
   *
   * @param ts Transaction scope.
   * @return Result of the operation.
   */
  @Override
  public StoreResults undoLocalSourceExecute(IStoreTransactionScope ts) {
    StoreMapping sourceLeft = mappingsSource.get(0).getLeft();
    StoreMapping targetLeft = mappingsTarget.get(0).getLeft();

    StoreShard dssOriginal = new StoreShard(sourceLeft.getStoreShard().getId(),
        this.getOriginalShardVersionAdds(), sourceLeft.getShardMapId(),
        sourceLeft.getStoreShard().getLocation(), sourceLeft.getStoreShard().getStatus());

    StoreMapping dsmSource = new StoreMapping(sourceLeft.getId(), sourceLeft.getShardMapId(),
        sourceLeft.getMinValue(), sourceLeft.getMaxValue(), sourceLeft.getStatus(),
        mappingsSource.get(0).getRight(), dssOriginal);

    StoreMapping dsmTarget = new StoreMapping(targetLeft.getId(), targetLeft.getShardMapId(),
        targetLeft.getMinValue(), targetLeft.getMaxValue(), targetLeft.getStatus(),
        mappingsTarget.get(0).getRight(), dssOriginal);

    StoreMapping[] ms = new StoreMapping[]{dsmSource};
    StoreMapping[] mt = new StoreMapping[]{dsmTarget};

    return ts.executeOperation(StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARD_MAPPINGS_LOCAL,
        StoreOperationRequestBuilder.replaceShardMappingsLocal(this.getId(), true, shardMap,
            (StoreMapping[]) ArrayUtils.addAll(mt, mappingsTarget.stream().skip(1)
                .map(Pair::getLeft).toArray()),
            (StoreMapping[]) ArrayUtils.addAll(ms, mappingsSource.stream().skip(1)
                .map(Pair::getLeft).toArray())));
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
    throw StoreOperationErrorHandler.onShardMapperErrorLocal(result,
        mappingsSource.get(0).getLeft().getStoreShard().getLocation(),
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
    // undo
    return ts.executeOperation(
        StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARD_MAPPINGS_GLOBAL_END,
        StoreOperationRequestBuilder.replaceShardMappingsGlobal(this.getId(),
            this.getOperationCode(), true, shardMap, mappingsSource, mappingsTarget));
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
      this.getShardMapManager().getCache().deleteShardMap(shardMap);
    }

    // Possible errors are:
    // StoreResult.ShardMapDoesNotExist
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler.onShardMapperErrorGlobal(result, shardMap,
        mappingsSource.get(0).getLeft().getStoreShard(), ShardManagementErrorCategory.RangeShardMap,
        StoreOperationErrorHandler.operationNameFromStoreOperationCode(this.getOperationCode()),
        StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARD_MAPPINGS_GLOBAL_END);
  }

  /**
   * Source location of error.
   */
  @Override
  protected ShardLocation getErrorSourceLocation() {
    return mappingsSource.get(0).getLeft().getStoreShard().getLocation();
  }

  /**
   * Target location of error.
   */
  @Override
  protected ShardLocation getErrorTargetLocation() {
    return mappingsSource.get(0).getLeft().getStoreShard().getLocation();
  }

  /**
   * Error category for error.
   */
  @Override
  protected ShardManagementErrorCategory getErrorCategory() {
    return ShardManagementErrorCategory.RangeShardMap;
  }
}