package com.microsoft.azure.elasticdb.shard.storeops.map;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
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
 * Removes a shard from given shard map.
 */
public class RemoveShardOperation extends StoreOperation {

  /**
   * Shard map for which to remove the shard.
   */
  private StoreShardMap _shardMap;

  /**
   * Shard to remove.
   */
  private StoreShard _shard;

  /**
   * Creates request to remove shard from given shard map.
   *
   * @param shardMapManager Shard map manager object.
   * @param shardMap Shard map for which to remove shard.
   * @param shard Shard to remove.
   */
  public RemoveShardOperation(ShardMapManager shardMapManager, StoreShardMap shardMap,
      StoreShard shard) {
    this(shardMapManager, UUID.randomUUID(), StoreOperationState.UndoBegin, shardMap, shard);
  }

  /**
   * Creates request to remove shard from given shard map.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationId Operation id.
   * @param undoStartState State from which Undo operation starts.
   * @param shardMap Shard map for which to remove shard.
   * @param shard Shard to remove.
   */
  public RemoveShardOperation(ShardMapManager shardMapManager, UUID operationId,
      StoreOperationState undoStartState, StoreShardMap shardMap, StoreShard shard) {
    super(shardMapManager, operationId, undoStartState, StoreOperationCode.RemoveShard, null, null);
    _shardMap = shardMap;
    _shard = shard;
  }

  /**
   * Requests the derived class to provide information regarding the connections
   * needed for the operation.
   *
   * @return Information about shards involved in the operation.
   */
  @Override
  public StoreConnectionInfo GetStoreConnectionInfo() {
    StoreConnectionInfo tempVar = new StoreConnectionInfo();
    ShardLocation loc =
        this.getUndoStartState().getValue() <= StoreOperationState.UndoLocalSourceBeginTransaction
            .getValue() ? _shard.getLocation() : null;
    tempVar.setSourceLocation(loc);
    return tempVar;
  }

  /**
   * Performs the initial GSM operation prior to LSM operations.
   *
   * @param ts Transaction scope.
   * @return Pending operations on the target objects if any.
   */
  @Override
  public StoreResults DoGlobalPreLocalExecute(IStoreTransactionScope ts) {
    return ts.ExecuteOperation(StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARDS_GLOBAL_BEGIN,
        StoreOperationRequestBuilder
            .removeShardGlobal(this.getId(), this.getOperationCode(), false, _shardMap,
                _shard)); // undo
  }

  /**
   * Handles errors from the initial GSM operation prior to LSM operations.
   *
   * @param result Operation result.
   */
  @Override
  public void HandleDoGlobalPreLocalExecuteError(StoreResults result) {
    if (result.getResult() == StoreResult.ShardMapDoesNotExist) {
      // Remove shard map from cache.
      this.getShardMapManager().getCache().deleteShardMap(_shardMap);
    }

    // Possible errors are:
    // StoreResult.ShardMapDoesNotExist
    // StoreResult.ShardDoesNotExist
    // StoreResult.ShardVersionMismatch
    // StoreResult.ShardHasMappings
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler
        .OnShardMapErrorGlobal(result, _shardMap, _shard, ShardManagementErrorCategory.ShardMap,
            StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()),
            StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARDS_GLOBAL_BEGIN);
  }

  /**
   * Performs the LSM operation on the source shard.
   *
   * @param ts Transaction scope.
   * @return Result of the operation.
   */
  @Override
  public StoreResults DoLocalSourceExecute(IStoreTransactionScope ts) {
    // Now actually add the shard entries.
    return ts.ExecuteOperation(StoreOperationRequestBuilder.SP_REMOVE_SHARD_LOCAL,
        StoreOperationRequestBuilder.removeShardLocal(this.getId(), _shardMap, _shard));
  }

  /**
   * Handles errors from the the LSM operation on the source shard.
   *
   * @param result Operation result.
   */
  @Override
  public void HandleDoLocalSourceExecuteError(StoreResults result) {
    // Possible errors are:
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler.OnShardMapErrorLocal(result, _shardMap, _shard.getLocation(),
        ShardManagementErrorCategory.ShardMap,
        StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()),
        StoreOperationRequestBuilder.SP_REMOVE_SHARD_LOCAL);
  }

  /**
   * Performs the final GSM operation after the LSM operations.
   *
   * @param ts Transaction scope.
   * @return Pending operations on the target objects if any.
   */
  @Override
  public StoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts) {
    return ts.ExecuteOperation(StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARDS_GLOBAL_END,
        StoreOperationRequestBuilder
            .removeShardGlobal(this.getId(), this.getOperationCode(), false, _shardMap,
                _shard)); // undo
  }

  /**
   * Handles errors from the final GSM operation after the LSM operations.
   *
   * @param result Operation result.
   */
  @Override
  public void HandleDoGlobalPostLocalExecuteError(StoreResults result) {
    if (result.getResult() == StoreResult.ShardMapDoesNotExist) {
      // Remove shard map from cache.
      this.getShardMapManager().getCache().deleteShardMap(_shardMap);
    }

    // Possible errors are:
    // StoreResult.ShardMapDoesNotExist
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler
        .OnShardMapErrorGlobal(result, _shardMap, _shard, ShardManagementErrorCategory.ShardMap,
            StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()),
            StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARDS_GLOBAL_END);
  }

  /**
   * Performs the undo of LSM operation on the source shard.
   *
   * @param ts Transaction scope.
   * @return Result of the operation.
   */
  @Override
  public StoreResults UndoLocalSourceExecute(IStoreTransactionScope ts) {
    // Adds back the removed shard entries.
    return ts.ExecuteOperation(StoreOperationRequestBuilder.SP_ADD_SHARD_LOCAL,
        StoreOperationRequestBuilder.addShardLocal(this.getId(), true, _shardMap, _shard));
  }

  /**
   * Handles errors from the undo of LSM operation on the source shard.
   *
   * @param result Operation result.
   */
  @Override
  public void HandleUndoLocalSourceExecuteError(StoreResults result) {
    // Possible errors are:
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler.OnShardMapErrorLocal(result, _shardMap, _shard.getLocation(),
        ShardManagementErrorCategory.ShardMap,
        StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()),
        StoreOperationRequestBuilder.SP_ADD_SHARD_LOCAL);
  }

  /**
   * Performs the undo of GSM operation after LSM operations.
   *
   * @param ts Transaction scope.
   * @return Pending operations on the target objects if any.
   */
  @Override
  public StoreResults UndoGlobalPostLocalExecute(IStoreTransactionScope ts) {
    return ts.ExecuteOperation(StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARDS_GLOBAL_END,
        StoreOperationRequestBuilder
            .removeShardGlobal(this.getId(), this.getOperationCode(), true, _shardMap,
                _shard)); // undo
  }

  /**
   * Handles errors from the undo of GSM operation after LSM operations.
   *
   * @param result Operation result.
   */
  @Override
  public void HandleUndoGlobalPostLocalExecuteError(StoreResults result) {
    if (result.getResult() == StoreResult.ShardMapDoesNotExist) {
      // Remove shard map from cache.
      this.getShardMapManager().getCache().deleteShardMap(_shardMap);
    }

    // Possible errors are:
    // StoreResult.ShardMapDoesNotExist
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler
        .OnShardMapErrorGlobal(result, _shardMap, _shard, ShardManagementErrorCategory.ShardMap,
            StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()),
            StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARDS_GLOBAL_END);
  }

  /**
   * Source location of error.
   */
  @Override
  protected ShardLocation getErrorSourceLocation() {
    return _shard.getLocation();
  }

  /**
   * Target location of error.
   */
  @Override
  protected ShardLocation getErrorTargetLocation() {
    return _shard.getLocation();
  }

  /**
   * Error category for error.
   */
  @Override
  protected ShardManagementErrorCategory getErrorCategory() {
    return ShardManagementErrorCategory.ShardMap;
  }

  @Override
  public void close() throws Exception {

  }
}