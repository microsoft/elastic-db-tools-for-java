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
import com.microsoft.azure.elasticdb.shard.utils.GlobalConstants;
import com.microsoft.azure.elasticdb.shard.utils.SqlUtils;
import java.util.UUID;

/**
 * Adds a shard to given shard map.
 */
public class AddShardOperation extends StoreOperation {

  /**
   * Shard map for which to add the shard.
   */
  private StoreShardMap shardMap;

  /**
   * Shard to add.
   */
  private StoreShard shard;

  /**
   * Creates request to add shard to given shard map.
   *
   * @param shardMapManager Shard map manager object.
   * @param shardMap Shard map for which to add shard.
   * @param shard Shard to add.
   */
  public AddShardOperation(ShardMapManager shardMapManager, StoreShardMap shardMap,
      StoreShard shard) {
    this(shardMapManager, UUID.randomUUID(), StoreOperationState.UndoBegin, shardMap, shard);
  }

  /**
   * Creates request to add shard to given shard map.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationId Operation id.
   * @param undoStartState State from which Undo operation starts.
   * @param shardMap Shard map for which to add shard.
   * @param shard Shard to add.
   */
  public AddShardOperation(ShardMapManager shardMapManager, UUID operationId,
      StoreOperationState undoStartState, StoreShardMap shardMap, StoreShard shard) {
    super(shardMapManager, operationId, undoStartState, StoreOperationCode.AddShard, null, null);
    this.shardMap = shardMap;
    this.shard = shard;
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
    ShardLocation location =
        this.getUndoStartState().getValue() <= StoreOperationState.UndoLocalSourceBeginTransaction
            .getValue() ? shard.getLocation() : null;
    tempVar.setSourceLocation(location);
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
    return ts.executeOperation(StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARDS_GLOBAL_BEGIN,
        StoreOperationRequestBuilder
            .addShardGlobal(this.getId(), this.getOperationCode(), false, shardMap,
                shard)); // undo
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

    // Possible errors are:
    // StoreResult.ShardMapDoesNotExist
    // StoreResult.ShardExists
    // StoreResult.ShardLocationExists
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler
        .onShardMapErrorGlobal(result, shardMap, shard, ShardManagementErrorCategory.ShardMap,
            StoreOperationErrorHandler.operationNameFromStoreOperationCode(this.getOperationCode()),
            StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARDS_GLOBAL_BEGIN);
  }

  /**
   * Performs the LSM operation on the source shard.
   *
   * @param ts Transaction scope.
   * @return Result of the operation.
   */
  @Override
  public StoreResults doLocalSourceExecute(IStoreTransactionScope ts) {
    StoreResults checkResult = ts
        .executeCommandSingle(SqlUtils.getCheckIfExistsLocalScript().get(0));

    // Shard not already deployed, just need to add the proper entries.
    if (checkResult.getStoreVersion() == null) {
      // create initial version of LSM
      ts.executeCommandBatch(SqlUtils.getCreateLocalScript());

      // now upgrade LSM to latest version
      ts.executeCommandBatch(SqlUtils.filterUpgradeCommands(SqlUtils.getUpgradeLocalScript(),
          GlobalConstants.LsmVersionClient));
    }

    // Now actually add the shard entries.
    return ts.executeOperation(StoreOperationRequestBuilder.SP_ADD_SHARD_LOCAL,
        StoreOperationRequestBuilder.addShardLocal(this.getId(), false, shardMap, shard));
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
    throw StoreOperationErrorHandler.onShardMapErrorLocal(result, shardMap, shard.getLocation(),
        ShardManagementErrorCategory.ShardMap,
        StoreOperationErrorHandler.operationNameFromStoreOperationCode(this.getOperationCode()),
        StoreOperationRequestBuilder.SP_ADD_SHARD_LOCAL);
  }

  /**
   * Performs the final GSM operation after the LSM operations.
   *
   * @param ts Transaction scope.
   * @return Pending operations on the target objects if any.
   */
  @Override
  public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
    return ts.executeOperation(StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARDS_GLOBAL_END,
        StoreOperationRequestBuilder
            .addShardGlobal(this.getId(), this.getOperationCode(), false, shardMap,
                shard)); // undo
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
        .onShardMapErrorGlobal(result, shardMap, shard, ShardManagementErrorCategory.ShardMap,
            StoreOperationErrorHandler.operationNameFromStoreOperationCode(this.getOperationCode()),
            StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARDS_GLOBAL_END);
  }

  /**
   * Performs the undo of LSM operation on the source shard.
   *
   * @param ts Transaction scope.
   * @return Result of the operation.
   */
  @Override
  public StoreResults undoLocalSourceExecute(IStoreTransactionScope ts) {
    StoreResults checkResult = ts
        .executeCommandSingle(SqlUtils.getCheckIfExistsLocalScript().get(0));

    if (checkResult.getStoreVersion() != null) {
      // Remove the added shard entries.
      return ts.executeOperation(StoreOperationRequestBuilder.SP_REMOVE_SHARD_LOCAL,
          StoreOperationRequestBuilder.removeShardLocal(this.getId(), shardMap, shard));
    } else {
      // If version is < 0, then shard never got deployed, consider it a success.
      return new StoreResults();
    }
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
    throw StoreOperationErrorHandler.onShardMapErrorLocal(result, shardMap, shard.getLocation(),
        ShardManagementErrorCategory.ShardMap,
        StoreOperationErrorHandler.operationNameFromStoreOperationCode(this.getOperationCode()),
        StoreOperationRequestBuilder.SP_ADD_SHARD_LOCAL);
  }

  /**
   * Performs the undo of GSM operation after LSM operations.
   *
   * @param ts Transaction scope.
   * @return Pending operations on the target objects if any.
   */
  @Override
  public StoreResults undoGlobalPostLocalExecute(IStoreTransactionScope ts) {
    return ts.executeOperation(StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARDS_GLOBAL_END,
        StoreOperationRequestBuilder
            .addShardGlobal(this.getId(), this.getOperationCode(), true, shardMap,
                shard)); // undo
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
    throw StoreOperationErrorHandler
        .onShardMapErrorGlobal(result, shardMap, shard, ShardManagementErrorCategory.ShardMap,
            StoreOperationErrorHandler.operationNameFromStoreOperationCode(this.getOperationCode()),
            StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARDS_GLOBAL_END);
  }

  /**
   * Source location of error.
   */
  @Override
  protected ShardLocation getErrorSourceLocation() {
    return shard.getLocation();
  }

  /**
   * Target location of error.
   */
  @Override
  protected ShardLocation getErrorTargetLocation() {
    return shard.getLocation();
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
