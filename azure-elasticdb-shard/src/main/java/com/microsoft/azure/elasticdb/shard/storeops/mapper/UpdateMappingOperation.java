package com.microsoft.azure.elasticdb.shard.storeops.mapper;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.cache.CacheStoreMappingUpdatePolicy;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreConnection;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreConnectionKind;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreResult;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreShard;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.store.StoreTransactionScopeKind;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreConnectionInfo;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperation;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationCode;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationErrorHandler;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationState;
import com.microsoft.azure.elasticdb.shard.utils.SqlUtils;
import java.util.UUID;

/**
 * Updates a mapping in given shard map.
 */
public class UpdateMappingOperation extends StoreOperation {

  /**
   * Shard map for which to update the mapping.
   */
  private StoreShardMap shardMap;

  /**
   * Mapping to update.
   */
  private StoreMapping mappingSource;

  /**
   * Updated mapping.
   */
  private StoreMapping mappingTarget;

  /**
   * Lock owner.
   */
  private UUID lockOwnerId;

  /**
   * Error category to use.
   */
  private ShardManagementErrorCategory errorCategory;

  /**
   * Is this a shard location update operation.
   */
  private boolean updateLocation;

  /**
   * Is mapping being taken offline.
   */
  private boolean fromOnlineToOffline;

  /**
   * Pattern for kill commands.
   */
  private String patternForKill;

  /**
   * Creates request to add shard to given shard map.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationCode Store operation code.
   * @param shardMap Shard map for which to update mapping.
   * @param mappingSource Mapping to update.
   * @param mappingTarget Updated mapping.
   * @param patternForKill Pattern for kill commands.
   * @param lockOwnerId Id of lock owner.
   */
  public UpdateMappingOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode,
      StoreShardMap shardMap, StoreMapping mappingSource, StoreMapping mappingTarget,
      String patternForKill, UUID lockOwnerId) {
    this(shardMapManager, UUID.randomUUID(), StoreOperationState.UndoBegin, operationCode, shardMap,
        mappingSource, mappingTarget, patternForKill, lockOwnerId, null, null);
  }

  /**
   * Creates request to add shard to given shard map.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationId Operation id.
   * @param undoStartState State from which Undo operation starts.
   * @param operationCode Store operation code.
   * @param shardMap Shard map for which to update mapping.
   * @param mappingSource Mapping to update.
   * @param mappingTarget Updated mapping.
   * @param patternForKill Pattern for kill commands.
   * @param lockOwnerId Id of lock owner.
   * @param originalShardVersionRemoves Original shard version for removes.
   * @param originalShardVersionAdds Original shard version for adds.
   */
  public UpdateMappingOperation(ShardMapManager shardMapManager, UUID operationId,
      StoreOperationState undoStartState, StoreOperationCode operationCode, StoreShardMap shardMap,
      StoreMapping mappingSource, StoreMapping mappingTarget, String patternForKill,
      UUID lockOwnerId, UUID originalShardVersionRemoves, UUID originalShardVersionAdds) {
    super(shardMapManager, operationId, undoStartState, operationCode, originalShardVersionRemoves,
        originalShardVersionAdds);
    this.shardMap = shardMap;
    this.mappingSource = mappingSource;
    this.mappingTarget = mappingTarget;
    this.lockOwnerId = lockOwnerId;

    errorCategory = (operationCode == StoreOperationCode.UpdateRangeMapping
        || operationCode == StoreOperationCode.UpdateRangeMappingWithOffline)
        ? ShardManagementErrorCategory.RangeShardMap : ShardManagementErrorCategory.ListShardMap;

    updateLocation =
        this.mappingSource.getStoreShard().getId() != this.mappingTarget.getStoreShard().getId();

    fromOnlineToOffline = operationCode == StoreOperationCode.UpdatePointMappingWithOffline
        || operationCode == StoreOperationCode.UpdateRangeMappingWithOffline;

    this.patternForKill = patternForKill;
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
            .getValue() ? mappingSource.getStoreShard().getLocation() : null);
    tempVar.setTargetLocation((updateLocation && this.getUndoStartState().getValue()
        <= StoreOperationState.UndoLocalTargetBeginTransaction.getValue()) ? mappingTarget
        .getStoreShard().getLocation() : null);
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
            .updateShardMappingGlobal(this.getId(), this.getOperationCode(), false, patternForKill,
                shardMap, mappingSource, mappingTarget, lockOwnerId)); // undo
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
      // Remove mapping from cache.
      this.getShardMapManager().getCache().deleteMapping(mappingSource);
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
    throw StoreOperationErrorHandler.onShardMapperErrorGlobal(result, shardMap,
        result.getResult() == StoreResult.MappingRangeAlreadyMapped ? mappingTarget.getStoreShard()
            : mappingSource.getStoreShard(), errorCategory,
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
    StoreResults result;

    if (updateLocation) {
      result = ts
          .executeOperation(StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARD_MAPPINGS_LOCAL,
              StoreOperationRequestBuilder
                  .removeShardMappingLocal(this.getId(), false, shardMap, mappingSource));
    } else {
      result = ts
          .executeOperation(StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARD_MAPPINGS_LOCAL,
              StoreOperationRequestBuilder
                  .updateShardMappingLocal(this.getId(), false, shardMap, mappingSource,
                      mappingTarget));
    }

    // We need to treat the kill connection operation separately, the reason
    // being that we cannot perform kill operations within a transaction.
    if (result.getResult() == StoreResult.Success && fromOnlineToOffline) {
      this.killConnectionsOnSourceShard();
    }

    return result;
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
        .onShardMapperErrorLocal(result, mappingSource.getStoreShard().getLocation(),
            StoreOperationErrorHandler.operationNameFromStoreOperationCode(this.getOperationCode()),
            StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARD_MAPPINGS_LOCAL);
  }

  /**
   * Performs the LSM operation on the target shard.
   *
   * @param ts Transaction scope.
   * @return Result of the operation.
   */
  @Override
  public StoreResults doLocalTargetExecute(IStoreTransactionScope ts) {
    assert updateLocation;
    return ts.executeOperation(StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARD_MAPPINGS_LOCAL,
        StoreOperationRequestBuilder
            .addShardMappingLocal(this.getId(), false, shardMap, mappingTarget));
  }

  /**
   * Handles errors from the the LSM operation on the target shard.
   *
   * @param result Operation result.
   */
  @Override
  public void handleDoLocalTargetExecuteError(StoreResults result) {
    // Possible errors are:
    // StoreResult.UnableToKillSessions
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler
        .onShardMapperErrorLocal(result, mappingTarget.getStoreShard().getLocation(),
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
                .updateShardMappingGlobal(this.getId(), this.getOperationCode(), false,
                    patternForKill,
                    shardMap, mappingSource, mappingTarget, lockOwnerId)); // undo
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
        .onShardMapperErrorGlobal(result, shardMap, mappingSource.getStoreShard(), errorCategory,
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
    this.getShardMapManager().getCache().deleteMapping(mappingSource);

    // Add to cache.
    this.getShardMapManager().getCache()
        .addOrUpdateMapping(mappingTarget, CacheStoreMappingUpdatePolicy.OverwriteExisting);
  }

  /**
   * Performs the undo of LSM operation on the source shard.
   *
   * @param ts Transaction scope.
   * @return Result of the operation.
   */
  @Override
  public StoreResults undoLocalSourceExecute(IStoreTransactionScope ts) {
    StoreMapping dsmSource = new StoreMapping(mappingSource.getId(), shardMap.getId(),
        mappingSource.getMinValue(), mappingSource.getMaxValue(), mappingSource.getStatus(),
        lockOwnerId, new StoreShard(mappingSource.getStoreShard().getId(),
        this.getOriginalShardVersionRemoves(), shardMap.getId(),
        mappingSource.getStoreShard().getLocation(), mappingSource.getStoreShard().getStatus())
    );

    if (updateLocation) {
      return ts
          .executeOperation(StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARD_MAPPINGS_LOCAL,
              StoreOperationRequestBuilder
                  .addShardMappingLocal(this.getId(), true, shardMap, dsmSource));
    } else {
      StoreMapping dsmTarget = new StoreMapping(mappingTarget.getId(), shardMap.getId(),
          mappingTarget.getMinValue(), mappingTarget.getMaxValue(), mappingTarget.getStatus(),
          lockOwnerId, new StoreShard(mappingTarget.getStoreShard().getId(),
          this.getOriginalShardVersionRemoves(), shardMap.getId(),
          mappingTarget.getStoreShard().getLocation(), mappingTarget.getStoreShard().getStatus())
      );

      return ts
          .executeOperation(StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARD_MAPPINGS_LOCAL,
              StoreOperationRequestBuilder
                  .updateShardMappingLocal(this.getId(), true, shardMap, dsmTarget, dsmSource));
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
    throw StoreOperationErrorHandler
        .onShardMapperErrorLocal(result, mappingSource.getStoreShard().getLocation(),
            StoreOperationErrorHandler.operationNameFromStoreOperationCode(this.getOperationCode()),
            StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARD_MAPPINGS_LOCAL);
  }

  /**
   * Performs undo of LSM operation on the target shard.
   *
   * @param ts Transaction scope.
   * @return Result of the operation.
   */
  @Override
  public StoreResults undoLocalTargetExecute(IStoreTransactionScope ts) {
    StoreMapping dsmTarget = new StoreMapping(mappingTarget.getId(), shardMap.getId(),
        mappingTarget.getMinValue(), mappingTarget.getMaxValue(), mappingTarget.getStatus(),
        lockOwnerId, new StoreShard(mappingTarget.getStoreShard().getId(),
        this.getOriginalShardVersionAdds(), shardMap.getId(),
        mappingTarget.getStoreShard().getLocation(),
        mappingTarget.getStoreShard().getStatus())
    );

    return ts.executeOperation(StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARD_MAPPINGS_LOCAL,
        StoreOperationRequestBuilder
            .removeShardMappingLocal(this.getId(), true, shardMap, dsmTarget));
  }

  /**
   * Performs undo of LSM operation on the target shard.
   *
   * @param result Operation result.
   */
  @Override
  public void handleUndoLocalTargetExecuteError(StoreResults result) {
    // Possible errors are:
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler
        .onShardMapperErrorLocal(result, mappingSource.getStoreShard().getLocation(),
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
                .updateShardMappingGlobal(this.getId(), this.getOperationCode(), true,
                    patternForKill,
                    shardMap, mappingSource, mappingTarget, lockOwnerId)); // undo
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
        .onShardMapperErrorGlobal(result, shardMap, mappingSource.getStoreShard(), errorCategory,
            StoreOperationErrorHandler.operationNameFromStoreOperationCode(this.getOperationCode()),
            StoreOperationRequestBuilder.SP_BULK_OPERATION_SHARD_MAPPINGS_GLOBAL_END);
  }

  /**
   * Source location of error.
   */
  @Override
  protected ShardLocation getErrorSourceLocation() {
    return mappingSource.getStoreShard().getLocation();
  }

  /**
   * Target location of error.
   */
  @Override
  protected ShardLocation getErrorTargetLocation() {
    return mappingTarget.getStoreShard().getLocation();
  }

  /**
   * Error category for error.
   */
  @Override
  protected ShardManagementErrorCategory getErrorCategory() {
    return errorCategory;
  }

  /**
   * Terminates connection on the source shard object.
   */
  private void killConnectionsOnSourceShard() {
    SqlUtils.withSqlExceptionHandling(() -> {
      String sourceShardConnectionString = this
          .getConnectionStringForShardLocation(mappingSource.getStoreShard().getLocation());

      StoreResults result = null;

      try (IStoreConnection connectionForKill = this.getShardMapManager()
          .getStoreConnectionFactory()
          .getConnection(StoreConnectionKind.LocalSource, sourceShardConnectionString)) {
        connectionForKill.open();

        try (IStoreTransactionScope ts = connectionForKill
            .getTransactionScope(StoreTransactionScopeKind.NonTransactional)) {
          result = ts
              .executeOperation(
                  StoreOperationRequestBuilder.SP_KILL_SESSIONS_FOR_SHARD_MAPPING_LOCAL,
                  StoreOperationRequestBuilder.killSessionsForShardMappingLocal(patternForKill));
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

      if (result.getResult() != StoreResult.Success) {
        // Possible errors are:
        // StoreResult.UnableToKillSessions
        // StoreResult.StoreVersionMismatch
        // StoreResult.MissingParametersForStoredProcedure
        throw StoreOperationErrorHandler
            .onShardMapErrorLocal(result, shardMap, mappingSource.getStoreShard().getLocation(),
                errorCategory, StoreOperationErrorHandler
                    .operationNameFromStoreOperationCode(this.getOperationCode()),
                StoreOperationRequestBuilder.SP_KILL_SESSIONS_FOR_SHARD_MAPPING_LOCAL);
      }
    });
  }
}
