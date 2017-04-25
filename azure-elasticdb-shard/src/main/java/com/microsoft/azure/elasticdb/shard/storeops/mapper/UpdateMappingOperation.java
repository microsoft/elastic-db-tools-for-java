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
  private StoreShardMap _shardMap;

  /**
   * Mapping to update.
   */
  private StoreMapping _mappingSource;

  /**
   * Updated mapping.
   */
  private StoreMapping _mappingTarget;

  /**
   * Lock owner.
   */
  private UUID _lockOwnerId;

  /**
   * Error category to use.
   */
  private ShardManagementErrorCategory _errorCategory;

  /**
   * Is this a shard location update operation.
   */
  private boolean _updateLocation;

  /**
   * Is mapping being taken offline.
   */
  private boolean _fromOnlineToOffline;

  /**
   * Pattern for kill commands.
   */
  private String _patternForKill;

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
    _shardMap = shardMap;
    _mappingSource = mappingSource;
    _mappingTarget = mappingTarget;
    _lockOwnerId = lockOwnerId;

    _errorCategory = (operationCode == StoreOperationCode.UpdateRangeMapping
        || operationCode == StoreOperationCode.UpdateRangeMappingWithOffline)
        ? ShardManagementErrorCategory.RangeShardMap : ShardManagementErrorCategory.ListShardMap;

    _updateLocation =
        _mappingSource.getStoreShard().getId() != _mappingTarget.getStoreShard().getId();

    _fromOnlineToOffline = operationCode == StoreOperationCode.UpdatePointMappingWithOffline
        || operationCode == StoreOperationCode.UpdateRangeMappingWithOffline;

    _patternForKill = patternForKill;
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
    //TODO: tempVar.getSourceLocation() = this.getUndoStartState() <= StoreOperationState.UndoLocalSourceBeginTransaction ? _mappingSource.getStoreShard().getLocation() : null;
    //TODO: tempVar.getTargetLocation() = (_updateLocation && this.getUndoStartState() <= StoreOperationState.UndoLocalTargetBeginTransaction) ? _mappingTarget.getStoreShard().getLocation() : null;
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
    return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalBegin,
        StoreOperationRequestBuilder
            .UpdateShardMappingGlobal(this.getId(), this.getOperationCode(), false, _patternForKill,
                _shardMap, _mappingSource, _mappingTarget, _lockOwnerId)); // undo
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
      this.getManager().getCache().deleteShardMap(_shardMap);
    }

    if (result.getResult() == StoreResult.MappingDoesNotExist) {
      // Remove mapping from cache.
      this.getManager().getCache().deleteMapping(_mappingSource);
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
    throw StoreOperationErrorHandler.OnShardMapperErrorGlobal(result, _shardMap,
        result.getResult() == StoreResult.MappingRangeAlreadyMapped ? _mappingTarget.getStoreShard()
            : _mappingSource.getStoreShard(), _errorCategory,
        StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()),
        StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalBegin);
  }

  /**
   * Performs the LSM operation on the source shard.
   *
   * @param ts Transaction scope.
   * @return Result of the operation.
   */
  @Override
  public StoreResults DoLocalSourceExecute(IStoreTransactionScope ts) {
    StoreResults result;

    if (_updateLocation) {
      result = ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal,
          StoreOperationRequestBuilder
              .RemoveShardMappingLocal(this.getId(), false, _shardMap, _mappingSource));
    } else {
      result = ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal,
          StoreOperationRequestBuilder
              .UpdateShardMappingLocal(this.getId(), false, _shardMap, _mappingSource,
                  _mappingTarget));
    }

    // We need to treat the kill connection operation separately, the reason
    // being that we cannot perform kill operations within a transaction.
    if (result.getResult() == StoreResult.Success && _fromOnlineToOffline) {
      this.KillConnectionsOnSourceShard();
    }

    return result;
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
    throw StoreOperationErrorHandler
        .OnShardMapperErrorLocal(result, _mappingSource.getStoreShard().getLocation(),
            StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()),
            StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal);
  }

  /**
   * Performs the LSM operation on the target shard.
   *
   * @param ts Transaction scope.
   * @return Result of the operation.
   */
  @Override
  public StoreResults DoLocalTargetExecute(IStoreTransactionScope ts) {
    assert _updateLocation;
    return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal,
        StoreOperationRequestBuilder
            .AddShardMappingLocal(this.getId(), false, _shardMap, _mappingTarget));
  }

  /**
   * Handles errors from the the LSM operation on the target shard.
   *
   * @param result Operation result.
   */
  @Override
  public void HandleDoLocalTargetExecuteError(StoreResults result) {
    // Possible errors are:
    // StoreResult.UnableToKillSessions
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler
        .OnShardMapperErrorLocal(result, _mappingTarget.getStoreShard().getLocation(),
            StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()),
            StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal);
  }

  /**
   * Performs the final GSM operation after the LSM operations.
   *
   * @param ts Transaction scope.
   * @return Pending operations on the target objects if any.
   */
  @Override
  public StoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts) {
    return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalEnd,
        StoreOperationRequestBuilder
            .UpdateShardMappingGlobal(this.getId(), this.getOperationCode(), false, _patternForKill,
                _shardMap, _mappingSource, _mappingTarget, _lockOwnerId)); // undo
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
      this.getManager().getCache().deleteShardMap(_shardMap);
    }

    // Possible errors are:
    // StoreResult.ShardMapDoesNotExist
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler
        .OnShardMapperErrorGlobal(result, _shardMap, _mappingSource.getStoreShard(), _errorCategory,
            StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()),
            StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalEnd);
  }

  /**
   * Refreshes the cache on successful commit of the final GSM operation after the LSM operations.
   *
   * @param result Operation result.
   */
  @Override
  public void DoGlobalPostLocalUpdateCache(StoreResults result) {
    // Remove from cache.
    this.getManager().getCache().deleteMapping(_mappingSource);

    // Add to cache.
    this.getManager().getCache()
        .addOrUpdateMapping(_mappingTarget, CacheStoreMappingUpdatePolicy.OverwriteExisting);
  }

  /**
   * Performs the undo of LSM operation on the source shard.
   *
   * @param ts Transaction scope.
   * @return Result of the operation.
   */
  @Override
  public StoreResults UndoLocalSourceExecute(IStoreTransactionScope ts) {
    StoreMapping dsmSource = new StoreMapping(_mappingSource.getId()
        , _shardMap.getId()
        , _mappingSource.getMinValue()
        , _mappingSource.getMaxValue()
        , _mappingSource.getStatus()
        , _lockOwnerId
        , new StoreShard(_mappingSource.getStoreShard().getId(),
        this.getOriginalShardVersionRemoves(), _shardMap.getId(),
        _mappingSource.getStoreShard().getLocation(), _mappingSource.getStoreShard().getStatus())
    );

    if (_updateLocation) {
      return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal,
          StoreOperationRequestBuilder
              .AddShardMappingLocal(this.getId(), true, _shardMap, dsmSource));
    } else {
      StoreMapping dsmTarget = new StoreMapping(_mappingTarget.getId()
          , _shardMap.getId()
          , _mappingTarget.getMinValue()
          , _mappingTarget.getMaxValue()
          , _mappingTarget.getStatus()
          , _lockOwnerId
          , new StoreShard(_mappingTarget.getStoreShard().getId(),
          this.getOriginalShardVersionRemoves(), _shardMap.getId(),
          _mappingTarget.getStoreShard().getLocation(), _mappingTarget.getStoreShard().getStatus())
      );

      return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal,
          StoreOperationRequestBuilder
              .UpdateShardMappingLocal(this.getId(), true, _shardMap, dsmTarget, dsmSource));
    }
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
    throw StoreOperationErrorHandler
        .OnShardMapperErrorLocal(result, _mappingSource.getStoreShard().getLocation(),
            StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()),
            StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal);
  }

  /**
   * Performs undo of LSM operation on the target shard.
   *
   * @param ts Transaction scope.
   * @return Result of the operation.
   */
  @Override
  public StoreResults UndoLocalTargetExecute(IStoreTransactionScope ts) {
    StoreMapping dsmTarget = new StoreMapping(_mappingTarget.getId()
        , _shardMap.getId()
        , _mappingTarget.getMinValue()
        , _mappingTarget.getMaxValue()
        , _mappingTarget.getStatus()
        , _lockOwnerId
        , new StoreShard(_mappingTarget.getStoreShard().getId(), this.getOriginalShardVersionAdds(),
        _shardMap.getId(), _mappingTarget.getStoreShard().getLocation(),
        _mappingTarget.getStoreShard().getStatus())
    );

    return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal,
        StoreOperationRequestBuilder
            .RemoveShardMappingLocal(this.getId(), true, _shardMap, dsmTarget));
  }

  /**
   * Performs undo of LSM operation on the target shard.
   *
   * @param result Operation result.
   */
  @Override
  public void HandleUndoLocalTargetExecuteError(StoreResults result) {
    // Possible errors are:
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler
        .OnShardMapperErrorLocal(result, _mappingSource.getStoreShard().getLocation(),
            StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()),
            StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal);
  }

  /**
   * Performs the undo of GSM operation after LSM operations.
   *
   * @param ts Transaction scope.
   * @return Pending operations on the target objects if any.
   */
  @Override
  public StoreResults UndoGlobalPostLocalExecute(IStoreTransactionScope ts) {
    return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalEnd,
        StoreOperationRequestBuilder
            .UpdateShardMappingGlobal(this.getId(), this.getOperationCode(), true, _patternForKill,
                _shardMap, _mappingSource, _mappingTarget, _lockOwnerId)); // undo
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
      this.getManager().getCache().deleteShardMap(_shardMap);
    }

    // Possible errors are:
    // StoreResult.ShardMapDoesNotExist
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler
        .OnShardMapperErrorGlobal(result, _shardMap, _mappingSource.getStoreShard(), _errorCategory,
            StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()),
            StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalEnd);
  }

  /**
   * Source location of error.
   */
  @Override
  protected ShardLocation getErrorSourceLocation() {
    return _mappingSource.getStoreShard().getLocation();
  }

  /**
   * Target location of error.
   */
  @Override
  protected ShardLocation getErrorTargetLocation() {
    return _mappingTarget.getStoreShard().getLocation();
  }

  /**
   * Error category for error.
   */
  @Override
  protected ShardManagementErrorCategory getErrorCategory() {
    return _errorCategory;
  }

  /**
   * Terminates connection on the source shard object.
   */
  private void KillConnectionsOnSourceShard() {
    SqlUtils.WithSqlExceptionHandling(() -> {
      String sourceShardConnectionString = this
          .GetConnectionStringForShardLocation(_mappingSource.getStoreShard().getLocation());

      StoreResults result = null;

      try (IStoreConnection connectionForKill = this.getManager().getStoreConnectionFactory()
          .GetConnection(StoreConnectionKind.LocalSource, sourceShardConnectionString)) {
        connectionForKill.Open();

        try (IStoreTransactionScope ts = connectionForKill
            .GetTransactionScope(StoreTransactionScopeKind.NonTransactional)) {
          result = ts
              .ExecuteOperation(StoreOperationRequestBuilder.SpKillSessionsForShardMappingLocal,
                  StoreOperationRequestBuilder.KillSessionsForShardMappingLocal(_patternForKill));
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
            .OnShardMapErrorLocal(result, _shardMap, _mappingSource.getStoreShard().getLocation(),
                _errorCategory, StoreOperationErrorHandler
                    .OperationNameFromStoreOperationCode(this.getOperationCode()),
                StoreOperationRequestBuilder.SpKillSessionsForShardMappingLocal);
      }
    });
  }

  @Override
  public void close() throws Exception {

  }
}
