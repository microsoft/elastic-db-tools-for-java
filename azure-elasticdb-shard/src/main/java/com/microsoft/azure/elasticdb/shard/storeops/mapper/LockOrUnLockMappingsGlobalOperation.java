package com.microsoft.azure.elasticdb.shard.storeops.mapper;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.base.LockOwnerIdOpType;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreLogEntry;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreResult;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperation;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationErrorHandler;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;
import java.io.IOException;
import java.util.UUID;

/**
 * Locks or unlocks given mappings GSM.
 */
public class LockOrUnLockMappingsGlobalOperation extends StoreOperationGlobal {

  /**
   * Shard map manager object.
   */
  private ShardMapManager _shardMapManager;

  /**
   * Shard map to add.
   */
  private StoreShardMap _shardMap;

  /**
   * Mapping to lock or unlock.
   */
  private StoreMapping _mapping;

  /**
   * Lock owner id.
   */
  private UUID _lockOwnerId;

  /**
   * Operation type.
   */
  private LockOwnerIdOpType _lockOpType;

  /**
   * Error category to use.
   */
  private ShardManagementErrorCategory _errorCategory;

  /**
   * Constructs request to lock or unlock given mappings in GSM.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationName Operation name, useful for diagnostics.
   * @param shardMap Shard map to add.
   * @param mapping Mapping to lock or unlock. Null means all mappings.
   * @param lockOwnerId Lock owner.
   * @param lockOpType Lock operation type.
   * @param errorCategory Error category.
   */
  public LockOrUnLockMappingsGlobalOperation(ShardMapManager shardMapManager, String operationName,
      StoreShardMap shardMap, StoreMapping mapping, UUID lockOwnerId, LockOwnerIdOpType lockOpType,
      ShardManagementErrorCategory errorCategory) {
    super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), operationName);
    _shardMapManager = shardMapManager;
    _shardMap = shardMap;
    _mapping = mapping;
    _lockOwnerId = lockOwnerId;
    _lockOpType = lockOpType;
    _errorCategory = errorCategory;

    assert mapping != null || (lockOpType == LockOwnerIdOpType.UnlockAllMappingsForId
        || lockOpType == LockOwnerIdOpType.UnlockAllMappings);
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
  public StoreResults DoGlobalExecute(IStoreTransactionScope ts) {
    return ts
        .ExecuteOperation(StoreOperationRequestBuilder.SP_LOCK_OR_UN_LOCK_SHARD_MAPPINGS_GLOBAL,
            StoreOperationRequestBuilder
                .lockOrUnLockShardMappingsGlobal(_shardMap, _mapping, _lockOwnerId, _lockOpType));
  }

  /**
   * Handles errors from the GSM operation after the LSM operations.
   *
   * @param result Operation result.
   */
  @Override
  public void HandleDoGlobalExecuteError(StoreResults result) {
    if (result.getResult() == StoreResult.ShardMapDoesNotExist) {
      // Remove shard map from cache.
      _shardMapManager.getCache().deleteShardMap(_shardMap);
    }

    if (result.getResult() == StoreResult.MappingDoesNotExist) {
      assert _mapping != null;

      // Remove mapping from cache.
      _shardMapManager.getCache().deleteMapping(_mapping);
    }

    // Possible errors are:
    // StoreResult.ShardMapDoesNotExist
    // StoreResult.MappingDoesNotExist
    // StoreResult.MappingAlreadyLocked
    // StoreResult.MappingLockOwnerIdMismatch
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler.OnShardMapperErrorGlobal(result, _shardMap,
        _mapping == null ? null : _mapping.getStoreShard(), _errorCategory, this.getOperationName(),
        StoreOperationRequestBuilder.SP_LOCK_OR_UN_LOCK_SHARD_MAPPINGS_GLOBAL);
  }

  /**
   * Error category for store exception.
   */
  @Override
  protected ShardManagementErrorCategory getErrorCategory() {
    return ShardManagementErrorCategory.ShardMap;
  }

  /**
   * Performs undo of the storage operation that is pending.
   *
   * @param logEntry Log entry for the pending operation.
   */
  @Override
  protected void UndoPendingStoreOperations(StoreLogEntry logEntry) throws Exception {
    try (IStoreOperation op = _shardMapManager.getStoreOperationFactory()
        .FromLogEntry(_shardMapManager, logEntry)) {
      op.Undo();
    }
  }

  @Override
  public void close() throws IOException {

  }
}