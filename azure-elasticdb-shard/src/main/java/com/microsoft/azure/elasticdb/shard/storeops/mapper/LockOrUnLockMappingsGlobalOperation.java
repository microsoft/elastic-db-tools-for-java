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
  private ShardMapManager shardMapManager;

  /**
   * Shard map to add.
   */
  private StoreShardMap shardMap;

  /**
   * Mapping to lock or unlock.
   */
  private StoreMapping mapping;

  /**
   * Lock owner id.
   */
  private UUID lockOwnerId;

  /**
   * Operation type.
   */
  private LockOwnerIdOpType lockOwnerIdOpType;

  /**
   * Error category to use.
   */
  private ShardManagementErrorCategory errorCategory;

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
    this.shardMapManager = shardMapManager;
    this.shardMap = shardMap;
    this.mapping = mapping;
    this.lockOwnerId = lockOwnerId;
    lockOwnerIdOpType = lockOpType;
    this.errorCategory = errorCategory;

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
  public StoreResults doGlobalExecute(IStoreTransactionScope ts) {
    return ts
        .executeOperation(StoreOperationRequestBuilder.SP_LOCK_OR_UN_LOCK_SHARD_MAPPINGS_GLOBAL,
            StoreOperationRequestBuilder.lockOrUnLockShardMappingsGlobal(shardMap, mapping,
                lockOwnerId, lockOwnerIdOpType));
  }

  /**
   * Handles errors from the GSM operation after the LSM operations.
   *
   * @param result Operation result.
   */
  @Override
  public void handleDoGlobalExecuteError(StoreResults result) {
    if (result.getResult() == StoreResult.ShardMapDoesNotExist) {
      // Remove shard map from cache.
      shardMapManager.getCache().deleteShardMap(shardMap);
    }

    if (result.getResult() == StoreResult.MappingDoesNotExist) {
      assert mapping != null;

      // Remove mapping from cache.
      shardMapManager.getCache().deleteMapping(mapping);
    }

    // Possible errors are:
    // StoreResult.ShardMapDoesNotExist
    // StoreResult.MappingDoesNotExist
    // StoreResult.MappingAlreadyLocked
    // StoreResult.MappingLockOwnerIdMismatch
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler.onShardMapperErrorGlobal(result, shardMap,
        mapping == null ? null : mapping.getStoreShard(), errorCategory, this.getOperationName(),
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
  protected void undoPendingStoreOperations(StoreLogEntry logEntry) throws Exception {
    try (IStoreOperation op = shardMapManager.getStoreOperationFactory()
        .fromLogEntry(shardMapManager, logEntry)) {
      op.undoOperation();
    }
  }

  @Override
  public void close() throws IOException {

  }
}