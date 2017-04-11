package com.microsoft.azure.elasticdb.shard.storeops.mapper;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.*;
import com.microsoft.azure.elasticdb.shard.storeops.base.*;

import java.util.UUID;

/**
 * Removes a mapping from given shard map.
 */
public class RemoveMappingOperation extends StoreOperation {
    /**
     * Shard map from which to remove the mapping.
     */
    private IStoreShardMap _shardMap;

    /**
     * Mapping to remove.
     */
    private IStoreMapping _mapping;

    /**
     * Lock owner.
     */
    private UUID _lockOwnerId;

    /**
     * Error category to use.
     */
    private ShardManagementErrorCategory _errorCategory;

    /**
     * Creates request to add shard to given shard map.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationCode   Store operation code.
     * @param shardMap        Shard map from which to remove mapping.
     * @param mapping         Mapping to add.
     * @param lockOwnerId     Id of lock owner.
     */
    protected RemoveMappingOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, IStoreShardMap shardMap, IStoreMapping mapping, UUID lockOwnerId) {
        this(shardMapManager, UUID.randomUUID(), StoreOperationState.UndoBegin, operationCode, shardMap, mapping, lockOwnerId, null);
    }

    /**
     * Creates request to add shard to given shard map.
     *
     * @param shardMapManager             Shard map manager object.
     * @param operationId                 Operation id.
     * @param undoStartState              State from which Undo operation starts.
     * @param operationCode               Store operation code.
     * @param shardMap                    Shard map from which to remove mapping.
     * @param mapping                     Mapping to add.
     * @param lockOwnerId                 Id of lock owner.
     * @param originalShardVersionRemoves Original shard version.
     */
    public RemoveMappingOperation(ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState, StoreOperationCode operationCode, IStoreShardMap shardMap, IStoreMapping mapping, UUID lockOwnerId, UUID originalShardVersionRemoves) {
        super(shardMapManager, operationId, undoStartState, operationCode, originalShardVersionRemoves, null);
        _shardMap = shardMap;
        _mapping = mapping;
        _lockOwnerId = lockOwnerId;
        _errorCategory = operationCode == StoreOperationCode.RemoveRangeMapping ? ShardManagementErrorCategory.RangeShardMap : ShardManagementErrorCategory.ListShardMap;
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
        //TODO: tempVar.getSourceLocation() = this.getUndoStartState() <= StoreOperationState.UndoLocalSourceBeginTransaction ? _mapping.getStoreShard().getLocation() : null;
        return tempVar;
    }

    /**
     * Performs the initial GSM operation prior to LSM operations.
     *
     * @param ts Transaction scope.
     * @return Pending operations on the target objects if any.
     */
    @Override
    public IStoreResults DoGlobalPreLocalExecute(IStoreTransactionScope ts) {
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalBegin, StoreOperationRequestBuilder.RemoveShardMappingGlobal(this.getId(), this.getOperationCode(), false, _shardMap, _mapping, _lockOwnerId)); // undo
    }

    /**
     * Handles errors from the initial GSM operation prior to LSM operations.
     *
     * @param result Operation result.
     */
    @Override
    public void HandleDoGlobalPreLocalExecuteError(IStoreResults result) {
        if (result.getResult() == StoreResult.ShardMapDoesNotExist) {
            // Remove shard map from cache.
            this.getManager().getCache().DeleteShardMap(_shardMap);
        }

        if (result.getResult() == StoreResult.MappingDoesNotExist) {
            // Remove mapping from cache.
            this.getManager().getCache().DeleteMapping(_mapping);
        }

        // Possible errors are:
        // StoreResult.ShardMapDoesNotExist
        // StoreResult.ShardDoesNotExist
        // DefaultStoreShard.MappingDoesNotExist
        // StoreResult.MappingLockOwnerIdDoesNotMatch
        // StoreResult.MappingIsNotOffline
        // StoreResult.StoreVersionMismatch
        // StoreResult.MissingParametersForStoredProcedure
        throw StoreOperationErrorHandler.OnShardMapperErrorGlobal(result, _shardMap, _mapping.getStoreShard(), _errorCategory, StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()), StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalBegin);
    }

    /**
     * Performs the LSM operation on the source shard.
     *
     * @param ts Transaction scope.
     * @return Result of the operation.
     */
    @Override
    public IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts) {
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal, StoreOperationRequestBuilder.RemoveShardMappingLocal(this.getId(), false, _shardMap, _mapping));
    }

    /**
     * Handles errors from the the LSM operation on the source shard.
     *
     * @param result Operation result.
     */
    @Override
    public void HandleDoLocalSourceExecuteError(IStoreResults result) {
        // Possible errors are:
        // StoreResult.StoreVersionMismatch
        // StoreResult.MissingParametersForStoredProcedure
        throw StoreOperationErrorHandler.OnShardMapperErrorLocal(result, _mapping.getStoreShard().getLocation(), StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()), StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal);
    }

    /**
     * Performs the final GSM operation after the LSM operations.
     *
     * @param ts Transaction scope.
     * @return Pending operations on the target objects if any.
     */
    @Override
    public IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts) {
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalEnd, StoreOperationRequestBuilder.RemoveShardMappingGlobal(this.getId(), this.getOperationCode(), false, _shardMap, _mapping, _lockOwnerId)); // undo
    }

    /**
     * Handles errors from the final GSM operation after the LSM operations.
     *
     * @param result Operation result.
     */
    @Override
    public void HandleDoGlobalPostLocalExecuteError(IStoreResults result) {
        if (result.getResult() == StoreResult.ShardMapDoesNotExist) {
            // Remove shard map from cache.
            this.getManager().getCache().DeleteShardMap(_shardMap);
        }

        // Possible errors are:
        // StoreResult.ShardMapDoesNotExist
        // StoreResult.StoreVersionMismatch
        // StoreResult.MissingParametersForStoredProcedure
        throw StoreOperationErrorHandler.OnShardMapperErrorGlobal(result, _shardMap, _mapping.getStoreShard(), _errorCategory, StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()), StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalEnd);
    }

    /**
     * Refreshes the cache on successful commit of the final GSM operation after the LSM operations.
     *
     * @param result Operation result.
     */
    @Override
    public void DoGlobalPostLocalUpdateCache(IStoreResults result) {
        // Remove from cache.
        this.getManager().getCache().DeleteMapping(_mapping);
    }

    /**
     * Performs the undo of LSM operation on the source shard.
     *
     * @param ts Transaction scope.
     * @return Result of the operation.
     */
    @Override
    public IStoreResults UndoLocalSourceExecute(IStoreTransactionScope ts) {
        DefaultStoreMapping dsm = new DefaultStoreMapping(_mapping.getId(), _shardMap.getId(), new StoreShard(_mapping.getStoreShard().getId(), this.getOriginalShardVersionRemoves(), _shardMap.getId(), _mapping.getStoreShard().getLocation(), _mapping.getStoreShard().getStatus()), _mapping.getMinValue(), _mapping.getMaxValue(), _mapping.getStatus(), _lockOwnerId);

        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal, StoreOperationRequestBuilder.AddShardMappingLocal(this.getId(), true, _shardMap, dsm));
    }

    /**
     * Handles errors from the undo of LSM operation on the source shard.
     *
     * @param result Operation result.
     */
    @Override
    public void HandleUndoLocalSourceExecuteError(IStoreResults result) {
        // Possible errors are:
        // StoreResult.StoreVersionMismatch
        // StoreResult.MissingParametersForStoredProcedure
        throw StoreOperationErrorHandler.OnShardMapperErrorLocal(result, _mapping.getStoreShard().getLocation(), StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()), StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal);
    }

    /**
     * Performs the undo of GSM operation after LSM operations.
     *
     * @param ts Transaction scope.
     * @return Pending operations on the target objects if any.
     */
    @Override
    public IStoreResults UndoGlobalPostLocalExecute(IStoreTransactionScope ts) {
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalEnd, StoreOperationRequestBuilder.RemoveShardMappingGlobal(this.getId(), this.getOperationCode(), true, _shardMap, _mapping, _lockOwnerId)); // undo
    }

    /**
     * Handles errors from the undo of GSM operation after LSM operations.
     *
     * @param result Operation result.
     */
    @Override
    public void HandleUndoGlobalPostLocalExecuteError(IStoreResults result) {
        if (result.getResult() == StoreResult.ShardMapDoesNotExist) {
            // Remove shard map from cache.
            this.getManager().getCache().DeleteShardMap(_shardMap);
        }

        // Possible errors are:
        // StoreResult.ShardMapDoesNotExist
        // StoreResult.StoreVersionMismatch
        // StoreResult.MissingParametersForStoredProcedure
        throw StoreOperationErrorHandler.OnShardMapperErrorGlobal(result, _shardMap, _mapping.getStoreShard(), _errorCategory, StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()), StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalEnd);
    }

    /**
     * Source location of error.
     */
    @Override
    protected ShardLocation getErrorSourceLocation() {
        return _mapping.getStoreShard().getLocation();
    }

    /**
     * Target location of error.
     */
    @Override
    protected ShardLocation getErrorTargetLocation() {
        return _mapping.getStoreShard().getLocation();
    }

    /**
     * Error category for error.
     */
    @Override
    protected ShardManagementErrorCategory getErrorCategory() {
        return _errorCategory;
    }

    @Override
    public void close() throws Exception {

    }
}