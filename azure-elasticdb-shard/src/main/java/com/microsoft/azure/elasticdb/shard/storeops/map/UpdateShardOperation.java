package com.microsoft.azure.elasticdb.shard.storeops.map;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import java.util.UUID;

/**
 * Updates a shard in given shard map.
 */
public class UpdateShardOperation extends StoreOperation {
    /**
     * Shard map for which to update the shard.
     */
    private IStoreShardMap _shardMap;

    /**
     * Shard to update.
     */
    private IStoreShard _shardOld;

    /**
     * Updated shard.
     */
    private IStoreShard _shardNew;

    /**
     * Creates request to update shard in given shard map.
     *
     * @param shardMapManager Shard map manager object.
     * @param shardMap        Shard map for which to remove shard.
     * @param shardOld        Shard to update.
     * @param shardNew        Updated shard.
     */
    protected UpdateShardOperation(ShardMapManager shardMapManager, IStoreShardMap shardMap, IStoreShard shardOld, IStoreShard shardNew) {
        this(shardMapManager, UUID.NewGuid(), StoreOperationState.UndoBegin, shardMap, shardOld, shardNew);
    }

    /**
     * Creates request to update shard in given shard map.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationId     Operation id.
     * @param undoStartState  State from which Undo operation starts.
     * @param shardMap        Shard map for which to remove shard.
     * @param shardOld        Shard to update.
     * @param shardNew        Updated shard.
     */
    public UpdateShardOperation(ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState, IStoreShardMap shardMap, IStoreShard shardOld, IStoreShard shardNew) {
        super(shardMapManager, operationId, undoStartState, StoreOperationCode.UpdateShard, null, null);
        _shardMap = shardMap;
        _shardOld = shardOld;
        _shardNew = shardNew;
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
        tempVar.SourceLocation = this.UndoStartState <= StoreOperationState.UndoLocalSourceBeginTransaction ? _shardOld.Location : null;
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
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardsGlobalBegin, StoreOperationRequestBuilder.UpdateShardGlobal(this.Id, this.OperationCode, false, _shardMap, _shardOld, _shardNew)); // undo
    }

    /**
     * Handles errors from the initial GSM operation prior to LSM operations.
     *
     * @param result Operation result.
     */
    @Override
    public void HandleDoGlobalPreLocalExecuteError(IStoreResults result) {
        if (result.Result == StoreResult.ShardMapDoesNotExist) {
            // Remove shard map from cache.
            this.Manager.Cache.DeleteShardMap(_shardMap);
        }

        // Possible errors are:
        // StoreResult.ShardMapDoesNotExist
        // StoreResult.ShardDoesNotExist
        // StoreResult.ShardVersionMismatch
        // StoreResult.ShardHasMappings
        // StoreResult.StoreVersionMismatch
        // StoreResult.MissingParametersForStoredProcedure
        throw StoreOperationErrorHandler.OnShardMapErrorGlobal(result, _shardMap, _shardOld, ShardManagementErrorCategory.ShardMap, StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.OperationCode), StoreOperationRequestBuilder.SpBulkOperationShardsGlobalBegin);
    }

    /**
     * Performs the LSM operation on the source shard.
     *
     * @param ts Transaction scope.
     * @return Result of the operation.
     */
    @Override
    public IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts) {
        // Now actually update the shard.
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpUpdateShardLocal, StoreOperationRequestBuilder.UpdateShardLocal(this.Id, _shardMap, _shardNew));
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
        // Stored procedure can also return StoreResult.ShardDoesNotExist, but only for AttachShard operations
        throw StoreOperationErrorHandler.OnShardMapErrorLocal(result, _shardMap, _shardOld.Location, ShardManagementErrorCategory.ShardMap, StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.OperationCode), StoreOperationRequestBuilder.SpUpdateShardLocal);
    }

    /**
     * Performs the final GSM operation after the LSM operations.
     *
     * @param ts Transaction scope.
     * @return Pending operations on the target objects if any.
     */
    @Override
    public IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts) {
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardsGlobalEnd, StoreOperationRequestBuilder.UpdateShardGlobal(this.Id, this.OperationCode, false, _shardMap, _shardOld, _shardNew)); // undo
    }

    /**
     * Handles errors from the final GSM operation after the LSM operations.
     *
     * @param result Operation result.
     */
    @Override
    public void HandleDoGlobalPostLocalExecuteError(IStoreResults result) {
        if (result.Result == StoreResult.ShardMapDoesNotExist) {
            // Remove shard map from cache.
            this.Manager.Cache.DeleteShardMap(_shardMap);
        }

        // Possible errors are:
        // StoreResult.ShardMapDoesNotExist
        // StoreResult.StoreVersionMismatch
        // StoreResult.MissingParametersForStoredProcedure
        throw StoreOperationErrorHandler.OnShardMapErrorGlobal(result, _shardMap, _shardOld, ShardManagementErrorCategory.ShardMap, StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.OperationCode), StoreOperationRequestBuilder.SpBulkOperationShardsGlobalEnd);
    }

    /**
     * Performs the undo of LSM operation on the source shard.
     *
     * @param ts Transaction scope.
     * @return Result of the operation.
     */
    @Override
    public IStoreResults UndoLocalSourceExecute(IStoreTransactionScope ts) {
        // Adds back the removed shard entries.
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpUpdateShardLocal, StoreOperationRequestBuilder.UpdateShardLocal(this.Id, _shardMap, _shardOld));
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
        // Stored procedure can also return StoreResult.ShardDoesNotExist, but only for AttachShard operations
        throw StoreOperationErrorHandler.OnShardMapErrorLocal(result, _shardMap, _shardNew.Location, ShardManagementErrorCategory.ShardMap, StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.OperationCode), StoreOperationRequestBuilder.SpUpdateShardLocal);
    }

    /**
     * Performs the undo of GSM operation after LSM operations.
     *
     * @param ts Transaction scope.
     * @return Pending operations on the target objects if any.
     */
    @Override
    public IStoreResults UndoGlobalPostLocalExecute(IStoreTransactionScope ts) {
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardsGlobalEnd, StoreOperationRequestBuilder.UpdateShardGlobal(this.Id, this.OperationCode, true, _shardMap, _shardOld, _shardNew)); // undo
    }

    /**
     * Handles errors from the undo of GSM operation after LSM operations.
     *
     * @param result Operation result.
     */
    @Override
    public void HandleUndoGlobalPostLocalExecuteError(IStoreResults result) {
        if (result.Result == StoreResult.ShardMapDoesNotExist) {
            // Remove shard map from cache.
            this.Manager.Cache.DeleteShardMap(_shardMap);
        }

        // Possible errors are:
        // StoreResult.ShardMapDoesNotExist
        // StoreResult.StoreVersionMismatch
        // StoreResult.MissingParametersForStoredProcedure
        throw StoreOperationErrorHandler.OnShardMapErrorGlobal(result, _shardMap, _shardOld, ShardManagementErrorCategory.ShardMap, StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.OperationCode), StoreOperationRequestBuilder.SpBulkOperationShardsGlobalEnd);
    }

    /**
     * Source location of error.
     */
    @Override
    protected ShardLocation getErrorSourceLocation() {
        return _shardOld.Location;
    }

    /**
     * Target location of error.
     */
    @Override
    protected ShardLocation getErrorTargetLocation() {
        return _shardNew.Location;
    }

    /**
     * Error category for error.
     */
    @Override
    protected ShardManagementErrorCategory getErrorCategory() {
        return ShardManagementErrorCategory.ShardMap;
    }
}