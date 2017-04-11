package com.microsoft.azure.elasticdb.shard.storeops.map;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.*;
import com.microsoft.azure.elasticdb.shard.storeops.base.*;
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
    private StoreShardMap _shardMap;

    /**
     * Shard to add.
     */
    private StoreShard _shard;

    /**
     * Creates request to add shard to given shard map.
     *
     * @param shardMapManager Shard map manager object.
     * @param shardMap        Shard map for which to add shard.
     * @param shard           Shard to add.
     */
    public AddShardOperation(ShardMapManager shardMapManager, StoreShardMap shardMap, StoreShard shard) {
        this(shardMapManager, UUID.randomUUID(), StoreOperationState.UndoBegin, shardMap, shard);
    }

    /**
     * Creates request to add shard to given shard map.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationId     Operation id.
     * @param undoStartState  State from which Undo operation starts.
     * @param shardMap        Shard map for which to add shard.
     * @param shard           Shard to add.
     */
    public AddShardOperation(ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState, StoreShardMap shardMap, StoreShard shard) {
        super(shardMapManager, operationId, undoStartState, StoreOperationCode.AddShard, null, null);
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
        ShardLocation location = this.getUndoStartState().getValue() <= StoreOperationState.UndoLocalSourceBeginTransaction.getValue() ? _shard.getLocation() : null;
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
    public StoreResults DoGlobalPreLocalExecute(IStoreTransactionScope ts) {
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardsGlobalBegin, StoreOperationRequestBuilder.AddShardGlobal(this.getId(), this.getOperationCode(), false, _shardMap, _shard)); // undo
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
            this.getManager().getCache().DeleteShardMap(_shardMap);
        }

        // Possible errors are:
        // StoreResult.ShardMapDoesNotExist
        // StoreResult.ShardExists
        // StoreResult.ShardLocationExists
        // StoreResult.StoreVersionMismatch
        // StoreResult.MissingParametersForStoredProcedure
        throw StoreOperationErrorHandler.OnShardMapErrorGlobal(result, _shardMap, _shard, ShardManagementErrorCategory.ShardMap, StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()), StoreOperationRequestBuilder.SpBulkOperationShardsGlobalBegin);
    }

    /**
     * Performs the LSM operation on the source shard.
     *
     * @param ts Transaction scope.
     * @return Result of the operation.
     */
    @Override
    public StoreResults DoLocalSourceExecute(IStoreTransactionScope ts) {
        StoreResults checkResult = ts.ExecuteCommandSingle(SqlUtils.getCheckIfExistsLocalScript().get(0));

        // Shard not already deployed, just need to add the proper entries.
        if (checkResult.getStoreVersion() == null) {
            // create initial version of LSM
            ts.ExecuteCommandBatch(SqlUtils.getCreateLocalScript());

            // now upgrade LSM to latest version
            ts.ExecuteCommandBatch(SqlUtils.FilterUpgradeCommands(SqlUtils.getUpgradeLocalScript(), GlobalConstants.LsmVersionClient));
        }

        // Now actually add the shard entries.
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpAddShardLocal, StoreOperationRequestBuilder.AddShardLocal(this.getId(), false, _shardMap, _shard));
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
        throw StoreOperationErrorHandler.OnShardMapErrorLocal(result, _shardMap, _shard.getLocation(), ShardManagementErrorCategory.ShardMap, StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()), StoreOperationRequestBuilder.SpAddShardLocal);
    }

    /**
     * Performs the final GSM operation after the LSM operations.
     *
     * @param ts Transaction scope.
     * @return Pending operations on the target objects if any.
     */
    @Override
    public StoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts) {
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardsGlobalEnd, StoreOperationRequestBuilder.AddShardGlobal(this.getId(), this.getOperationCode(), false, _shardMap, _shard)); // undo
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
            this.getManager().getCache().DeleteShardMap(_shardMap);
        }

        // Possible errors are:
        // StoreResult.ShardMapDoesNotExist
        // StoreResult.StoreVersionMismatch
        // StoreResult.MissingParametersForStoredProcedure
        throw StoreOperationErrorHandler.OnShardMapErrorGlobal(result, _shardMap, _shard, ShardManagementErrorCategory.ShardMap, StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()), StoreOperationRequestBuilder.SpBulkOperationShardsGlobalEnd);
    }

    /**
     * Performs the undo of LSM operation on the source shard.
     *
     * @param ts Transaction scope.
     * @return Result of the operation.
     */
    @Override
    public StoreResults UndoLocalSourceExecute(IStoreTransactionScope ts) {
        StoreResults checkResult = ts.ExecuteCommandSingle(SqlUtils.getCheckIfExistsLocalScript().get(0));

        if (checkResult.getStoreVersion() != null) {
            // Remove the added shard entries.
            return ts.ExecuteOperation(StoreOperationRequestBuilder.SpRemoveShardLocal, StoreOperationRequestBuilder.RemoveShardLocal(this.getId(), _shardMap, _shard));
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
    public void HandleUndoLocalSourceExecuteError(StoreResults result) {
        // Possible errors are:
        // StoreResult.StoreVersionMismatch
        // StoreResult.MissingParametersForStoredProcedure
        throw StoreOperationErrorHandler.OnShardMapErrorLocal(result, _shardMap, _shard.getLocation(), ShardManagementErrorCategory.ShardMap, StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()), StoreOperationRequestBuilder.SpAddShardLocal);
    }

    /**
     * Performs the undo of GSM operation after LSM operations.
     *
     * @param ts Transaction scope.
     * @return Pending operations on the target objects if any.
     */
    @Override
    public StoreResults UndoGlobalPostLocalExecute(IStoreTransactionScope ts) {
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardsGlobalEnd, StoreOperationRequestBuilder.AddShardGlobal(this.getId(), this.getOperationCode(), true, _shardMap, _shard)); // undo
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
            this.getManager().getCache().DeleteShardMap(_shardMap);
        }

        // Possible errors are:
        // StoreResult.ShardMapDoesNotExist
        // StoreResult.StoreVersionMismatch
        // StoreResult.MissingParametersForStoredProcedure
        throw StoreOperationErrorHandler.OnShardMapErrorGlobal(result, _shardMap, _shard, ShardManagementErrorCategory.ShardMap, StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()), StoreOperationRequestBuilder.SpBulkOperationShardsGlobalEnd);
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
