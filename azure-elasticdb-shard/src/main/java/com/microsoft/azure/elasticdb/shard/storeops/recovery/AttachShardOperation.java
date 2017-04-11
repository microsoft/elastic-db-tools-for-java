package com.microsoft.azure.elasticdb.shard.storeops.recovery;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlResults;
import com.microsoft.azure.elasticdb.shard.store.*;
import com.microsoft.azure.elasticdb.shard.storeops.base.*;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.GlobalConstants;
import com.microsoft.azure.elasticdb.shard.utils.SqlUtils;

import java.util.UUID;

/**
 * Attaches the given shard map and shard information to the GSM database and update shard location in LSM.
 * UndoLocalSourceExecute is not implemented for this operation because UpdateShardLocal operation is needed
 * irrespective of the success/error condition, it updates Shards table with correct location of the Shard.
 */
public class AttachShardOperation extends StoreOperation {
    /**
     * Shard map to attach the shard.
     */
    private IStoreShardMap _shardMap;

    /**
     * Shard to attach.
     */
    private StoreShard _shard;

    /**
     * Creates request to attach shard to given shard map.
     *
     * @param shardMapManager Shard map manager object.
     * @param shardMap        Shard map for which to attach shard.
     * @param shard           Shard to attach.
     */
    protected AttachShardOperation(ShardMapManager shardMapManager, IStoreShardMap shardMap, StoreShard shard) {
        this(shardMapManager, UUID.randomUUID(), StoreOperationState.UndoBegin, shardMap, shard);
    }

    /**
     * Creates request to attach shard to given shard map.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationId     Operation id.
     * @param undoStartState  State from which Undo operation starts.
     * @param shardMap        Shard map for which to attach shard.
     * @param shard           Shard to attach.
     */
    public AttachShardOperation(ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState, IStoreShardMap shardMap, StoreShard shard) {
        super(shardMapManager, operationId, undoStartState, StoreOperationCode.AttachShard, null, null);
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
        //TODO: tempVar.getSourceLocation() = this.getUndoStartState() <= StoreOperationState.UndoLocalSourceBeginTransaction ? _shard.getLocation() : null;
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
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardsGlobalBegin, StoreOperationRequestBuilder.AddShardGlobal(this.getId(), this.getOperationCode(), false, _shardMap, _shard)); // undo
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

        // Possible errors are:
        // StoreResult.ShardMapDoesNotExist
        // StoreResult.ShardExists
        // StoreResult.ShardLocationExists
        // StoreResult.StoreVersionMismatch
        // StoreResult.MissingParametersForStoredProcedure
        throw StoreOperationErrorHandler.OnShardMapErrorGlobal(result, _shardMap, _shard, ShardManagementErrorCategory.Recovery, StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()), StoreOperationRequestBuilder.SpBulkOperationShardsGlobalBegin);
    }

    /**
     * Performs the LSM operation on the source shard.
     *
     * @param ts Transaction scope.
     * @return Result of the operation.
     */
    @Override
    public IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts) {
        // There should already be some version of LSM at this location as RecoveryMAnager.AttachShard() first reads existing shard maps from this location.

        IStoreResults checkResult = ts.ExecuteCommandSingle(SqlUtils.getCheckIfExistsLocalScript().get(0));
        assert checkResult.getStoreVersion() != null;

        // Upgrade local shard map to latest version before attaching.
        ts.ExecuteCommandBatch(SqlUtils.FilterUpgradeCommands(SqlUtils.getUpgradeLocalScript(), GlobalConstants.LsmVersionClient, checkResult.getStoreVersion().getVersion()));

        // Now update the shards table in LSM.
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpUpdateShardLocal, StoreOperationRequestBuilder.UpdateShardLocal(this.getId(), _shardMap, _shard));
    }

    /**
     * Handles errors from the the LSM operation on the source shard.
     *
     * @param result Operation result.
     */
    @Override
    public void HandleDoLocalSourceExecuteError(IStoreResults result) {
        // Possible errors from spUpdateShardLocal:
        // StoreResult.StoreVersionMismatch
        // StoreResult.MissingParametersForStoredProcedure
        // StoreResult.ShardDoesNotExist
        switch (result.getResult()) {
            case StoreVersionMismatch:
            case MissingParametersForStoredProcedure:
            case ShardDoesNotExist:
                throw StoreOperationErrorHandler.OnShardMapErrorLocal(result, _shardMap, _shard.getLocation(), ShardManagementErrorCategory.ShardMap, StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()), StoreOperationRequestBuilder.SpUpdateShardLocal);

            default:
                throw new ShardManagementException(ShardManagementErrorCategory.ShardMapManager, ShardManagementErrorCode.StorageOperationFailure, Errors._Store_SqlExceptionLocal, getOperationName());
        }
    }

    /**
     * Performs the final GSM operation after the LSM operations.
     *
     * @param ts Transaction scope.
     * @return Pending operations on the target objects if any.
     */
    @Override
    public IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts) {
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardsGlobalEnd, StoreOperationRequestBuilder.AddShardGlobal(this.getId(), this.getOperationCode(), false, _shardMap, _shard)); // undo
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
        throw StoreOperationErrorHandler.OnShardMapErrorGlobal(result, _shardMap, _shard, ShardManagementErrorCategory.ShardMap, StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()), StoreOperationRequestBuilder.SpBulkOperationShardsGlobalEnd);
    }

    /**
     * Performs the undo of LSM operation on the source shard.
     *
     * @param ts Transaction scope.
     * @return Result of the operation.
     */
    @Override
    public IStoreResults UndoLocalSourceExecute(IStoreTransactionScope ts) {
        // as part of local source execute step, we just update shard location to reflect correct
        // servername and database name, so there is no need to undo that action.
        return new SqlResults();
    }

    /**
     * Handles errors from the undo of LSM operation on the source shard.
     *
     * @param result Operation result.
     */
    @Override
    public void HandleUndoLocalSourceExecuteError(IStoreResults result) {
    }

    /**
     * Performs the undo of GSM operation after LSM operations.
     *
     * @param ts Transaction scope.
     * @return Pending operations on the target objects if any.
     */
    @Override
    public IStoreResults UndoGlobalPostLocalExecute(IStoreTransactionScope ts) {
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardsGlobalEnd, StoreOperationRequestBuilder.AddShardGlobal(this.getId(), this.getOperationCode(), true, _shardMap, _shard)); // undo
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
     * Error category for store exception.
     */
    @Override
    protected ShardManagementErrorCategory getErrorCategory() {
        return ShardManagementErrorCategory.Recovery;
    }

    @Override
    public void close() throws Exception {

    }
}