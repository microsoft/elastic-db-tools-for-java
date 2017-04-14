package com.microsoft.azure.elasticdb.shard.storeops.mapper;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.cache.CacheStoreMappingUpdatePolicy;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.*;
import com.microsoft.azure.elasticdb.shard.storeops.base.*;

import java.util.UUID;

/**
 * Adds a mapping to given shard map.
 */
public class AddMappingOperation extends StoreOperation {
    /**
     * Shard map for which to add the mapping.
     */
    private StoreShardMap _shardMap;

    /**
     * Mapping to add.
     */
    private StoreMapping _mapping;

    /**
     * Error category to use.
     */
    private ShardManagementErrorCategory _errorCategory;

    /**
     * Creates request to add shard to given shard map.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationCode   Store operation code.
     * @param shardMap        Shard map for which to add mapping.
     * @param mapping         Mapping to add.
     */
    public AddMappingOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping mapping) {
        this(shardMapManager, UUID.randomUUID(), StoreOperationState.UndoBegin, operationCode, shardMap, mapping, null);
    }

    /**
     * Creates request to add shard to given shard map.
     *
     * @param shardMapManager          Shard map manager object.
     * @param operationId              Operation id.
     * @param undoStartState           State from which Undo operation starts.
     * @param operationCode            Store operation code.
     * @param shardMap                 Shard map for which to add mapping.
     * @param mapping                  Mapping to add.
     * @param originalShardVersionAdds Original shard version.
     */
    public AddMappingOperation(ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState, StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping mapping, UUID originalShardVersionAdds) {
        super(shardMapManager, operationId, undoStartState, operationCode, null, originalShardVersionAdds);
        _shardMap = shardMap;
        _mapping = mapping;
        _errorCategory = operationCode == StoreOperationCode.AddRangeMapping ? ShardManagementErrorCategory.RangeShardMap : ShardManagementErrorCategory.ListShardMap;
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
    public StoreResults DoGlobalPreLocalExecute(IStoreTransactionScope ts) {
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalBegin, StoreOperationRequestBuilder.AddShardMappingGlobal(this.getId(), this.getOperationCode(), false, _shardMap, _mapping)); // undo
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
        // StoreResult.ShardDoesNotExist
        // StoreResult.MappingPointAlreadyMapped
        // StoreResult.MappingRangeAlreadyMapped
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
    public StoreResults DoLocalSourceExecute(IStoreTransactionScope ts) {
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal, StoreOperationRequestBuilder.AddShardMappingLocal(this.getId(), false, _shardMap, _mapping));
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
        throw StoreOperationErrorHandler.OnShardMapperErrorLocal(result, _mapping.getStoreShard().getLocation(), StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()), StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal);
    }

    /**
     * Performs the final GSM operation after the LSM operations.
     *
     * @param ts Transaction scope.
     * @return Pending operations on the target objects if any.
     */
    @Override
    public StoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts) {
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalEnd, StoreOperationRequestBuilder.AddShardMappingGlobal(this.getId(), this.getOperationCode(), false, _shardMap, _mapping)); // undo
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
        throw StoreOperationErrorHandler.OnShardMapperErrorGlobal(result, _shardMap, _mapping.getStoreShard(), _errorCategory, StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()), StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalEnd);
    }

    /**
     * Refreshes the cache on successful commit of the final GSM operation after the LSM operations.
     *
     * @param result Operation result.
     */
    @Override
    public void DoGlobalPostLocalUpdateCache(StoreResults result) {
        // Add mapping to cache.
        this.getManager().getCache().AddOrUpdateMapping(_mapping, CacheStoreMappingUpdatePolicy.OverwriteExisting);
    }

    /**
     * Performs the undo of LSM operation on the source shard.
     *
     * @param ts Transaction scope.
     * @return Result of the operation.
     */
    @Override
    public StoreResults UndoLocalSourceExecute(IStoreTransactionScope ts) {
        StoreMapping dsm = new StoreMapping(_mapping.getId()
                , _shardMap.getId()
                , _mapping.getMinValue()
                , _mapping.getMaxValue()
                , _mapping.getStatus()
                , null
                , new StoreShard(_mapping.getStoreShard().getId(), this.getOriginalShardVersionAdds(), _shardMap.getId(), _mapping.getStoreShard().getLocation(), _mapping.getStoreShard().getStatus()));

        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal, StoreOperationRequestBuilder.RemoveShardMappingLocal(this.getId(), true, _shardMap, dsm));
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
        throw StoreOperationErrorHandler.OnShardMapperErrorLocal(result, _mapping.getStoreShard().getLocation(), StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()), StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal);
    }

    /**
     * Performs the undo of GSM operation after LSM operations.
     *
     * @param ts Transaction scope.
     * @return Pending operations on the target objects if any.
     */
    @Override
    public StoreResults UndoGlobalPostLocalExecute(IStoreTransactionScope ts) {
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalEnd, StoreOperationRequestBuilder.AddShardMappingGlobal(this.getId(), this.getOperationCode(), true, _shardMap, _mapping)); // undo
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