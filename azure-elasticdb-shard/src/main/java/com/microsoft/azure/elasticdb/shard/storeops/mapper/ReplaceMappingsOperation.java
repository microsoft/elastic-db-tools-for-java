package com.microsoft.azure.elasticdb.shard.storeops.mapper;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.*;
import com.microsoft.azure.elasticdb.shard.storeops.base.*;
import org.apache.commons.lang3.tuple.Pair;

import java.util.UUID;

/**
 * Replaces existing mappings with new mappings in given shard map.
 */
public class ReplaceMappingsOperation extends StoreOperation {
    /**
     * Shard map for which to perform operation.
     */
    private StoreShardMap _shardMap;

    /**
     * Original mappings.
     */
    private Pair<StoreMapping, UUID>[] _mappingsSource;

    /**
     * New mappings.
     */
    private Pair<StoreMapping, UUID>[] _mappingsTarget;

    /**
     * Creates request to replace mappings within shard map.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationCode   Store operation code.
     * @param shardMap        Shard map for which to update mapping.
     * @param mappingsSource  Original mappings.
     * @param mappingsTarget  Target mappings mapping.
     */
    public ReplaceMappingsOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, StoreShardMap shardMap, Pair<StoreMapping, UUID>[] mappingsSource, Pair<StoreMapping, UUID>[] mappingsTarget) {
        this(shardMapManager, UUID.randomUUID(), StoreOperationState.UndoBegin, operationCode, shardMap, mappingsSource, mappingsTarget, null);
    }

    /**
     * Creates request to replace mappings within shard map.
     *
     * @param shardMapManager          Shard map manager object.
     * @param operationId              Operation id.
     * @param undoStartState           State from which Undo operation starts.
     * @param operationCode            Store operation code.
     * @param shardMap                 Shard map for which to update mapping.
     * @param mappingsSource           Original mappings.
     * @param mappingsTarget           Target mappings mapping.
     * @param originalShardVersionAdds Original shard version on source.
     */
    public ReplaceMappingsOperation(ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState, StoreOperationCode operationCode, StoreShardMap shardMap, Pair<StoreMapping, UUID>[] mappingsSource, Pair<StoreMapping, UUID>[] mappingsTarget, UUID originalShardVersionAdds) {
        super(shardMapManager, operationId, undoStartState, operationCode, originalShardVersionAdds, originalShardVersionAdds);
        _shardMap = shardMap;
        _mappingsSource = mappingsSource;
        _mappingsTarget = mappingsTarget;
    }

    /**
     * Requests the derived class to provide information regarding the connections
     * needed for the operation.
     *
     * @return Information about shards involved in the operation.
     */
    @Override
    public StoreConnectionInfo GetStoreConnectionInfo() {
        assert _mappingsSource.length > 0;
        StoreConnectionInfo tempVar = new StoreConnectionInfo();
        //TODO: tempVar.getSourceLocation() = this.getUndoStartState() <= StoreOperationState.UndoLocalSourceBeginTransaction ? _mappingsSource[0].Item1.getStoreShard().getLocation() : null;
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
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalBegin, StoreOperationRequestBuilder.ReplaceShardMappingsGlobal(this.getId(), this.getOperationCode(), false, _shardMap, _mappingsSource, _mappingsTarget)); // undo
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

        if (result.getResult() == StoreResult.MappingDoesNotExist) {
            //TODO:
            /*for (StoreMapping mappingSource : _mappingsSource.Select(m -> m.Item1)) {
                // Remove mapping from cache.
                this.getManager().getCache().DeleteMapping(mappingSource);
            }*/
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
        //TODO: throw StoreOperationErrorHandler.OnShardMapperErrorGlobal(result, _shardMap, _mappingsSource[0].Item1.getStoreShard(), ShardManagementErrorCategory.RangeShardMap, StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()), StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalBegin);
    }

    /**
     * Performs the LSM operation on the source shard.
     *
     * @param ts Transaction scope.
     * @return Result of the operation.
     */
    @Override
    public StoreResults DoLocalSourceExecute(IStoreTransactionScope ts) {
        return null;//TODO: return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal, StoreOperationRequestBuilder.ReplaceShardMappingsLocal(this.getId(), false, _shardMap, _mappingsSource.Select(m -> m.Item1).ToArray(), _mappingsTarget.Select(m -> m.Item1).ToArray()));
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
        //TODO: throw StoreOperationErrorHandler.OnShardMapperErrorLocal(result, _mappingsSource[0].Item1.getStoreShard().getLocation(), StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()), StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal);
    }

    /**
     * Performs the final GSM operation after the LSM operations.
     *
     * @param ts Transaction scope.
     * @return Pending operations on the target objects if any.
     */
    @Override
    public StoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts) {
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalEnd, StoreOperationRequestBuilder.ReplaceShardMappingsGlobal(this.getId(), this.getOperationCode(), false, _shardMap, _mappingsSource, _mappingsTarget)); // undo
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
        //TODO: throw StoreOperationErrorHandler.OnShardMapperErrorGlobal(result, _shardMap, _mappingsSource[0].Item1.getStoreShard(), ShardManagementErrorCategory.RangeShardMap, StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()), StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalEnd);
    }

    /**
     * Refreshes the cache on successful commit of the final GSM operation after the LSM operations.
     *
     * @param result Operation result.
     */
    @Override
    public void DoGlobalPostLocalUpdateCache(StoreResults result) {
        // Remove from cache.
        //TODO:
        /*for (Pair<StoreMapping, UUID> ssm : _mappingsSource) {
            this.getManager().getCache().DeleteMapping(ssm.Item1);
        }

        // Add to cache.
        for (Pair<StoreMapping, UUID> ssm : _mappingsTarget) {
            this.getManager().getCache().AddOrUpdateMapping(ssm.Item1, CacheStoreMappingUpdatePolicy.OverwriteExisting);
        }*/
    }

    /**
     * Performs the undo of LSM operation on the source shard.
     *
     * @param ts Transaction scope.
     * @return Result of the operation.
     */
    @Override
    public StoreResults UndoLocalSourceExecute(IStoreTransactionScope ts) {
        /*DefaultStoreShard dssOriginal = new DefaultStoreShard(_mappingsSource[0].Item1.getStoreShard().getId(), this.getOriginalShardVersionAdds(), _mappingsSource[0].Item1.ShardMapId, _mappingsSource[0].Item1.getStoreShard().getLocation(), _mappingsSource[0].Item1.getStoreShard().getStatus());

        StoreMapping dsmSource = new StoreMapping(_mappingsSource[0].Item1.getId(), _mappingsSource[0].Item1.ShardMapId, dssOriginal, _mappingsSource[0].Item1.getMinValue(), _mappingsSource[0].Item1.getMaxValue(), _mappingsSource[0].Item1.getStatus(), _mappingsSource[0].Item2);

        StoreMapping dsmTarget = new StoreMapping(_mappingsTarget[0].Item1.getId(), _mappingsTarget[0].Item1.ShardMapId, dssOriginal, _mappingsTarget[0].Item1.getMinValue(), _mappingsTarget[0].Item1.getMaxValue(), _mappingsTarget[0].Item1.getStatus(), _mappingsTarget[0].Item2);

        StoreMapping[] ms = new StoreMapping[]{dsmSource};
        StoreMapping[] mt = new StoreMapping[]{dsmTarget};

        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal, StoreOperationRequestBuilder.ReplaceShardMappingsLocal(this.getId(), true, _shardMap, mt.Concat(_mappingsTarget.Skip(1).Select(m -> m.Item1)).ToArray(), ms.Concat(_mappingsSource.Skip(1).Select(m -> m.Item1)).ToArray()));*/
        return null; //TODO
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
        //TODO: throw StoreOperationErrorHandler.OnShardMapperErrorLocal(result, _mappingsSource[0].Item1.getStoreShard().getLocation(), StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()), StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal);
    }

    /**
     * Performs the undo of GSM operation after LSM operations.
     *
     * @param ts Transaction scope.
     * @return Pending operations on the target objects if any.
     */
    @Override
    public StoreResults UndoGlobalPostLocalExecute(IStoreTransactionScope ts) {
        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalEnd, StoreOperationRequestBuilder.ReplaceShardMappingsGlobal(this.getId(), this.getOperationCode(), true, _shardMap, _mappingsSource, _mappingsTarget)); // undo
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
        //TODO: throw StoreOperationErrorHandler.OnShardMapperErrorGlobal(result, _shardMap, _mappingsSource[0].Item1.getStoreShard(), ShardManagementErrorCategory.RangeShardMap, StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()), StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalEnd);
    }

    /**
     * Source location of error.
     */
    @Override
    protected ShardLocation getErrorSourceLocation() {
        return null; //TODO: _mappingsSource[0].Item1.getStoreShard().getLocation();
    }

    /**
     * Target location of error.
     */
    @Override
    protected ShardLocation getErrorTargetLocation() {
        return null; //TODO: _mappingsSource[0].Item1.getStoreShard().getLocation();
    }

    /**
     * Error category for error.
     */
    @Override
    protected ShardManagementErrorCategory getErrorCategory() {
        return ShardManagementErrorCategory.RangeShardMap;
    }

    @Override
    public void close() throws Exception {

    }
}