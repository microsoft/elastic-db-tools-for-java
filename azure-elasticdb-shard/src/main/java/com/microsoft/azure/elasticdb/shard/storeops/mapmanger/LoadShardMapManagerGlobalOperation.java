package com.microsoft.azure.elasticdb.shard.storeops.mapmanger;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.cache.CacheStoreMappingUpdatePolicy;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.*;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationErrorHandler;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Gets all shard maps from GSM.
 */
public class LoadShardMapManagerGlobalOperation extends StoreOperationGlobal {
    /**
     * Shard map manager object.
     */
    private ShardMapManager _shardMapManager;

    private ArrayList<LoadResult> _loadResults;

    private StoreShardMap _ssmCurrent;

    /**
     * Constructs request to get all shard maps from GSM.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationName   Operation name, useful for diagnostics.
     */
    public LoadShardMapManagerGlobalOperation(ShardMapManager shardMapManager, String operationName) {
        super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), operationName);
        _shardMapManager = shardMapManager;
        _loadResults = new ArrayList<LoadResult>();
    }

    /**
     * Whether this is a read-only operation.
     */
    @Override
    public boolean getReadOnly() {
        return true;
    }

    /**
     * Execute the operation against GSM in the current transaction scope.
     *
     * @param ts Transaction scope.
     * @return Results of the operation.
     */
    @Override
    public IStoreResults DoGlobalExecute(IStoreTransactionScope ts) {
        _loadResults.clear();

        IStoreResults result = ts.ExecuteOperation(StoreOperationRequestBuilder.SpGetAllShardMapsGlobal, StoreOperationRequestBuilder.GetAllShardMapsGlobal());

        if (result.getResult() == StoreResult.Success) {
            for (StoreShardMap ssm : result.getStoreShardMaps()) {
                _ssmCurrent = ssm;

                result = ts.ExecuteOperation(StoreOperationRequestBuilder.SpGetAllShardMappingsGlobal, StoreOperationRequestBuilder.GetAllShardMappingsGlobal(ssm, null, null));

                if (result.getResult() == StoreResult.Success) {
                    LoadResult tempVar = new LoadResult();
                    tempVar.setShardMap(ssm);
                    tempVar.setMappings(result.getStoreMappings());
                    _loadResults.add(tempVar);
                } else {
                    if (result.getResult() != StoreResult.ShardMapDoesNotExist) {
                        // Ignore some possible failures for Load operation and skip failed
                        // shard map caching operations.
                        break;
                    }
                }
            }
        }

        return result;
    }

    /**
     * Handles errors from the GSM operation after the LSM operations.
     *
     * @param result Operation result.
     */
    @Override
    public void HandleDoGlobalExecuteError(IStoreResults result) {
        if (_ssmCurrent == null) {
            // Possible errors are:
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnShardMapManagerErrorGlobal(result, null, this.getOperationName(), StoreOperationRequestBuilder.SpGetAllShardMapsGlobal);
        } else {
            if (result.getResult() != StoreResult.ShardMapDoesNotExist) {
                // Possible errors are:
                // StoreResult.StoreVersionMismatch
                // StoreResult.MissingParametersForStoredProcedure
                throw StoreOperationErrorHandler.OnShardMapperErrorGlobal(result, _ssmCurrent, null, ShardManagementErrorCategory.ShardMapManager, this.getOperationName(), StoreOperationRequestBuilder.SpGetAllShardMappingsGlobal); // shard
            }
        }
    }

    /**
     * Refreshes the cache on successful commit of the GSM operation.
     *
     * @param result Operation result.
     */
    @Override
    public void DoGlobalUpdateCachePost(IStoreResults result) {
        assert result.getResult() == StoreResult.Success || result.getResult() == StoreResult.ShardMapDoesNotExist;

        // Add shard maps and mappings to cache.
        for (LoadResult loadResult : _loadResults) {
            _shardMapManager.getCache().AddOrUpdateShardMap(loadResult.getShardMap());

            for (StoreMapping sm : loadResult.getMappings()) {
                _shardMapManager.getCache().AddOrUpdateMapping(sm, CacheStoreMappingUpdatePolicy.OverwriteExisting);
            }
        }
    }

    /**
     * Error category for store exception.
     */
    @Override
    protected ShardManagementErrorCategory getErrorCategory() {
        return ShardManagementErrorCategory.ShardMapManager;
    }

    @Override
    public void close() throws IOException {

    }

    /**
     * Result of load operation.
     */
    private static class LoadResult {
        /**
         * Shard map from the store.
         */
        private StoreShardMap ShardMap;
        /**
         * Mappings corresponding to the shard map.
         */
        private List<StoreMapping> Mappings;

        public final StoreShardMap getShardMap() {
            return ShardMap;
        }

        public final void setShardMap(StoreShardMap value) {
            ShardMap = value;
        }

        public final List<StoreMapping> getMappings() {
            return Mappings;
        }

        public final void setMappings(List<StoreMapping> value) {
            Mappings = value;
        }
    }
}
