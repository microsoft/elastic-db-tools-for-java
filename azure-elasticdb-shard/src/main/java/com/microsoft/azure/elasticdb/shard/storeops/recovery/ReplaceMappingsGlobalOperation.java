package com.microsoft.azure.elasticdb.shard.storeops.recovery;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.map.ShardMapType;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.*;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationErrorHandler;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Replaces the GSM mappings for given shard map with the input mappings.
 */
public class ReplaceMappingsGlobalOperation extends StoreOperationGlobal {
    /**
     * Global shard map.
     */
    private StoreShardMap _shardMap;

    /**
     * Global shard.
     */
    private StoreShard _shard;

    /**
     * List of mappings to remove.
     */
    private List<IStoreMapping> _mappingsToRemove;

    /**
     * List of mappings to add.
     */
    private List<IStoreMapping> _mappingsToAdd;

    /**
     * Constructs request for replacing the GSM mappings for given shard map with the input mappings.
     *
     * @param shardMapManager  Shard map manager.
     * @param operationName    Operation name.
     * @param shardMap         GSM Shard map.
     * @param shard            GSM Shard.
     * @param mappingsToRemove Optional list of mappings to remove.
     * @param mappingsToAdd    List of mappings to add.
     */
    public ReplaceMappingsGlobalOperation(ShardMapManager shardMapManager, String operationName, StoreShardMap shardMap, StoreShard shard, List<IStoreMapping> mappingsToRemove, List<IStoreMapping> mappingsToAdd) {
        super(shardMapManager.getCredentials(), shardMapManager.getRetryPolicy(), operationName);
        _shardMap = shardMap;
        _shard = shard;
        _mappingsToRemove = mappingsToRemove;
        _mappingsToAdd = mappingsToAdd;
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
    public IStoreResults DoGlobalExecute(IStoreTransactionScope ts) {
        List<IStoreMapping> mappingsToReplace = this.GetMappingsToPurge(ts);

        return ts.ExecuteOperation(StoreOperationRequestBuilder.SpReplaceShardMappingsGlobal, StoreOperationRequestBuilder.ReplaceShardMappingsGlobalWithoutLogging(_shardMap, mappingsToReplace.toArray(new IStoreMapping[0]), _mappingsToAdd.toArray(new IStoreMapping[0])));
    }

    /**
     * Handles errors from the GSM operation after the LSM operations.
     *
     * @param result Operation result.
     */
    @Override
    public void HandleDoGlobalExecuteError(IStoreResults result) {
        // Possible errors are:
        // StoreResult.ShardMapDoesNotExist
        // StoreResult.StoreVersionMismatch
        // StoreResult.MissingParametersForStoredProcedure
        throw StoreOperationErrorHandler.OnRecoveryErrorGlobal(result, _shardMap, _shard, ShardManagementErrorCategory.Recovery, this.getOperationName(), StoreOperationRequestBuilder.SpReplaceShardMappingsGlobal);
    }

    /**
     * Error category for store exception.
     */
    @Override
    protected ShardManagementErrorCategory getErrorCategory() {
        return ShardManagementErrorCategory.Recovery;
    }

    /**
     * Finds all mappings to be purged based on the given input ranges.
     *
     * @param ts GSM transaction scope.
     * @return Mappings which are to be removed.
     */
    private List<IStoreMapping> GetMappingsToPurge(IStoreTransactionScope ts) {
        // Find all the mappings in GSM belonging to the shard
        IStoreResults gsmMappingsByShard = ts.ExecuteOperation(StoreOperationRequestBuilder.SpGetAllShardMappingsGlobal, StoreOperationRequestBuilder.GetAllShardMappingsGlobal(_shardMap, _shard, null));

        if (gsmMappingsByShard.getResult() != StoreResult.Success) {
            // Possible errors are:
            // StoreResult.ShardMapDoesNotExist
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnRecoveryErrorGlobal(gsmMappingsByShard, _shardMap, _shard, ShardManagementErrorCategory.Recovery, this.getOperationName(), StoreOperationRequestBuilder.SpGetAllShardMappingsGlobal);
        }

        Map<ShardRange, IStoreMapping> intersectingMappings = new HashMap<ShardRange, IStoreMapping>();

        for (IStoreMapping gsmMappingByShard : gsmMappingsByShard.getStoreMappings()) {
            ShardKey min = ShardKey.FromRawValue(_shardMap.getKeyType(), gsmMappingByShard.getMinValue());

            ShardKey max = null;

            switch (_shardMap.getMapType()) {
                case Range:
                    max = ShardKey.FromRawValue(_shardMap.getKeyType(), gsmMappingByShard.getMaxValue());
                    break;
                default:
                    assert _shardMap.getMapType() == ShardMapType.List;
                    max = ShardKey.FromRawValue(_shardMap.getKeyType(), gsmMappingByShard.getMinValue()).GetNextKey();
                    break;
            }

            intersectingMappings.put(new ShardRange(min, max), gsmMappingByShard);
        }

        // We need to discover, also, the range of intersecting mappings, so we can transitively detect
        // inconsistencies with other shards.
        for (IStoreMapping lsmMapping : _mappingsToRemove) {
            ShardKey min = ShardKey.FromRawValue(_shardMap.getKeyType(), lsmMapping.getMinValue());

            IStoreResults gsmMappingsByRange;

            switch (_shardMap.getMapType()) {
                case Range:
                    gsmMappingsByRange = ts.ExecuteOperation(StoreOperationRequestBuilder.SpGetAllShardMappingsGlobal, StoreOperationRequestBuilder.GetAllShardMappingsGlobal(_shardMap, null, new ShardRange(min, ShardKey.FromRawValue(_shardMap.getKeyType(), lsmMapping.getMaxValue()))));
                    break;

                default:
                    assert _shardMap.getMapType() == ShardMapType.List;
                    gsmMappingsByRange = ts.ExecuteOperation(StoreOperationRequestBuilder.SpFindShardMappingByKeyGlobal, StoreOperationRequestBuilder.FindShardMappingByKeyGlobal(_shardMap, min));
                    break;
            }

            if (gsmMappingsByRange.getResult() != StoreResult.Success) {
                if (gsmMappingsByRange.getResult() != StoreResult.MappingNotFoundForKey) {
                    // Possible errors are:
                    // StoreResult.ShardMapDoesNotExist
                    // StoreResult.StoreVersionMismatch
                    // StoreResult.MissingParametersForStoredProcedure
                    throw StoreOperationErrorHandler.OnRecoveryErrorGlobal(gsmMappingsByRange, _shardMap, _shard, ShardManagementErrorCategory.Recovery, this.getOperationName(), _shardMap.getMapType() == ShardMapType.Range ? StoreOperationRequestBuilder.SpGetAllShardMappingsGlobal : StoreOperationRequestBuilder.SpFindShardMappingByKeyGlobal);
                } else {
                    // No intersections being found is fine. Skip to the next mapping.
                    assert _shardMap.getMapType() == ShardMapType.List;
                }
            } else {
                for (IStoreMapping gsmMappingByRange : gsmMappingsByRange.getStoreMappings()) {
                    ShardKey minGlobal = ShardKey.FromRawValue(_shardMap.getKeyType(), gsmMappingByRange.getMinValue());
                    ShardKey maxGlobal = null;

                    switch (_shardMap.getMapType()) {
                        case Range:
                            maxGlobal = ShardKey.FromRawValue(_shardMap.getKeyType(), gsmMappingByRange.getMaxValue());
                            break;
                        default:
                            assert _shardMap.getMapType() == ShardMapType.List;
                            maxGlobal = ShardKey.FromRawValue(_shardMap.getKeyType(), gsmMappingByRange.getMinValue()).GetNextKey();
                            break;
                    }

                    intersectingMappings.put(new ShardRange(minGlobal, maxGlobal), gsmMappingByRange);
                }
            }
        }
        return (List<IStoreMapping>) intersectingMappings.values();
    }

    @Override
    public void close() throws IOException {

    }
}
