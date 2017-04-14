package com.microsoft.azure.elasticdb.shard.storeops.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.shard.base.LockOwnerIdOpType;
import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.cache.CacheStoreMappingUpdatePolicy;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerCreateMode;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlShardMapManagerCredentials;
import com.microsoft.azure.elasticdb.shard.store.*;
import com.microsoft.azure.elasticdb.shard.storeops.map.*;
import com.microsoft.azure.elasticdb.shard.storeops.mapmanagerfactory.CreateShardMapManagerGlobalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.mapmanagerfactory.GetShardMapManagerGlobalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.mapmanger.*;
import com.microsoft.azure.elasticdb.shard.storeops.mapper.*;
import com.microsoft.azure.elasticdb.shard.storeops.recovery.*;
import com.microsoft.azure.elasticdb.shard.storeops.schemainformation.*;
import com.microsoft.azure.elasticdb.shard.storeops.upgrade.UpgradeStoreGlobalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.upgrade.UpgradeStoreLocalOperation;
import com.microsoft.azure.elasticdb.shard.utils.XElement;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.UUID;

/**
 * Instantiates the operations that need to be undone corresponding to a request.
 */
public class StoreOperationFactory implements IStoreOperationFactory {
    /**
     * Create instance of StoreOperationFactory.
     */
    public StoreOperationFactory() {
    }

    ///#region Global Operations

    /**
     * Constructs request for deploying SMM storage objects to target GSM database.
     *
     * @param credentials   Credentials for connection.
     * @param retryPolicy   Retry policy.
     * @param operationName Operation name, useful for diagnostics.
     * @param createMode    Creation mode.
     * @param targetVersion target version of store to deploy, this will be used mainly for upgrade testing purposes.
     * @return The store operation.
     */
    public IStoreOperationGlobal CreateCreateShardMapManagerGlobalOperation(SqlShardMapManagerCredentials credentials, RetryPolicy retryPolicy, String operationName, ShardMapManagerCreateMode createMode, Version targetVersion) {
        return new CreateShardMapManagerGlobalOperation(credentials, retryPolicy, operationName, createMode, targetVersion);
    }

    /**
     * Constructs request for obtaining shard map manager object if the GSM has the SMM objects in it.
     *
     * @param credentials    Credentials for connection.
     * @param retryPolicy    Retry policy.
     * @param operationName  Operation name, useful for diagnostics.
     * @param throwOnFailure Whether to throw exception on failure or return error code.
     * @return The store operation.
     */
    public IStoreOperationGlobal CreateGetShardMapManagerGlobalOperation(SqlShardMapManagerCredentials credentials, RetryPolicy retryPolicy, String operationName, boolean throwOnFailure) {
        return new GetShardMapManagerGlobalOperation(credentials, retryPolicy, operationName, throwOnFailure);
    }

    /**
     * Constructs request for Detaching the given shard and mapping information to the GSM database.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationName   Operation name.
     * @param location        Location to be detached.
     * @param shardMapName    Shard map from which shard is being detached.
     * @return The store operation.
     */
    public IStoreOperationGlobal CreateDetachShardGlobalOperation(ShardMapManager shardMapManager, String operationName, ShardLocation location, String shardMapName) {
        return new DetachShardGlobalOperation(shardMapManager, operationName, location, shardMapName);
    }

    /**
     * Constructs request for replacing the GSM mappings for given shard map with the input mappings.
     *
     * @param shardMapManager  Shard map manager.
     * @param operationName    Operation name.
     * @param shardMap         GSM Shard map.
     * @param shard            GSM Shard.
     * @param mappingsToRemove Optional list of mappings to remove.
     * @param mappingsToAdd    List of mappings to add.
     * @return The store operation.
     */
    public IStoreOperationGlobal CreateReplaceMappingsGlobalOperation(ShardMapManager shardMapManager, String operationName, StoreShardMap shardMap, StoreShard shard, List<StoreMapping> mappingsToRemove, List<StoreMapping> mappingsToAdd) {
        return new ReplaceMappingsGlobalOperation(shardMapManager, operationName, shardMap, shard, mappingsToRemove, mappingsToAdd);
    }

    /**
     * Constructs a request to add schema info to GSM.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationName   Operation name, useful for diagnostics.
     * @param schemaInfo      Schema info to add.
     * @return The store operation.
     */
    public IStoreOperationGlobal CreateAddShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, String operationName, StoreSchemaInfo schemaInfo) {
        return new AddShardingSchemaInfoGlobalOperation(shardMapManager, operationName, schemaInfo);
    }

    /**
     * Constructs a request to find schema info in GSM.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationName   Operation name, useful for diagnostics.
     * @param schemaInfoName  Name of schema info to search.
     * @return The store operation.
     */
    public IStoreOperationGlobal CreateFindShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, String operationName, String schemaInfoName) {
        return new FindShardingSchemaInfoGlobalOperation(shardMapManager, operationName, schemaInfoName);
    }

    /**
     * Constructs a request to get all schema info objects from GSM.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationName   Operation name, useful for diagnostics.
     * @return The store operation.
     */
    public IStoreOperationGlobal CreateGetShardingSchemaInfosGlobalOperation(ShardMapManager shardMapManager, String operationName) {
        return new GetShardingSchemaInfosGlobalOperation(shardMapManager, operationName);
    }

    /**
     * Constructs a request to delete schema info from GSM.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationName   Operation name, useful for diagnostics.
     * @param schemaInfoName  Name of schema info to delete.
     * @return The store operation.
     */
    public IStoreOperationGlobal CreateRemoveShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, String operationName, String schemaInfoName) {
        return new RemoveShardingSchemaInfoGlobalOperation(shardMapManager, operationName, schemaInfoName);
    }

    /**
     * Constructs a request to update schema info to GSM.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationName   Operation name, useful for diagnostics.
     * @param schemaInfo      Schema info to update.
     * @return The store operation.
     */
    public IStoreOperationGlobal CreateUpdateShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, String operationName, StoreSchemaInfo schemaInfo) {
        return new UpdateShardingSchemaInfoGlobalOperation(shardMapManager, operationName, schemaInfo);
    }

    /**
     * Constructs request to get shard with specific location for given shard map from GSM.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationName   Operation name, useful for diagnostics.
     * @param shardMap        Shard map for which shard is being requested.
     * @param location        Location of shard being searched.
     * @return The store operation.
     */
    public IStoreOperationGlobal CreateFindShardByLocationGlobalOperation(ShardMapManager shardMapManager, String operationName, StoreShardMap shardMap, ShardLocation location) {
        return new FindShardByLocationGlobalOperation(shardMapManager, operationName, shardMap, location);
    }

    /**
     * Constructs request to get all shards for given shard map from GSM.
     *
     * @param operationName   Operation name, useful for diagnostics.
     * @param shardMapManager Shard map manager object.
     * @param shardMap        Shard map for which shards are being requested.
     * @return The store operation.
     */
    public IStoreOperationGlobal CreateGetShardsGlobalOperation(String operationName, ShardMapManager shardMapManager, StoreShardMap shardMap) {
        return new GetShardsGlobalOperation(operationName, shardMapManager, shardMap);
    }

    /**
     * Constructs request to add given shard map to GSM.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationName   Operation name, useful for diagnostics.
     * @param shardMap        Shard map to add.
     * @return The store operation.
     */
    public IStoreOperationGlobal CreateAddShardMapGlobalOperation(ShardMapManager shardMapManager, String operationName, StoreShardMap shardMap) {
        return new AddShardMapGlobalOperation(shardMapManager, operationName, shardMap);
    }

    /**
     * Constructs request to find shard map with given name from GSM.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationName   Operation name, useful for diagnostics.
     * @param shardMapName    Name of the shard map being searched.
     * @return The store operation.
     */
    public IStoreOperationGlobal CreateFindShardMapByNameGlobalOperation(ShardMapManager shardMapManager, String operationName, String shardMapName) {
        return new FindShardMapByNameGlobalOperation(shardMapManager, operationName, shardMapName);
    }

    /**
     * Constructs request to get distinct shard locations from GSM.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationName   Operation name, useful for diagnostics.
     * @return The store operation.
     */
    public IStoreOperationGlobal CreateGetDistinctShardLocationsGlobalOperation(ShardMapManager shardMapManager, String operationName) {
        return new GetDistinctShardLocationsGlobalOperation(shardMapManager, operationName);
    }

    /**
     * Constructs request to get all shard maps from GSM.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationName   Operation name, useful for diagnostics.
     * @return The store operation.
     */
    public IStoreOperationGlobal CreateGetShardMapsGlobalOperation(ShardMapManager shardMapManager, String operationName) {
        return new GetShardMapsGlobalOperation(shardMapManager, operationName);
    }

    /**
     * Constructs request to get all shard maps from GSM.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationName   Operation name, useful for diagnostics.
     * @return The store operation.
     */
    public IStoreOperationGlobal CreateLoadShardMapManagerGlobalOperation(ShardMapManager shardMapManager, String operationName) {
        return new LoadShardMapManagerGlobalOperation(shardMapManager, operationName);
    }

    /**
     * Constructs request to remove given shard map from GSM.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationName   Operation name, useful for diagnostics.
     * @param shardMap        Shard map to remove.
     * @return The store operation.
     */
    public IStoreOperationGlobal CreateRemoveShardMapGlobalOperation(ShardMapManager shardMapManager, String operationName, StoreShardMap shardMap) {
        return new RemoveShardMapGlobalOperation(shardMapManager, operationName, shardMap);
    }

    /**
     * Constructs request for obtaining mapping from GSM based on given key.
     *
     * @param shardMapManager Shard map manager.
     * @param operationName   Operation being executed.
     * @param shardMap        Local shard map.
     * @param mapping         Mapping whose Id will be used.
     * @param errorCategory   Error category.
     * @return The store operation.
     */
    public IStoreOperationGlobal CreateFindMappingByIdGlobalOperation(ShardMapManager shardMapManager, String operationName, StoreShardMap shardMap, StoreMapping mapping, ShardManagementErrorCategory errorCategory) {
        return new FindMappingByIdGlobalOperation(shardMapManager, operationName, shardMap, mapping, errorCategory);
    }

    /**
     * Constructs request for obtaining mapping from GSM based on given key.
     *
     * @param shardMapManager Shard map manager.
     * @param operationName   Operation being executed.
     * @param shardMap        Local shard map.
     * @param key             Key for lookup operation.
     * @param policy          Policy for cache update.
     * @param errorCategory   Error category.
     * @param cacheResults    Whether to cache the results of the operation.
     * @param ignoreFailure   Ignore shard map not found error.
     * @return The store operation.
     */
    public IStoreOperationGlobal CreateFindMappingByKeyGlobalOperation(ShardMapManager shardMapManager, String operationName, StoreShardMap shardMap, ShardKey key, CacheStoreMappingUpdatePolicy policy, ShardManagementErrorCategory errorCategory, boolean cacheResults, boolean ignoreFailure) {
        return new FindMappingByKeyGlobalOperation(shardMapManager, operationName, shardMap, key, policy, errorCategory, cacheResults, ignoreFailure);
    }

    /**
     * Constructs request for obtaining all the mappings from GSM based on given shard and mappings.
     *
     * @param shardMapManager Shard map manager.
     * @param operationName   Operation being executed.
     * @param shardMap        Local shard map.
     * @param shard           Local shard.
     * @param range           Optional range to get mappings from.
     * @param errorCategory   Error category.
     * @param cacheResults    Whether to cache the results of the operation.
     * @param ignoreFailure   Ignore shard map not found error.
     * @return The store operation.
     */
    public IStoreOperationGlobal CreateGetMappingsByRangeGlobalOperation(ShardMapManager shardMapManager, String operationName, StoreShardMap shardMap, StoreShard shard, ShardRange range, ShardManagementErrorCategory errorCategory, boolean cacheResults, boolean ignoreFailure) {
        return new GetMappingsByRangeGlobalOperation(shardMapManager, operationName, shardMap, shard, range, errorCategory, cacheResults, ignoreFailure);
    }

    /**
     * Constructs request to lock or unlock given mappings in GSM.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationName   Operation name, useful for diagnostics.
     * @param shardMap        Shard map to add.
     * @param mapping         Mapping to lock or unlock. Null means all mappings.
     * @param lockOwnerId     Lock owner.
     * @param lockOpType      Lock operation type.
     * @param errorCategory   Error category.
     * @return The store operation.
     */
    public IStoreOperationGlobal CreateLockOrUnLockMappingsGlobalOperation(ShardMapManager shardMapManager, String operationName, StoreShardMap shardMap, StoreMapping mapping, UUID lockOwnerId, LockOwnerIdOpType lockOpType, ShardManagementErrorCategory errorCategory) {
        return new LockOrUnLockMappingsGlobalOperation(shardMapManager, operationName, shardMap, mapping, lockOwnerId, lockOpType, errorCategory);
    }

    /**
     * Constructs a request to upgrade global store.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationName   Operation name, useful for diagnostics.
     * @param targetVersion   Target version of store to deploy.
     * @return The store operation.
     */
    public IStoreOperationGlobal CreateUpgradeStoreGlobalOperation(ShardMapManager shardMapManager, String operationName, Version targetVersion) {
        return new UpgradeStoreGlobalOperation(shardMapManager, operationName, targetVersion);
    }

    ///#endregion Global Operations

    ///#region Local Operations

    /**
     * Constructs request for obtaining all the shard maps and shards from an LSM.
     *
     * @param operationName   Operation name.
     * @param shardMapManager Shard map manager.
     * @param location        Location of the LSM.
     */
    public IStoreOperationLocal CreateCheckShardLocalOperation(String operationName, ShardMapManager shardMapManager, ShardLocation location) {
        return new CheckShardLocalOperation(operationName, shardMapManager, location);
    }

    /**
     * Constructs request for obtaining all the shard maps and shards from an LSM.
     *
     * @param shardMapManager Shard map manager.
     * @param location        Location of the LSM.
     * @param operationName   Operation name.
     * @param shardMap        Local shard map.
     * @param shard           Local shard.
     * @param range           Optional range to get mappings from.
     * @param ignoreFailure   Ignore shard map not found error.
     */
    public IStoreOperationLocal CreateGetMappingsByRangeLocalOperation(ShardMapManager shardMapManager, ShardLocation location, String operationName, StoreShardMap shardMap, StoreShard shard, ShardRange range, boolean ignoreFailure) {
        return new GetMappingsByRangeLocalOperation(shardMapManager, location, operationName, shardMap, shard, range, ignoreFailure);
    }

    /**
     * Constructs request for obtaining all the shard maps and shards from an LSM.
     *
     * @param shardMapManager Shard map manager.
     * @param location        Location of the LSM.
     * @param operationName   Operatio name.
     */
    public IStoreOperationLocal CreateGetShardsLocalOperation(ShardMapManager shardMapManager, ShardLocation location, String operationName) {
        return new GetShardsLocalOperation(shardMapManager, location, operationName);
    }

    /**
     * Constructs request for replacing the LSM mappings for given shard map with the input mappings.
     *
     * @param shardMapManager Shard map manager.
     * @param location        Location of the LSM.
     * @param operationName   Operation name.
     * @param shardMap        Local shard map.
     * @param shard           Local shard.
     * @param rangesToRemove  Optional list of ranges to minimize amount of deletions.
     * @param mappingsToAdd   List of mappings to add.
     */
    public IStoreOperationLocal CreateReplaceMappingsLocalOperation(ShardMapManager shardMapManager, ShardLocation location, String operationName, StoreShardMap shardMap, StoreShard shard, List<ShardRange> rangesToRemove, List<StoreMapping> mappingsToAdd) {
        return new ReplaceMappingsLocalOperation(shardMapManager, location, operationName, shardMap, shard, rangesToRemove, mappingsToAdd);
    }

    /**
     * Constructs a request to upgrade store location.
     *
     * @param shardMapManager Shard map manager object.
     * @param location        Location to upgrade.
     * @param operationName   Operation name, useful for diagnostics.
     * @param targetVersion   Target version of store to deploy.
     * @return The store operation.
     */
    public IStoreOperationLocal CreateUpgradeStoreLocalOperation(ShardMapManager shardMapManager, ShardLocation location, String operationName, Version targetVersion) {
        return new UpgradeStoreLocalOperation(shardMapManager, location, operationName, targetVersion);
    }

    ///#endregion LocalOperations

    ///#region Global And Local Operations

    ///#region Do Operations

    /**
     * Creates request to add shard to given shard map.
     *
     * @param shardMapManager Shard map manager object.
     * @param shardMap        Shard map for which to add shard.
     * @param shard           Shard to add.
     * @return The store operation.
     */
    public IStoreOperation CreateAddShardOperation(ShardMapManager shardMapManager, StoreShardMap shardMap, StoreShard shard) {
        return new AddShardOperation(shardMapManager, shardMap, shard);
    }

    /**
     * Creates request to remove shard from given shard map.
     *
     * @param shardMapManager Shard map manager object.
     * @param shardMap        Shard map for which to remove shard.
     * @param shard           Shard to remove.
     * @return The store operation.
     */
    public IStoreOperation CreateRemoveShardOperation(ShardMapManager shardMapManager, StoreShardMap shardMap, StoreShard shard) {
        return new RemoveShardOperation(shardMapManager, shardMap, shard);
    }

    /**
     * Creates request to update shard in given shard map.
     *
     * @param shardMapManager Shard map manager object.
     * @param shardMap        Shard map for which to remove shard.
     * @param shardOld        Shard to update.
     * @param shardNew        Updated shard.
     * @return The store operation.
     */
    public IStoreOperation CreateUpdateShardOperation(ShardMapManager shardMapManager, StoreShardMap shardMap, StoreShard shardOld, StoreShard shardNew) {
        return new UpdateShardOperation(shardMapManager, shardMap, shardOld, shardNew);
    }

    /**
     * Constructs request for attaching the given shard map and shard information to the GSM database.
     *
     * @param shardMapManager Shard map manager object.
     * @param shard           Shard to attach
     * @param shardMap        Shard map to attach specified shard
     * @return The store operation.
     */
    public IStoreOperation CreateAttachShardOperation(ShardMapManager shardMapManager, StoreShardMap shardMap, StoreShard shard) {
        return new AttachShardOperation(shardMapManager, shardMap, shard);
    }

    /**
     * Creates request to add shard to given shard map.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationCode   Store operation code.
     * @param shardMap        Shard map for which to add mapping.
     * @param mapping         Mapping to add.
     * @return The store operation.
     */
    public IStoreOperation CreateAddMappingOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping mapping) {
        return new AddMappingOperation(shardMapManager, operationCode, shardMap, mapping);
    }

    /**
     * Creates request to add shard to given shard map.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationCode   Store operation code.
     * @param shardMap        Shard map from which to remove mapping.
     * @param mapping         Mapping to add.
     * @param lockOwnerId     Id of lock owner.
     * @return The store operation.
     */
    public IStoreOperation CreateRemoveMappingOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping mapping, UUID lockOwnerId) {
        return new RemoveMappingOperation(shardMapManager, operationCode, shardMap, mapping, lockOwnerId);
    }

    /**
     * Creates request to add shard to given shard map.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationCode   Store operation code.
     * @param shardMap        Shard map for which to update mapping.
     * @param mappingSource   Mapping to update.
     * @param mappingTarget   Updated mapping.
     * @param patternForKill  Pattern for kill commands.
     * @param lockOwnerId     Id of lock owner.
     * @return The store operation.
     */
    public IStoreOperation CreateUpdateMappingOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping mappingSource, StoreMapping mappingTarget, String patternForKill, UUID lockOwnerId) {
        return new UpdateMappingOperation(shardMapManager, operationCode, shardMap, mappingSource, mappingTarget, patternForKill, lockOwnerId);
    }

    /**
     * Creates request to replace mappings within shard map.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationCode   Store operation code.
     * @param shardMap        Shard map for which to update mapping.
     * @param mappingsSource  Original mappings.
     * @param mappingsTarget  Target mappings mapping.
     * @return The store operation.
     */
    public IStoreOperation CreateReplaceMappingsOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, StoreShardMap shardMap, Pair<StoreMapping, UUID>[] mappingsSource, Pair<StoreMapping, UUID>[] mappingsTarget) {
        return new ReplaceMappingsOperation(shardMapManager, operationCode, shardMap, mappingsSource, mappingsTarget);
    }

    ///#endregion Do Operations

    ///#region Undo Operations

    /**
     * Create operation corresponding to the <see cref="StoreLogEntry"/> information.
     *
     * @param shardMapManager ShardMapManager instance for undo operation.
     * @param so              Store operation information.
     * @return The operation to be undone.
     */
    public IStoreOperation FromLogEntry(ShardMapManager shardMapManager, StoreLogEntry so) {
        XElement root;

        //TODO:
        /*try (XmlReader reader = so.Data.CreateReader()) {
            root = XElement.Load(reader);
        }

        switch (so.OpCode) {
            case StoreOperationCode.AddShard:
                return this.CreateAddShardOperation(shardMapManager, so.Id, so.UndoStartState, root);

            case StoreOperationCode.RemoveShard:
                return this.CreateRemoveShardOperation(shardMapManager, so.Id, so.UndoStartState, root);

            case StoreOperationCode.UpdateShard:
                return this.CreateUpdateShardOperation(shardMapManager, so.Id, so.UndoStartState, root);

            case StoreOperationCode.AddPointMapping:
            case StoreOperationCode.AddRangeMapping:
                return this.CreateAddMappingOperation(so.OpCode, shardMapManager, so.Id, so.UndoStartState, root, so.OriginalShardVersionAdds);

            case StoreOperationCode.RemovePointMapping:
            case StoreOperationCode.RemoveRangeMapping:
                return this.CreateRemoveMappingOperation(so.OpCode, shardMapManager, so.Id, so.UndoStartState, root, so.OriginalShardVersionRemoves);

            case StoreOperationCode.UpdatePointMapping:
            case StoreOperationCode.UpdateRangeMapping:
            case StoreOperationCode.UpdatePointMappingWithOffline:
            case StoreOperationCode.UpdateRangeMappingWithOffline:
                return this.CreateUpdateMappingOperation(so.OpCode, shardMapManager, so.Id, so.UndoStartState, root, so.OriginalShardVersionRemoves, so.OriginalShardVersionAdds);

            case StoreOperationCode.SplitMapping:
            case StoreOperationCode.MergeMappings:
                return this.CreateReplaceMappingsOperation(so.OpCode, shardMapManager, so.Id, so.UndoStartState, root, so.OriginalShardVersionAdds);

            default:
                Debug.Fail("Unexpected operation code.");
                return null;
        }*/
        return null;
    }

    @Override
    public IStoreOperation CreateAddShardOperation(ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState, Object root) {
        return null;
    }

    @Override
    public IStoreOperation CreateRemoveShardOperation(ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState, Object root) {
        return null;
    }

    @Override
    public IStoreOperation CreateUpdateShardOperation(ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState, Object root) {
        return null;
    }

    @Override
    public IStoreOperation CreateAddMappingOperation(StoreOperationCode operationCode, ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState, Object root, UUID shardIdOriginal) {
        return null;
    }

    @Override
    public IStoreOperation CreateRemoveMappingOperation(StoreOperationCode operationCode, ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState, Object root, UUID shardIdOriginal) {
        return null;
    }

    @Override
    public IStoreOperation CreateUpdateMappingOperation(StoreOperationCode operationCode, ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState, Object root, UUID shardIdOriginalSource, UUID shardIdOriginalTarget) {
        return null;
    }

    @Override
    public IStoreOperation CreateReplaceMappingsOperation(StoreOperationCode operationCode, ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState, Object root, UUID shardIdOriginalSource) {
        return null;
    }

    /**
     * Creates request to add shard to given shard map.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationId     Operation Id.
     * @param undoStartState  Undo start state.
     * @param root            Xml representation of the object.
     * @return The store operation.
     */
    public IStoreOperation CreateAddShardOperation(ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState, XElement root) {
        return null; /*new AddShardOperation(shardMapManager
                , operationId
                , undoStartState
                , StoreObjectFormatterXml.ReadIStoreShardMap(root.Element("ShardMap"))
                , StoreObjectFormatterXml.ReadIStoreShard(root.Element("Steps").Element("Step").Element("Shard")));*/
    }

    /**
     * Creates request to remove shard from given shard map.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationId     Operation Id.
     * @param undoStartState  Undo start state.
     * @param root            Xml representation of the object.
     * @return The store operation.
     */
    public IStoreOperation CreateRemoveShardOperation(ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState, XElement root) {
        return null; //TODO: new RemoveShardOperation(shardMapManager, operationId, undoStartState, StoreObjectFormatterXml.ReadIStoreShardMap(root.Element("ShardMap")), StoreObjectFormatterXml.ReadIStoreShard(root.Element("Steps").Element("Step").Element("Shard")));
    }

    /**
     * Creates request to update shard in given shard map.
     *
     * @param shardMapManager Shard map manager object.
     * @param operationId     Operation Id.
     * @param undoStartState  Undo start state.
     * @param root            Xml representation of the object.
     * @return The store operation.
     */
    public IStoreOperation CreateUpdateShardOperation(ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState, XElement root) {
        return null; //TODO: new UpdateShardOperation(shardMapManager, operationId, undoStartState, StoreObjectFormatterXml.ReadIStoreShardMap(root.Element("ShardMap")), StoreObjectFormatterXml.ReadIStoreShard(root.Element("Steps").Element("Step").Element("Shard")), StoreObjectFormatterXml.ReadIStoreShard(root.Element("Steps").Element("Step").Element("Update").Element("Shard")));
    }

    /**
     * Creates request to add a mapping in given shard map.
     *
     * @param operationCode            Operation code.
     * @param shardMapManager          Shard map manager object.
     * @param operationId              Operation Id.
     * @param undoStartState           Undo start state.
     * @param root                     Xml representation of the object.
     * @param originalShardVersionAdds Original shard version.
     * @return The store operation.
     */
    public IStoreOperation CreateAddMappingOperation(StoreOperationCode operationCode, ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState, XElement root, UUID originalShardVersionAdds) {
        return null; //TODO: new AddMappingOperation(shardMapManager, operationId, undoStartState, operationCode, StoreObjectFormatterXml.ReadIStoreShardMap(root.Element("ShardMap")), StoreObjectFormatterXml.ReadIStoreMapping(root.Element("Steps").Element("Step").Element("Mapping"), root.Element("Adds").Element("Shard")), originalShardVersionAdds);
    }

    /**
     * Creates request to remove a mapping in given shard map.
     *
     * @param operationCode               Operation code.
     * @param shardMapManager             Shard map manager object.
     * @param operationId                 Operation Id.
     * @param undoStartState              Undo start state.
     * @param root                        Xml representation of the object.
     * @param originalShardVersionRemoves Original shard version for Removes.
     * @return The store operation.
     */
    public IStoreOperation CreateRemoveMappingOperation(StoreOperationCode operationCode, ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState, XElement root, UUID originalShardVersionRemoves) {
        return null; //TODO: new RemoveMappingOperation(shardMapManager, operationId, undoStartState, operationCode, StoreObjectFormatterXml.ReadIStoreShardMap(root.Element("ShardMap")), StoreObjectFormatterXml.ReadIStoreMapping(root.Element("Steps").Element("Step").Element("Mapping"), root.Element("Removes").Element("Shard")), StoreObjectFormatterXml.ReadLock(root.Element("Steps").Element("Step").Element("Lock")), originalShardVersionRemoves);
    }

    /**
     * Creates request to update a mapping in given shard map.
     *
     * @param operationCode               Operation code.
     * @param shardMapManager             Shard map manager object.
     * @param operationId                 Operation Id.
     * @param undoStartState              Undo start state.
     * @param root                        Xml representation of the object.
     * @param originalShardVersionRemoves Original shard version for removes.
     * @param originalShardVersionAdds    Original shard version for adds.
     * @return The store operation.
     */
    public IStoreOperation CreateUpdateMappingOperation(StoreOperationCode operationCode, ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState, XElement root, UUID originalShardVersionRemoves, UUID originalShardVersionAdds) {
        return null; //TODO: new UpdateMappingOperation(shardMapManager, operationId, undoStartState, operationCode, StoreObjectFormatterXml.ReadIStoreShardMap(root.Element("ShardMap")), StoreObjectFormatterXml.ReadIStoreMapping(root.Element("Steps").Element("Step").Element("Mapping"), root.Element("Removes").Element("Shard")), StoreObjectFormatterXml.ReadIStoreMapping(root.Element("Steps").Element("Step").Element("Update").Element("Mapping"), root.Element("Adds").Element("Shard")), root.Element("PatternForKill").Value, StoreObjectFormatterXml.ReadLock(root.Element("Steps").Element("Step").Element("Lock")), originalShardVersionRemoves, originalShardVersionAdds);
    }

    /**
     * Creates request to replace a set of mappings with new set in given shard map.
     *
     * @param operationCode            Operation code.
     * @param shardMapManager          Shard map manager object.
     * @param operationId              Operation Id.
     * @param undoStartState           Undo start state.
     * @param root                     Xml representation of the object.
     * @param originalShardVersionAdds Original shard Id of source.
     * @return The store operation.
     */
    public IStoreOperation CreateReplaceMappingsOperation(StoreOperationCode operationCode, ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState, XElement root, UUID originalShardVersionAdds) {
        return null;
        //TODO:
    }

    ///#endregion Undo Operations

    ///#endregion Global And Local Operations
}