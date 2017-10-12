package com.microsoft.azure.elasticdb.shard.storeops.base;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.shard.base.LockOwnerIdOpType;
import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.base.SqlProtocol;
import com.microsoft.azure.elasticdb.shard.cache.CacheStoreMappingUpdatePolicy;
import com.microsoft.azure.elasticdb.shard.map.ShardMapType;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerCreateMode;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlShardMapManagerCredentials;
import com.microsoft.azure.elasticdb.shard.store.StoreLogEntry;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreSchemaInfo;
import com.microsoft.azure.elasticdb.shard.store.StoreShard;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.store.Version;
import com.microsoft.azure.elasticdb.shard.storeops.map.AddShardOperation;
import com.microsoft.azure.elasticdb.shard.storeops.map.FindShardByLocationGlobalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.map.GetShardsGlobalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.map.RemoveShardOperation;
import com.microsoft.azure.elasticdb.shard.storeops.map.UpdateShardOperation;
import com.microsoft.azure.elasticdb.shard.storeops.mapmanagerfactory.CreateShardMapManagerGlobalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.mapmanagerfactory.GetShardMapManagerGlobalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.mapmanger.AddShardMapGlobalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.mapmanger.FindShardMapByNameGlobalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.mapmanger.GetDistinctShardLocationsGlobalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.mapmanger.GetShardMapsGlobalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.mapmanger.LoadShardMapManagerGlobalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.mapmanger.RemoveShardMapGlobalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.mapper.AddMappingOperation;
import com.microsoft.azure.elasticdb.shard.storeops.mapper.FindMappingByIdGlobalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.mapper.FindMappingByKeyGlobalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.mapper.GetMappingsByRangeGlobalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.mapper.LockOrUnLockMappingsGlobalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.mapper.RemoveMappingOperation;
import com.microsoft.azure.elasticdb.shard.storeops.mapper.ReplaceMappingsOperation;
import com.microsoft.azure.elasticdb.shard.storeops.mapper.UpdateMappingOperation;
import com.microsoft.azure.elasticdb.shard.storeops.recovery.AttachShardOperation;
import com.microsoft.azure.elasticdb.shard.storeops.recovery.CheckShardLocalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.recovery.DetachShardGlobalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.recovery.GetMappingsByRangeLocalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.recovery.GetShardsLocalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.recovery.ReplaceMappingsGlobalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.recovery.ReplaceMappingsLocalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.schemainformation.AddShardingSchemaInfoGlobalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.schemainformation.FindShardingSchemaInfoGlobalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.schemainformation.GetShardingSchemaInfosGlobalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.schemainformation.RemoveShardingSchemaInfoGlobalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.schemainformation.UpdateShardingSchemaInfoGlobalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.upgrade.UpgradeStoreGlobalOperation;
import com.microsoft.azure.elasticdb.shard.storeops.upgrade.UpgradeStoreLocalOperation;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;

/**
 * Instantiates the operations that need to be undone corresponding to a request.
 */
public class StoreOperationFactory implements IStoreOperationFactory {

    /**
     * Create instance of StoreOperationFactory.
     */
    public StoreOperationFactory() {
    }

    /// #region Global Operations

    /**
     * Constructs request for deploying SMM storage objects to target GSM database.
     *
     * @param credentials
     *            Credentials for connection.
     * @param retryPolicy
     *            Retry policy.
     * @param operationName
     *            Operation name, useful for diagnostics.
     * @param createMode
     *            Creation mode.
     * @param targetVersion
     *            target version of store to deploy, this will be used mainly for upgrade testing purposes.
     * @return The store operation.
     */
    public IStoreOperationGlobal createCreateShardMapManagerGlobalOperation(SqlShardMapManagerCredentials credentials,
            RetryPolicy retryPolicy,
            String operationName,
            ShardMapManagerCreateMode createMode,
            Version targetVersion) {
        return new CreateShardMapManagerGlobalOperation(credentials, retryPolicy, operationName, createMode, targetVersion);
    }

    /**
     * Constructs request for obtaining shard map manager object if the GSM has the SMM objects in it.
     *
     * @param credentials
     *            Credentials for connection.
     * @param retryPolicy
     *            Retry policy.
     * @param operationName
     *            Operation name, useful for diagnostics.
     * @param throwOnFailure
     *            Whether to throw exception on failure or return error code.
     * @return The store operation.
     */
    public IStoreOperationGlobal createGetShardMapManagerGlobalOperation(SqlShardMapManagerCredentials credentials,
            RetryPolicy retryPolicy,
            String operationName,
            boolean throwOnFailure) {
        return new GetShardMapManagerGlobalOperation(credentials, retryPolicy, operationName, throwOnFailure);
    }

    /**
     * Constructs request for Detaching the given shard and mapping information to the GSM database.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationName
     *            Operation name.
     * @param location
     *            Location to be detached.
     * @param shardMapName
     *            Shard map from which shard is being detached.
     * @return The store operation.
     */
    public IStoreOperationGlobal createDetachShardGlobalOperation(ShardMapManager shardMapManager,
            String operationName,
            ShardLocation location,
            String shardMapName) {
        return new DetachShardGlobalOperation(shardMapManager, operationName, location, shardMapName);
    }

    /**
     * Constructs request for replacing the GSM mappings for given shard map with the input mappings.
     *
     * @param shardMapManager
     *            Shard map manager.
     * @param operationName
     *            Operation name.
     * @param shardMap
     *            GSM Shard map.
     * @param shard
     *            GSM Shard.
     * @param mappingsToRemove
     *            Optional list of mappings to remove.
     * @param mappingsToAdd
     *            List of mappings to add.
     * @return The store operation.
     */
    public IStoreOperationGlobal createReplaceMappingsGlobalOperation(ShardMapManager shardMapManager,
            String operationName,
            StoreShardMap shardMap,
            StoreShard shard,
            List<StoreMapping> mappingsToRemove,
            List<StoreMapping> mappingsToAdd) {
        return new ReplaceMappingsGlobalOperation(shardMapManager, operationName, shardMap, shard, mappingsToRemove, mappingsToAdd);
    }

    /**
     * Constructs a request to add schema info to GSM.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationName
     *            Operation name, useful for diagnostics.
     * @param schemaInfo
     *            Schema info to add.
     * @return The store operation.
     */
    public IStoreOperationGlobal createAddShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager,
            String operationName,
            StoreSchemaInfo schemaInfo) {
        return new AddShardingSchemaInfoGlobalOperation(shardMapManager, operationName, schemaInfo);
    }

    /**
     * Constructs a request to find schema info in GSM.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationName
     *            Operation name, useful for diagnostics.
     * @param schemaInfoName
     *            Name of schema info to search.
     * @return The store operation.
     */
    public IStoreOperationGlobal createFindShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager,
            String operationName,
            String schemaInfoName) {
        return new FindShardingSchemaInfoGlobalOperation(shardMapManager, operationName, schemaInfoName);
    }

    /**
     * Constructs a request to get all schema info objects from GSM.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationName
     *            Operation name, useful for diagnostics.
     * @return The store operation.
     */
    public IStoreOperationGlobal createGetShardingSchemaInfosGlobalOperation(ShardMapManager shardMapManager,
            String operationName) {
        return new GetShardingSchemaInfosGlobalOperation(shardMapManager, operationName);
    }

    /**
     * Constructs a request to delete schema info from GSM.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationName
     *            Operation name, useful for diagnostics.
     * @param schemaInfoName
     *            Name of schema info to delete.
     * @return The store operation.
     */
    public IStoreOperationGlobal createRemoveShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager,
            String operationName,
            String schemaInfoName) {
        return new RemoveShardingSchemaInfoGlobalOperation(shardMapManager, operationName, schemaInfoName);
    }

    /**
     * Constructs a request to update schema info to GSM.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationName
     *            Operation name, useful for diagnostics.
     * @param schemaInfo
     *            Schema info to update.
     * @return The store operation.
     */
    public IStoreOperationGlobal createUpdateShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager,
            String operationName,
            StoreSchemaInfo schemaInfo) {
        return new UpdateShardingSchemaInfoGlobalOperation(shardMapManager, operationName, schemaInfo);
    }

    /**
     * Constructs request to get shard with specific location for given shard map from GSM.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationName
     *            Operation name, useful for diagnostics.
     * @param shardMap
     *            Shard map for which shard is being requested.
     * @param location
     *            Location of shard being searched.
     * @return The store operation.
     */
    public IStoreOperationGlobal createFindShardByLocationGlobalOperation(ShardMapManager shardMapManager,
            String operationName,
            StoreShardMap shardMap,
            ShardLocation location) {
        return new FindShardByLocationGlobalOperation(shardMapManager, operationName, shardMap, location);
    }

    /**
     * Constructs request to get all shards for given shard map from GSM.
     *
     * @param operationName
     *            Operation name, useful for diagnostics.
     * @param shardMapManager
     *            Shard map manager object.
     * @param shardMap
     *            Shard map for which shards are being requested.
     * @return The store operation.
     */
    public IStoreOperationGlobal createGetShardsGlobalOperation(String operationName,
            ShardMapManager shardMapManager,
            StoreShardMap shardMap) {
        return new GetShardsGlobalOperation(operationName, shardMapManager, shardMap);
    }

    /**
     * Constructs request to add given shard map to GSM.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationName
     *            Operation name, useful for diagnostics.
     * @param shardMap
     *            Shard map to add.
     * @return The store operation.
     */
    public IStoreOperationGlobal createAddShardMapGlobalOperation(ShardMapManager shardMapManager,
            String operationName,
            StoreShardMap shardMap) {
        return new AddShardMapGlobalOperation(shardMapManager, operationName, shardMap);
    }

    /**
     * Constructs request to find shard map with given name from GSM.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationName
     *            Operation name, useful for diagnostics.
     * @param shardMapName
     *            Name of the shard map being searched.
     * @return The store operation.
     */
    public IStoreOperationGlobal createFindShardMapByNameGlobalOperation(ShardMapManager shardMapManager,
            String operationName,
            String shardMapName) {
        return new FindShardMapByNameGlobalOperation(shardMapManager, operationName, shardMapName);
    }

    /**
     * Constructs request to get distinct shard locations from GSM.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationName
     *            Operation name, useful for diagnostics.
     * @return The store operation.
     */
    public IStoreOperationGlobal createGetDistinctShardLocationsGlobalOperation(ShardMapManager shardMapManager,
            String operationName) {
        return new GetDistinctShardLocationsGlobalOperation(shardMapManager, operationName);
    }

    /**
     * Constructs request to get all shard maps from GSM.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationName
     *            Operation name, useful for diagnostics.
     * @return The store operation.
     */
    public IStoreOperationGlobal createGetShardMapsGlobalOperation(ShardMapManager shardMapManager,
            String operationName) {
        return new GetShardMapsGlobalOperation(shardMapManager, operationName);
    }

    /**
     * Constructs request to get all shard maps from GSM.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationName
     *            Operation name, useful for diagnostics.
     * @return The store operation.
     */
    public IStoreOperationGlobal createLoadShardMapManagerGlobalOperation(ShardMapManager shardMapManager,
            String operationName) {
        return new LoadShardMapManagerGlobalOperation(shardMapManager, operationName);
    }

    /**
     * Constructs request to remove given shard map from GSM.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationName
     *            Operation name, useful for diagnostics.
     * @param shardMap
     *            Shard map to remove.
     * @return The store operation.
     */
    public IStoreOperationGlobal createRemoveShardMapGlobalOperation(ShardMapManager shardMapManager,
            String operationName,
            StoreShardMap shardMap) {
        return new RemoveShardMapGlobalOperation(shardMapManager, operationName, shardMap);
    }

    /**
     * Constructs request for obtaining mapping from GSM based on given key.
     *
     * @param shardMapManager
     *            Shard map manager.
     * @param operationName
     *            Operation being executed.
     * @param shardMap
     *            Local shard map.
     * @param mapping
     *            Mapping whose Id will be used.
     * @param errorCategory
     *            Error category.
     * @return The store operation.
     */
    public IStoreOperationGlobal createFindMappingByIdGlobalOperation(ShardMapManager shardMapManager,
            String operationName,
            StoreShardMap shardMap,
            StoreMapping mapping,
            ShardManagementErrorCategory errorCategory) {
        return new FindMappingByIdGlobalOperation(shardMapManager, operationName, shardMap, mapping, errorCategory);
    }

    /**
     * Constructs request for obtaining mapping from GSM based on given key.
     *
     * @param shardMapManager
     *            Shard map manager.
     * @param operationName
     *            Operation being executed.
     * @param shardMap
     *            Local shard map.
     * @param key
     *            Key for lookup operation.
     * @param policy
     *            Policy for cache update.
     * @param errorCategory
     *            Error category.
     * @param cacheResults
     *            Whether to cache the results of the operation.
     * @param ignoreFailure
     *            Ignore shard map not found error.
     * @return The store operation.
     */
    public IStoreOperationGlobal createFindMappingByKeyGlobalOperation(ShardMapManager shardMapManager,
            String operationName,
            StoreShardMap shardMap,
            ShardKey key,
            CacheStoreMappingUpdatePolicy policy,
            ShardManagementErrorCategory errorCategory,
            boolean cacheResults,
            boolean ignoreFailure) {
        return new FindMappingByKeyGlobalOperation(shardMapManager, operationName, shardMap, key, policy, errorCategory, cacheResults, ignoreFailure);
    }

    /**
     * Constructs request for obtaining all the mappings from GSM based on given shard and mappings.
     *
     * @param shardMapManager
     *            Shard map manager.
     * @param operationName
     *            Operation being executed.
     * @param shardMap
     *            Local shard map.
     * @param shard
     *            Local shard.
     * @param range
     *            Optional range to get mappings from.
     * @param errorCategory
     *            Error category.
     * @param cacheResults
     *            Whether to cache the results of the operation.
     * @param ignoreFailure
     *            Ignore shard map not found error.
     * @return The store operation.
     */
    public IStoreOperationGlobal createGetMappingsByRangeGlobalOperation(ShardMapManager shardMapManager,
            String operationName,
            StoreShardMap shardMap,
            StoreShard shard,
            ShardRange range,
            ShardManagementErrorCategory errorCategory,
            boolean cacheResults,
            boolean ignoreFailure) {
        return new GetMappingsByRangeGlobalOperation(shardMapManager, operationName, shardMap, shard, range, errorCategory, cacheResults,
                ignoreFailure);
    }

    /**
     * Constructs request to lock or unlock given mappings in GSM.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationName
     *            Operation name, useful for diagnostics.
     * @param shardMap
     *            Shard map to add.
     * @param mapping
     *            Mapping to lock or unlock. Null means all mappings.
     * @param lockOwnerId
     *            Lock owner.
     * @param lockOpType
     *            Lock operation type.
     * @param errorCategory
     *            Error category.
     * @return The store operation.
     */
    public IStoreOperationGlobal createLockOrUnLockMappingsGlobalOperation(ShardMapManager shardMapManager,
            String operationName,
            StoreShardMap shardMap,
            StoreMapping mapping,
            UUID lockOwnerId,
            LockOwnerIdOpType lockOpType,
            ShardManagementErrorCategory errorCategory) {
        return new LockOrUnLockMappingsGlobalOperation(shardMapManager, operationName, shardMap, mapping, lockOwnerId, lockOpType, errorCategory);
    }

    /**
     * Constructs a request to upgrade global store.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationName
     *            Operation name, useful for diagnostics.
     * @param targetVersion
     *            Target version of store to deploy.
     * @return The store operation.
     */
    public IStoreOperationGlobal createUpgradeStoreGlobalOperation(ShardMapManager shardMapManager,
            String operationName,
            Version targetVersion) {
        return new UpgradeStoreGlobalOperation(shardMapManager, operationName, targetVersion);
    }

    /// #endregion Global Operations

    /// #region Local Operations

    /**
     * Constructs request for obtaining all the shard maps and shards from an LSM.
     *
     * @param operationName
     *            Operation name.
     * @param shardMapManager
     *            Shard map manager.
     * @param location
     *            Location of the LSM.
     */
    public IStoreOperationLocal createCheckShardLocalOperation(String operationName,
            ShardMapManager shardMapManager,
            ShardLocation location) {
        return new CheckShardLocalOperation(operationName, shardMapManager, location);
    }

    /**
     * Constructs request for obtaining all the shard maps and shards from an LSM.
     *
     * @param shardMapManager
     *            Shard map manager.
     * @param location
     *            Location of the LSM.
     * @param operationName
     *            Operation name.
     * @param shardMap
     *            Local shard map.
     * @param shard
     *            Local shard.
     * @param range
     *            Optional range to get mappings from.
     * @param ignoreFailure
     *            Ignore shard map not found error.
     */
    public IStoreOperationLocal createGetMappingsByRangeLocalOperation(ShardMapManager shardMapManager,
            ShardLocation location,
            String operationName,
            StoreShardMap shardMap,
            StoreShard shard,
            ShardRange range,
            boolean ignoreFailure) {
        return new GetMappingsByRangeLocalOperation(shardMapManager, location, operationName, shardMap, shard, range, ignoreFailure);
    }

    /**
     * Constructs request for obtaining all the shard maps and shards from an LSM.
     *
     * @param shardMapManager
     *            Shard map manager.
     * @param location
     *            Location of the LSM.
     * @param operationName
     *            Operation name.
     */
    public IStoreOperationLocal createGetShardsLocalOperation(ShardMapManager shardMapManager,
            ShardLocation location,
            String operationName) {
        return new GetShardsLocalOperation(shardMapManager, location, operationName);
    }

    /**
     * Constructs request for replacing the LSM mappings for given shard map with the input mappings.
     *
     * @param shardMapManager
     *            Shard map manager.
     * @param location
     *            Location of the LSM.
     * @param operationName
     *            Operation name.
     * @param shardMap
     *            Local shard map.
     * @param shard
     *            Local shard.
     * @param rangesToRemove
     *            Optional list of ranges to minimize amount of deletions.
     * @param mappingsToAdd
     *            List of mappings to add.
     */
    public IStoreOperationLocal createReplaceMappingsLocalOperation(ShardMapManager shardMapManager,
            ShardLocation location,
            String operationName,
            StoreShardMap shardMap,
            StoreShard shard,
            List<ShardRange> rangesToRemove,
            List<StoreMapping> mappingsToAdd) {
        return new ReplaceMappingsLocalOperation(shardMapManager, location, operationName, shardMap, shard, rangesToRemove, mappingsToAdd);
    }

    /**
     * Constructs a request to upgrade store location.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param location
     *            Location to upgrade.
     * @param operationName
     *            Operation name, useful for diagnostics.
     * @param targetVersion
     *            Target version of store to deploy.
     * @return The store operation.
     */
    public IStoreOperationLocal createUpgradeStoreLocalOperation(ShardMapManager shardMapManager,
            ShardLocation location,
            String operationName,
            Version targetVersion) {
        return new UpgradeStoreLocalOperation(shardMapManager, location, operationName, targetVersion);
    }

    /// #endregion LocalOperations

    /// #region Global And Local Operations

    /**
     * Creates request to add shard to given shard map.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param shardMap
     *            Shard map for which to add shard.
     * @param shard
     *            Shard to add.
     * @return The store operation.
     */
    public IStoreOperation createAddShardOperation(ShardMapManager shardMapManager,
            StoreShardMap shardMap,
            StoreShard shard) {
        return new AddShardOperation(shardMapManager, shardMap, shard);
    }

    /**
     * Creates request to add shard to given shard map.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationId
     *            Operation Id.
     * @param undoStartState
     *            Undo start state.
     * @param root
     *            Xml representation of the object.
     * @return The store operation.
     */
    public IStoreOperation createAddShardOperation(ShardMapManager shardMapManager,
            UUID operationId,
            StoreOperationState undoStartState,
            Element root) {
        StoreShardMap shardMap = getStoreShardMapFromXml(root);
        StoreShard shard = getStoreShardFromXml(root);
        return new AddShardOperation(shardMapManager, operationId, undoStartState, shardMap, shard);
    }

    /**
     * Creates request to remove shard from given shard map.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationId
     *            Operation Id.
     * @param undoStartState
     *            Undo start state.
     * @param root
     *            Xml representation of the object.
     * @return The store operation.
     */
    public IStoreOperation createRemoveShardOperation(ShardMapManager shardMapManager,
            UUID operationId,
            StoreOperationState undoStartState,
            Element root) {
        StoreShardMap shardMap = getStoreShardMapFromXml(root);
        StoreShard shard = getStoreShardFromXml(root);
        return new RemoveShardOperation(shardMapManager, operationId, undoStartState, shardMap, shard);
    }

    /**
     * Creates request to remove shard from given shard map.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param shardMap
     *            Shard map for which to remove shard.
     * @param shard
     *            Shard to remove.
     * @return The store operation.
     */
    public IStoreOperation createRemoveShardOperation(ShardMapManager shardMapManager,
            StoreShardMap shardMap,
            StoreShard shard) {
        return new RemoveShardOperation(shardMapManager, shardMap, shard);
    }

    /**
     * Creates request to update shard in given shard map.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param shardMap
     *            Shard map for which to remove shard.
     * @param shardOld
     *            Shard to update.
     * @param shardNew
     *            Updated shard.
     * @return The store operation.
     */
    public IStoreOperation createUpdateShardOperation(ShardMapManager shardMapManager,
            StoreShardMap shardMap,
            StoreShard shardOld,
            StoreShard shardNew) {
        return new UpdateShardOperation(shardMapManager, shardMap, shardOld, shardNew);
    }

    /**
     * Creates request to update shard in given shard map.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationId
     *            Operation Id.
     * @param undoStartState
     *            Undo start state.
     * @param root
     *            Xml representation of the object.
     * @return The store operation.
     */
    public IStoreOperation createUpdateShardOperation(ShardMapManager shardMapManager,
            UUID operationId,
            StoreOperationState undoStartState,
            Element root) {
        StoreShardMap shardMap = getStoreShardMapFromXml(root);
        StoreShard shardOld = getStoreShardFromXml((Element) root.getElementsByTagName("Step").item(0));
        StoreShard shardNew = getStoreShardFromXml((Element) root.getElementsByTagName("Update").item(0));
        return new UpdateShardOperation(shardMapManager, operationId, undoStartState, shardMap, shardOld, shardNew);
    }

    /**
     * Creates request to add a mapping in given shard map.
     *
     * @param operationCode
     *            Operation code.
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationId
     *            Operation Id.
     * @param undoStartState
     *            Undo start state.
     * @param root
     *            Xml representation of the object.
     * @param originalShardVersionAdds
     *            Original shard version.
     * @return The store operation.
     */
    public IStoreOperation createAddMappingOperation(StoreOperationCode operationCode,
            ShardMapManager shardMapManager,
            UUID operationId,
            StoreOperationState undoStartState,
            Element root,
            UUID originalShardVersionAdds) {
        StoreShardMap shardMap = getStoreShardMapFromXml(root);
        StoreMapping mapping = getStoreMappingFromXml(root, null);
        return new AddMappingOperation(shardMapManager, operationId, undoStartState, operationCode, shardMap, mapping, originalShardVersionAdds);
    }

    /**
     * Creates request to add shard to given shard map.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationCode
     *            Store operation code.
     * @param shardMap
     *            Shard map for which to add mapping.
     * @param mapping
     *            Mapping to add.
     * @return The store operation.
     */
    public IStoreOperation createAddMappingOperation(ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            StoreShardMap shardMap,
            StoreMapping mapping) {
        return new AddMappingOperation(shardMapManager, operationCode, shardMap, mapping);
    }

    /**
     * Constructs request for attaching the given shard map and shard information to the GSM database.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param shard
     *            Shard to attach
     * @param shardMap
     *            Shard map to attach specified shard
     * @return The store operation.
     */
    public IStoreOperation createAttachShardOperation(ShardMapManager shardMapManager,
            StoreShardMap shardMap,
            StoreShard shard) {
        return new AttachShardOperation(shardMapManager, shardMap, shard);
    }

    /**
     * Creates request to add shard to given shard map.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationCode
     *            Store operation code.
     * @param shardMap
     *            Shard map from which to remove mapping.
     * @param mapping
     *            Mapping to add.
     * @param lockOwnerId
     *            Id of lock owner.
     * @return The store operation.
     */
    public IStoreOperation createRemoveMappingOperation(ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            StoreShardMap shardMap,
            StoreMapping mapping,
            UUID lockOwnerId) {
        return new RemoveMappingOperation(shardMapManager, operationCode, shardMap, mapping, lockOwnerId);
    }

    /**
     * Creates request to remove a mapping in given shard map.
     *
     * @param operationCode
     *            Operation code.
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationId
     *            Operation Id.
     * @param undoStartState
     *            Undo start state.
     * @param root
     *            Xml representation of the object.
     * @param originalShardVersionRemoves
     *            Original shard version for Removes.
     * @return The store operation.
     */
    public IStoreOperation createRemoveMappingOperation(StoreOperationCode operationCode,
            ShardMapManager shardMapManager,
            UUID operationId,
            StoreOperationState undoStartState,
            Element root,
            UUID originalShardVersionRemoves) {
        StoreShardMap shardMap = getStoreShardMapFromXml(root);
        StoreShard shard = getStoreShardFromXml(root);
        StoreMapping mapping = getStoreMappingFromXml(root, shard);
        UUID lockOwnerId = UUID.fromString(root.getElementsByTagName("Lock").item(0).getTextContent());
        return new RemoveMappingOperation(shardMapManager, operationId, undoStartState, operationCode, shardMap, mapping, lockOwnerId,
                originalShardVersionRemoves);
    }

    /**
     * Creates request to update a mapping in given shard map.
     *
     * @param operationCode
     *            Operation code.
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationId
     *            Operation Id.
     * @param undoStartState
     *            Undo start state.
     * @param root
     *            Xml representation of the object.
     * @param originalShardVersionRemoves
     *            Original shard version for removes.
     * @param originalShardVersionAdds
     *            Original shard version for adds.
     * @return The store operation.
     */
    public IStoreOperation createUpdateMappingOperation(StoreOperationCode operationCode,
            ShardMapManager shardMapManager,
            UUID operationId,
            StoreOperationState undoStartState,
            Element root,
            UUID originalShardVersionRemoves,
            UUID originalShardVersionAdds) {
        StoreShardMap shardMap = getStoreShardMapFromXml(root);
        StoreShard shardSource = getStoreShardFromXml((Element) root.getElementsByTagName("Removes").item(0));
        StoreShard shardTarget = getStoreShardFromXml((Element) root.getElementsByTagName("Adds").item(0));
        StoreMapping mappingSource = getStoreMappingFromXml((Element) root.getElementsByTagName("Step").item(0), shardSource);
        StoreMapping mappingTarget = getStoreMappingFromXml((Element) root.getElementsByTagName("Update").item(0), shardTarget);
        String patternForKill = root.getElementsByTagName("PatternForKill").item(0).getTextContent();
        UUID lockOwnerId = UUID.fromString(root.getElementsByTagName("Lock").item(0).getTextContent());
        return new UpdateMappingOperation(shardMapManager, operationId, undoStartState, operationCode, shardMap, mappingSource, mappingTarget,
                patternForKill, lockOwnerId, originalShardVersionRemoves, originalShardVersionAdds);
    }

    /**
     * Creates request to add shard to given shard map.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationCode
     *            Store operation code.
     * @param shardMap
     *            Shard map for which to update mapping.
     * @param mappingSource
     *            Mapping to update.
     * @param mappingTarget
     *            Updated mapping.
     * @param patternForKill
     *            Pattern for kill commands.
     * @param lockOwnerId
     *            Id of lock owner.
     * @return The store operation.
     */
    public IStoreOperation createUpdateMappingOperation(ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            StoreShardMap shardMap,
            StoreMapping mappingSource,
            StoreMapping mappingTarget,
            String patternForKill,
            UUID lockOwnerId) {
        return new UpdateMappingOperation(shardMapManager, operationCode, shardMap, mappingSource, mappingTarget, patternForKill, lockOwnerId);
    }

    /**
     * Creates request to replace mappings within shard map.
     *
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationCode
     *            Store operation code.
     * @param shardMap
     *            Shard map for which to update mapping.
     * @param mappingsSource
     *            Original mappings.
     * @param mappingsTarget
     *            Target mappings mapping.
     * @return The store operation.
     */
    public IStoreOperation createReplaceMappingsOperation(ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            StoreShardMap shardMap,
            List<Pair<StoreMapping, UUID>> mappingsSource,
            List<Pair<StoreMapping, UUID>> mappingsTarget) {
        return new ReplaceMappingsOperation(shardMapManager, operationCode, shardMap, mappingsSource, mappingsTarget);
    }

    /**
     * Creates request to replace a set of mappings with new set in given shard map.
     *
     * @param operationCode
     *            Operation code.
     * @param shardMapManager
     *            Shard map manager object.
     * @param operationId
     *            Operation Id.
     * @param undoStartState
     *            Undo start state.
     * @param root
     *            Xml representation of the object.
     * @param originalShardVersionAdds
     *            Original shard Id of source.
     * @return The store operation.
     */
    public IStoreOperation createReplaceMappingsOperation(StoreOperationCode operationCode,
            ShardMapManager shardMapManager,
            UUID operationId,
            StoreOperationState undoStartState,
            Element root,
            UUID originalShardVersionAdds) {
        try {
            StoreShard sourceShard = getStoreShardFromXml((Element) root.getElementsByTagName("Removes").item(0));
            StoreShard targetShard = getStoreShardFromXml((Element) root.getElementsByTagName("Adds").item(0));

            List<Pair<StoreMapping, UUID>> mappingsSource = new ArrayList<>();
            XPath xmlPath = XPathFactory.newInstance().newXPath();
            NodeList nodeList = (NodeList) xmlPath.compile("//Step[@Kind=\"1\"]").evaluate(root, XPathConstants.NODESET);
            for (int i = 0; i < nodeList.getLength(); i++) {
                Element e = (Element) nodeList.item(i);
                mappingsSource.add(new ImmutablePair<>(getStoreMappingFromXml(e, sourceShard),
                        UUID.fromString(e.getElementsByTagName("Lock").item(0).getTextContent())));
            }

            List<Pair<StoreMapping, UUID>> mappingsTarget = new ArrayList<>();
            nodeList = (NodeList) xmlPath.compile("//Step[@Kind=\"3\"]").evaluate(root, XPathConstants.NODESET);
            for (int i = 0; i < nodeList.getLength(); i++) {
                Element e = (Element) nodeList.item(i);
                mappingsTarget.add(new ImmutablePair<>(getStoreMappingFromXml(e, targetShard),
                        UUID.fromString(e.getElementsByTagName("Lock").item(0).getTextContent())));
            }

            StoreShardMap shardMap = getStoreShardMapFromXml(root);
            return new ReplaceMappingsOperation(shardMapManager, operationId, undoStartState, operationCode, shardMap, mappingsSource, mappingsTarget,
                    originalShardVersionAdds);
        }
        catch (XPathExpressionException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Create operation corresponding to the <see cref="StoreLogEntry"/> information.
     *
     * @param shardMapManager
     *            ShardMapManager instance for undo operation.
     * @param so
     *            Store operation information.
     * @return The operation to be undone.
     */
    public IStoreOperation fromLogEntry(ShardMapManager shardMapManager,
            StoreLogEntry so) {
        StoreOperationCode code = so.getOpCode();
        switch (code) {
            case AddShard:
                return this.createAddShardOperation(shardMapManager, so.getId(), so.getUndoStartState(), so.getData());

            case RemoveShard:
                return this.createRemoveShardOperation(shardMapManager, so.getId(), so.getUndoStartState(), so.getData());

            case UpdateShard:
                return this.createUpdateShardOperation(shardMapManager, so.getId(), so.getUndoStartState(), so.getData());

            case AddPointMapping:
            case AddRangeMapping:
                return this.createAddMappingOperation(code, shardMapManager, so.getId(), so.getUndoStartState(), so.getData(),
                        so.getOriginalShardVersionAdds());

            case RemovePointMapping:
            case RemoveRangeMapping:
                return this.createRemoveMappingOperation(code, shardMapManager, so.getId(), so.getUndoStartState(), so.getData(),
                        so.getOriginalShardVersionRemoves());

            case UpdatePointMapping:
            case UpdateRangeMapping:
            case UpdatePointMappingWithOffline:
            case UpdateRangeMappingWithOffline:
                return this.createUpdateMappingOperation(code, shardMapManager, so.getId(), so.getUndoStartState(), so.getData(),
                        so.getOriginalShardVersionRemoves(), so.getOriginalShardVersionAdds());

            case SplitMapping:
            case MergeMappings:
                return this.createReplaceMappingsOperation(code, shardMapManager, so.getId(), so.getUndoStartState(), so.getData(),
                        so.getOriginalShardVersionAdds());

            default:
                // Debug.Fail("Unexpected operation code.");
                return null;
        }
    }

    /// #endregion Global And Local Operations

    private Node getFirstNode(Element root,
            String name) {
        if (root.getElementsByTagName(name).getLength() > 0) {
            Node node = root.getElementsByTagName(name).item(0);
            if (node.getAttributes().getLength() > 0) {
                if (node.getAttributes().getNamedItem("Null") != null && node.getAttributes().getNamedItem("Null").getNodeValue().equals("1")) {
                    return null;
                }
            }
            if (node.hasChildNodes()) {
                return node;
            }
        }
        return null;
    }

    private StoreShardMap getStoreShardMapFromXml(Element root) {
        Element element = (Element) getFirstNode(root, "ShardMap");
        if (element != null) {
            UUID id = UUID.fromString(element.getElementsByTagName("Id").item(0).getTextContent());
            String name = element.getElementsByTagName("Name").item(0).getTextContent();
            ShardMapType mapType = ShardMapType.forValue(Integer.parseInt(element.getElementsByTagName("Kind").item(0).getTextContent()));
            ShardKeyType keyType = ShardKeyType.forValue(Integer.parseInt(element.getElementsByTagName("KeyKind").item(0).getTextContent()));
            return new StoreShardMap(id, name, mapType, keyType);
        }
        return new StoreShardMap();
    }

    private StoreShard getStoreShardFromXml(Element root) {
        Element element = (Element) getFirstNode(root, "Shard");
        if (element != null) {
            UUID id = UUID.fromString(element.getElementsByTagName("Id").item(0).getTextContent());
            UUID version = UUID.fromString(element.getElementsByTagName("Version").item(0).getTextContent());
            UUID mapId = UUID.fromString(element.getElementsByTagName("ShardMapId").item(0).getTextContent());
            ShardLocation location = getShardLocationFromXml((Element) element.getElementsByTagName("Location").item(0));
            Integer status = Integer.parseInt(element.getElementsByTagName("Status").item(0).getTextContent());
            return new StoreShard(id, version, mapId, location, status);
        }
        return new StoreShard();
    }

    private ShardLocation getShardLocationFromXml(Element location) {
        if (location != null) {
            SqlProtocol protocol = SqlProtocol.forValue(Integer.parseInt(location.getElementsByTagName("Protocol").item(0).getTextContent()));
            String server = location.getElementsByTagName("ServerName").item(0).getTextContent();
            Integer port = Integer.parseInt(location.getElementsByTagName("Port").item(0).getTextContent());
            String database = location.getElementsByTagName("DatabaseName").item(0).getTextContent();
            return new ShardLocation(server, database, protocol, port);
        }
        return new ShardLocation();
    }

    private StoreMapping getStoreMappingFromXml(Element root,
            StoreShard shard) {
        Element element = (Element) getFirstNode(root, "Mapping");
        if (element != null) {
            UUID id = UUID.fromString(element.getElementsByTagName("Id").item(0).getTextContent());
            UUID shardMapId = UUID.fromString(element.getElementsByTagName("ShardMapId").item(0).getTextContent());
            UUID lockOwnerId = UUID.fromString(element.getElementsByTagName("LockOwnerId").item(0).getTextContent());
            Integer status = Integer.parseInt(element.getElementsByTagName("Status").item(0).getTextContent());
            byte[] minValue = getByteArrayFromBinaryString(element.getElementsByTagName("MinValue").item(0).getTextContent());
            byte[] maxValue = getByteArrayFromBinaryString(element.getElementsByTagName("MaxValue").item(0).getTextContent());
            if (shard == null) {
                shard = getStoreShardFromXml(root);
            }
            return new StoreMapping(id, shardMapId, minValue, maxValue, status, lockOwnerId, shard);
        }
        return new StoreMapping();
    }

    private byte[] getByteArrayFromBinaryString(String value) {
        if (!StringUtilsLocal.isNullOrEmpty(value)) {
            String binaryValue = value.substring(2, value.length());
            int returnValueSize = binaryValue.length() / 2;
            byte[] returnValue = new byte[returnValueSize];
            for (int i = 0; i < returnValueSize; i++) {
                returnValue[i] = (byte) Integer.parseInt(binaryValue.substring((i * 2), ((i * 2) + 2)), 16);
            }
            return returnValue;
        }
        return new byte[0];
    }
}