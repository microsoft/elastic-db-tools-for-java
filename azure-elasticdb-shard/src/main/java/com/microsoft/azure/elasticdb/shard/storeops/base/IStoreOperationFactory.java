package com.microsoft.azure.elasticdb.shard.storeops.base;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

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
import com.microsoft.azure.elasticdb.shard.store.StoreLogEntry;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreSchemaInfo;
import com.microsoft.azure.elasticdb.shard.store.StoreShard;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.store.Version;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Factory for storage operation creation.
 */
public interface IStoreOperationFactory {
  ///#region Global Operations

  /**
   * Constructs request for deploying SMM storage objects to target GSM database.
   *
   * @param credentials Credentials for connection.
   * @param retryPolicy Retry policy.
   * @param operationName Operation name, useful for diagnostics.
   * @param createMode Creation mode.
   * @param targetVersion target version of store to deploy, this will be used mainly for upgrade
   * testing purposes.
   * @return The store operation.
   */
  IStoreOperationGlobal createCreateShardMapManagerGlobalOperation(
      SqlShardMapManagerCredentials credentials, RetryPolicy retryPolicy, String operationName,
      ShardMapManagerCreateMode createMode, Version targetVersion);

  /**
   * Constructs request for obtaining shard map manager object if the GSM has the SMM objects in it.
   *
   * @param credentials Credentials for connection.
   * @param retryPolicy Retry policy.
   * @param operationName Operation name, useful for diagnostics.
   * @param throwOnFailure Whether to throw exception on failure or return error code.
   * @return The store operation.
   */
  IStoreOperationGlobal createGetShardMapManagerGlobalOperation(
      SqlShardMapManagerCredentials credentials, RetryPolicy retryPolicy, String operationName,
      boolean throwOnFailure);

  /**
   * Constructs request for Detaching the given shard and mapping information to the GSM database.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationName Operation name.
   * @param location Location to be detached.
   * @param shardMapName Shard map from which shard is being detached.
   * @return The store operation.
   */
  IStoreOperationGlobal createDetachShardGlobalOperation(ShardMapManager shardMapManager,
      String operationName, ShardLocation location, String shardMapName);

  /**
   * Constructs request for replacing the GSM mappings for given shard map with the input mappings.
   *
   * @param shardMapManager Shard map manager.
   * @param operationName Operation name.
   * @param shardMap GSM Shard map.
   * @param shard GSM Shard.
   * @param mappingsToRemove Optional list of mappings to remove.
   * @param mappingsToAdd List of mappings to add.
   * @return The store operation.
   */
  IStoreOperationGlobal createReplaceMappingsGlobalOperation(ShardMapManager shardMapManager,
      String operationName, StoreShardMap shardMap, StoreShard shard,
      List<StoreMapping> mappingsToRemove, List<StoreMapping> mappingsToAdd);

  /**
   * Constructs a request to add schema info to GSM.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationName Operation name, useful for diagnostics.
   * @param schemaInfo Schema info to add.
   * @return The store operation.
   */
  IStoreOperationGlobal createAddShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager,
      String operationName, StoreSchemaInfo schemaInfo);

  /**
   * Constructs a request to find schema info in GSM.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationName Operation name, useful for diagnostics.
   * @param schemaInfoName Name of schema info to search.
   * @return The store operation.
   */
  IStoreOperationGlobal createFindShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager,
      String operationName, String schemaInfoName);

  /**
   * Constructs a request to get all schema info objects from GSM.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationName Operation name, useful for diagnostics.
   * @return The store operation.
   */
  IStoreOperationGlobal createGetShardingSchemaInfosGlobalOperation(ShardMapManager shardMapManager,
      String operationName);

  /**
   * Constructs a request to delete schema info from GSM.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationName Operation name, useful for diagnostics.
   * @param schemaInfoName Name of schema info to delete.
   * @return The store operation.
   */
  IStoreOperationGlobal createRemoveShardingSchemaInfoGlobalOperation(
      ShardMapManager shardMapManager, String operationName, String schemaInfoName);

  /**
   * Constructs a request to update schema info to GSM.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationName Operation name, useful for diagnostics.
   * @param schemaInfo Schema info to update.
   * @return The store operation.
   */
  IStoreOperationGlobal createUpdateShardingSchemaInfoGlobalOperation(
      ShardMapManager shardMapManager, String operationName, StoreSchemaInfo schemaInfo);

  /**
   * Constructs request to get shard with specific location for given shard map from GSM.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationName Operation name, useful for diagnostics.
   * @param shardMap Shard map for which shard is being requested.
   * @param location Location of shard being searched.
   * @return The store operation.
   */
  IStoreOperationGlobal createFindShardByLocationGlobalOperation(ShardMapManager shardMapManager,
      String operationName, StoreShardMap shardMap, ShardLocation location);

  /**
   * Constructs request to get all shards for given shard map from GSM.
   *
   * @param operationName Operation name, useful for diagnostics.
   * @param shardMapManager Shard map manager object.
   * @param shardMap Shard map for which shards are being requested.
   * @return The store operation.
   */
  IStoreOperationGlobal createGetShardsGlobalOperation(String operationName,
      ShardMapManager shardMapManager, StoreShardMap shardMap);

  /**
   * Constructs request to add given shard map to GSM.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationName Operation name, useful for diagnostics.
   * @param shardMap Shard map to add.
   * @return The store operation.
   */
  IStoreOperationGlobal createAddShardMapGlobalOperation(ShardMapManager shardMapManager,
      String operationName, StoreShardMap shardMap);

  /**
   * Constructs request to find shard map with given name from GSM.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationName Operation name, useful for diagnostics.
   * @param shardMapName Name of the shard map being searched.
   * @return The store operation.
   */
  IStoreOperationGlobal createFindShardMapByNameGlobalOperation(ShardMapManager shardMapManager,
      String operationName, String shardMapName);

  /**
   * Constructs request to get distinct shard locations from GSM.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationName Operation name, useful for diagnostics.
   * @return The store operation.
   */
  IStoreOperationGlobal createGetDistinctShardLocationsGlobalOperation(
      ShardMapManager shardMapManager, String operationName);

  /**
   * Constructs request to get all shard maps from GSM.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationName Operation name, useful for diagnostics.
   * @return The store operation.
   */
  IStoreOperationGlobal createGetShardMapsGlobalOperation(ShardMapManager shardMapManager,
      String operationName);

  /**
   * Constructs request to get all shard maps from GSM.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationName Operation name, useful for diagnostics.
   * @return The store operation.
   */
  IStoreOperationGlobal createLoadShardMapManagerGlobalOperation(ShardMapManager shardMapManager,
      String operationName);

  /**
   * Constructs request to remove given shard map from GSM.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationName Operation name, useful for diagnostics.
   * @param shardMap Shard map to remove.
   * @return The store operation.
   */
  IStoreOperationGlobal createRemoveShardMapGlobalOperation(ShardMapManager shardMapManager,
      String operationName, StoreShardMap shardMap);

  /**
   * Constructs request for obtaining mapping from GSM based on given key.
   *
   * @param shardMapManager Shard map manager.
   * @param operationName Operation being executed.
   * @param shardMap Local shard map.
   * @param mapping Mapping whose Id will be used.
   * @param errorCategory Error category.
   * @return The store operation.
   */
  IStoreOperationGlobal createFindMappingByIdGlobalOperation(ShardMapManager shardMapManager,
      String operationName, StoreShardMap shardMap, StoreMapping mapping,
      ShardManagementErrorCategory errorCategory);

  /**
   * Constructs request for obtaining mapping from GSM based on given key.
   *
   * @param shardMapManager Shard map manager.
   * @param operationName Operation being executed.
   * @param shardMap Local shard map.
   * @param key Key for lookup operation.
   * @param policy Policy for cache update.
   * @param errorCategory Error category.
   * @param cacheResults Whether to cache the results of the operation.
   * @param ignoreFailure Ignore shard map not found error.
   * @return The store operation.
   */
  IStoreOperationGlobal createFindMappingByKeyGlobalOperation(ShardMapManager shardMapManager,
      String operationName, StoreShardMap shardMap, ShardKey key,
      CacheStoreMappingUpdatePolicy policy, ShardManagementErrorCategory errorCategory,
      boolean cacheResults, boolean ignoreFailure);

  /**
   * Constructs request for obtaining all the mappings from GSM based on given shard and mappings.
   *
   * @param shardMapManager Shard map manager.
   * @param operationName Operation being executed.
   * @param shardMap Local shard map.
   * @param shard Local shard.
   * @param range Optional range to get mappings from.
   * @param errorCategory Error category.
   * @param cacheResults Whether to cache the results of the operation.
   * @param ignoreFailure Ignore shard map not found error.
   * @return The store operation.
   */
  IStoreOperationGlobal createGetMappingsByRangeGlobalOperation(ShardMapManager shardMapManager,
      String operationName, StoreShardMap shardMap, StoreShard shard, ShardRange range,
      ShardManagementErrorCategory errorCategory, boolean cacheResults, boolean ignoreFailure);

  /**
   * Constructs request to lock or unlock given mappings in GSM.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationName Operation name, useful for diagnostics.
   * @param shardMap Shard map to add.
   * @param mapping Mapping to lock or unlock. Null means all mappings.
   * @param lockOwnerId Lock owner.
   * @param lockOpType Lock operation type.
   * @param errorCategory Error category.
   * @return The store operation.
   */
  IStoreOperationGlobal createLockOrUnLockMappingsGlobalOperation(ShardMapManager shardMapManager,
      String operationName, StoreShardMap shardMap, StoreMapping mapping, UUID lockOwnerId,
      LockOwnerIdOpType lockOpType, ShardManagementErrorCategory errorCategory);

  /**
   * Constructs a request to upgrade global store.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationName Operation name, useful for diagnostics.
   * @param targetVersion Target version of store to deploy.
   * @return The store operation.
   */
  IStoreOperationGlobal createUpgradeStoreGlobalOperation(ShardMapManager shardMapManager,
      String operationName, Version targetVersion);

  ///#endregion Global Operations

  ///#region Local Operations

  /**
   * Constructs request for obtaining all the shard maps and shards from an LSM.
   *
   * @param operationName Operation name.
   * @param shardMapManager Shard map manager.
   * @param location Location of the LSM.
   */
  IStoreOperationLocal createCheckShardLocalOperation(String operationName,
      ShardMapManager shardMapManager, ShardLocation location);

  /**
   * Constructs request for obtaining all the shard maps and shards from an LSM.
   *
   * @param shardMapManager Shard map manager.
   * @param location Location of the LSM.
   * @param operationName Operation name.
   * @param shardMap Local shard map.
   * @param shard Local shard.
   * @param range Optional range to get mappings from.
   * @param ignoreFailure Ignore shard map not found error.
   */
  IStoreOperationLocal createGetMappingsByRangeLocalOperation(ShardMapManager shardMapManager,
      ShardLocation location, String operationName, StoreShardMap shardMap, StoreShard shard,
      ShardRange range, boolean ignoreFailure);

  /**
   * Constructs request for obtaining all the shard maps and shards from an LSM.
   *
   * @param shardMapManager Shard map manager.
   * @param location Location of the LSM.
   * @param operationName Operatio name.
   */
  IStoreOperationLocal createGetShardsLocalOperation(ShardMapManager shardMapManager,
      ShardLocation location, String operationName);

  /**
   * Constructs request for replacing the LSM mappings for given shard map with the input mappings.
   *
   * @param shardMapManager Shard map manager.
   * @param location Location of the LSM.
   * @param operationName Operation name.
   * @param shardMap Local shard map.
   * @param shard Local shard.
   * @param rangesToRemove Optional list of ranges to minimize amount of deletions.
   * @param mappingsToAdd List of mappings to add.
   */
  IStoreOperationLocal createReplaceMappingsLocalOperation(ShardMapManager shardMapManager,
      ShardLocation location, String operationName, StoreShardMap shardMap, StoreShard shard,
      List<ShardRange> rangesToRemove, List<StoreMapping> mappingsToAdd);

  /**
   * Constructs a request to upgrade store location.
   *
   * @param shardMapManager Shard map manager object.
   * @param location Location to upgrade.
   * @param operationName Operation name, useful for diagnostics.
   * @param targetVersion Target version of store to deploy.
   * @return The store operation.
   */
  IStoreOperationLocal createUpgradeStoreLocalOperation(ShardMapManager shardMapManager,
      ShardLocation location, String operationName, Version targetVersion);

  ///#endregion LocalOperations

  ///#region Global And Local Operations

  /**
   * Creates request to add shard to given shard map.
   *
   * @param shardMapManager Shard map manager object.
   * @param shardMap Shard map for which to add shard.
   * @param shard Shard to add.
   * @return The store operation.
   */
  IStoreOperation createAddShardOperation(ShardMapManager shardMapManager, StoreShardMap shardMap,
      StoreShard shard);

  /**
   * Creates request to add shard to given shard map.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationId Operation Id.
   * @param undoStartState Undo start state.
   * @param root Xml representation of the object.
   * @return The store operation.
   */
  IStoreOperation createAddShardOperation(ShardMapManager shardMapManager, UUID operationId,
      StoreOperationState undoStartState, Object root);

  /**
   * Creates request to remove shard from given shard map.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationId Operation Id.
   * @param undoStartState Undo start state.
   * @param root Xml representation of the object.
   * @return The store operation.
   */
  IStoreOperation createRemoveShardOperation(ShardMapManager shardMapManager, UUID operationId,
      StoreOperationState undoStartState, Object root);

  /**
   * Creates request to remove shard from given shard map.
   *
   * @param shardMapManager Shard map manager object.
   * @param shardMap Shard map for which to remove shard.
   * @param shard Shard to remove.
   * @return The store operation.
   */
  IStoreOperation createRemoveShardOperation(ShardMapManager shardMapManager,
      StoreShardMap shardMap, StoreShard shard);

  /**
   * Creates request to update shard in given shard map.
   *
   * @param shardMapManager Shard map manager object.
   * @param shardMap Shard map for which to remove shard.
   * @param shardOld Shard to update.
   * @param shardNew Updated shard.
   * @return The store operation.
   */
  IStoreOperation createUpdateShardOperation(ShardMapManager shardMapManager,
      StoreShardMap shardMap, StoreShard shardOld, StoreShard shardNew);

  /**
   * Creates request to update shard in given shard map.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationId Operation Id.
   * @param undoStartState Undo start state.
   * @param root Xml representation of the object.
   * @return The store operation.
   */
  IStoreOperation createUpdateShardOperation(ShardMapManager shardMapManager, UUID operationId,
      StoreOperationState undoStartState, Object root);

  /**
   * Creates request to add a mapping in given shard map.
   *
   * @param operationCode Operation code.
   * @param shardMapManager Shard map manager object.
   * @param operationId Operation Id.
   * @param undoStartState Undo start state.
   * @param root Xml representation of the object.
   * @param shardIdOriginal Original shard Id.
   * @return The store operation.
   */
  IStoreOperation createAddMappingOperation(StoreOperationCode operationCode,
      ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState,
      Object root, UUID shardIdOriginal);

  /**
   * Creates request to add shard to given shard map.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationCode Store operation code.
   * @param shardMap Shard map for which to add mapping.
   * @param mapping Mapping to add.
   * @return The store operation.
   */
  IStoreOperation createAddMappingOperation(ShardMapManager shardMapManager,
      StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping mapping);

  /**
   * Constructs request for attaching the given shard map and shard information to the GSM database.
   *
   * @param shardMapManager Shard map manager object.
   * @param shard Shard to attach
   * @param shardMap Shard Map to attach shard to.
   * @return The store operation.
   */
  IStoreOperation createAttachShardOperation(ShardMapManager shardMapManager,
      StoreShardMap shardMap, StoreShard shard);

  /**
   * Creates request to add shard to given shard map.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationCode Store operation code.
   * @param shardMap Shard map from which to remove mapping.
   * @param mapping Mapping to add.
   * @param lockOwnerId Id of lock owner.
   * @return The store operation.
   */
  IStoreOperation createRemoveMappingOperation(ShardMapManager shardMapManager,
      StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping mapping,
      UUID lockOwnerId);

  /**
   * Creates request to remove a mapping in given shard map.
   *
   * @param operationCode Operation code.
   * @param shardMapManager Shard map manager object.
   * @param operationId Operation Id.
   * @param undoStartState Undo start state.
   * @param root Xml representation of the object.
   * @param shardIdOriginal Original shard Id.
   * @return The store operation.
   */
  IStoreOperation createRemoveMappingOperation(StoreOperationCode operationCode,
      ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState,
      Object root, UUID shardIdOriginal);

  /**
   * Creates request to update a mapping in given shard map.
   *
   * @param operationCode Operation code.
   * @param shardMapManager Shard map manager object.
   * @param operationId Operation Id.
   * @param undoStartState Undo start state.
   * @param root Xml representation of the object.
   * @param shardIdOriginalSource Original shard Id of source.
   * @param shardIdOriginalTarget Original shard Id of target.
   * @return The store operation.
   */
  IStoreOperation createUpdateMappingOperation(StoreOperationCode operationCode,
      ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState,
      Object root, UUID shardIdOriginalSource, UUID shardIdOriginalTarget);

  /**
   * Creates request to add shard to given shard map.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationCode Store operation code.
   * @param shardMap Shard map for which to update mapping.
   * @param mappingSource Mapping to update.
   * @param mappingTarget Updated mapping.
   * @param patternForKill Pattern for kill commands.
   * @param lockOwnerId Id of lock owner.
   * @return The store operation.
   */
  IStoreOperation createUpdateMappingOperation(ShardMapManager shardMapManager,
      StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping mappingSource,
      StoreMapping mappingTarget, String patternForKill, UUID lockOwnerId);

  /**
   * Creates request to replace mappings within shard map.
   *
   * @param shardMapManager Shard map manager object.
   * @param operationCode Store operation code.
   * @param shardMap Shard map for which to update mapping.
   * @param mappingsSource Original mappings.
   * @param mappingsTarget Target mappings mapping.
   * @return The store operation.
   */
  IStoreOperation createReplaceMappingsOperation(ShardMapManager shardMapManager,
      StoreOperationCode operationCode, StoreShardMap shardMap,
      Pair<StoreMapping, UUID>[] mappingsSource, Pair<StoreMapping, UUID>[] mappingsTarget);

  /**
   * Creates request to replace a set of mappings with new set in given shard map.
   *
   * @param operationCode Operation code.
   * @param shardMapManager Shard map manager object.
   * @param operationId Operation Id.
   * @param undoStartState Undo start state.
   * @param root Xml representation of the object.
   * @param shardIdOriginalSource Original shard Id of source.
   * @return The store operation.
   */
  IStoreOperation createReplaceMappingsOperation(StoreOperationCode operationCode,
      ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState,
      Object root, UUID shardIdOriginalSource);

  /**
   * Create operation corresponding to the <see cref="StoreLogEntry"/> information.
   *
   * @param shardMapManager ShardMapManager instance for undo operation.
   * @param so Store operation information.
   * @return The store operation.
   */
  IStoreOperation fromLogEntry(ShardMapManager shardMapManager, StoreLogEntry so);

  ///#endregion Global And Local Operations
}