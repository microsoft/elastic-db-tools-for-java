package com.microsoft.azure.elasticdb.shard.storeops.base;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.base.LockOwnerIdOpType;
import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreSchemaInfo;
import com.microsoft.azure.elasticdb.shard.store.StoreShard;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.namespace.QName;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Constructs requests for store operations.
 */
public final class StoreOperationRequestBuilder {

  ///#region GSM Stored Procedures

  /**
   * FindAndUpdateOperationLogEntryByIdGlobal stored procedure.
   */
  public static final String SP_FIND_AND_UPDATE_OPERATION_LOG_ENTRY_BY_ID_GLOBAL =
      "__ShardManagement.spFindAndUpdateOperationLogEntryByIdGlobal";

  /**
   * GetAllShardMapsGlobal stored procedure.
   */
  public static final String SP_GET_ALL_SHARD_MAPS_GLOBAL =
      "__ShardManagement.spGetAllShardMapsGlobal";

  /**
   * FindShardMapByNameGlobal stored procedure.
   */
  public static final String SP_FIND_SHARD_MAP_BY_NAME_GLOBAL =
      "__ShardManagement.spFindShardMapByNameGlobal";

  /**
   * GetAllDistinctShardLocationsGlobal stored procedure.
   */
  public static final String SP_GET_ALL_DISTINCT_SHARD_LOCATIONS_GLOBAL =
      "__ShardManagement.spGetAllDistinctShardLocationsGlobal";

  /**
   * AddShardMapGlobal stored procedure.
   */
  public static final String SP_ADD_SHARD_MAP_GLOBAL =
      "__ShardManagement.spAddShardMapGlobal";

  /**
   * RemoveShardMapGlobal stored procedure.
   */
  public static final String SP_REMOVE_SHARD_MAP_GLOBAL =
      "__ShardManagement.spRemoveShardMapGlobal";

  /**
   * GetAllShardsGlobal stored procedure.
   */
  public static final String SP_GET_ALL_SHARDS_GLOBAL =
      "__ShardManagement.spGetAllShardsGlobal";

  /**
   * FindShardByLocationGlobal stored procedure.
   */
  public static final String SP_FIND_SHARD_BY_LOCATION_GLOBAL =
      "__ShardManagement.spFindShardByLocationGlobal";

  /**
   * BulkOperationShardsGlobalBegin stored procedure.
   */
  public static final String SP_BULK_OPERATION_SHARDS_GLOBAL_BEGIN =
      "__ShardManagement.spBulkOperationShardsGlobalBegin";

  /**
   * BulkOperationShardsGlobalEnd stored procedure.
   */
  public static final String SP_BULK_OPERATION_SHARDS_GLOBAL_END =
      "__ShardManagement.spBulkOperationShardsGlobalEnd";

  /**
   * GetAllShardMappingsGlobal stored procedure.
   */
  public static final String SP_GET_ALL_SHARD_MAPPINGS_GLOBAL =
      "__ShardManagement.spGetAllShardMappingsGlobal";

  /**
   * FindShardMappingByKeyGlobal stored procedure.
   */
  public static final String SP_FIND_SHARD_MAPPING_BY_KEY_GLOBAL =
      "__ShardManagement.spFindShardMappingByKeyGlobal";

  /**
   * FindShardMappingByIdGlobal stored procedure.
   */
  public static final String SP_FIND_SHARD_MAPPING_BY_ID_GLOBAL =
      "__ShardManagement.spFindShardMappingByIdGlobal";

  /**
   * BulkShardMappingOperationsGlobalBegin stored procedure.
   */
  public static final String SP_BULK_OPERATION_SHARD_MAPPINGS_GLOBAL_BEGIN =
      "__ShardManagement.spBulkOperationShardMappingsGlobalBegin";

  /**
   * BulkShardMappingOperationsGlobalEnd stored procedure.
   */
  public static final String SP_BULK_OPERATION_SHARD_MAPPINGS_GLOBAL_END =
      "__ShardManagement.spBulkOperationShardMappingsGlobalEnd";

  /**
   * LockOrUnLockShardMappingsGlobal stored procedure.
   */
  public static final String SP_LOCK_OR_UN_LOCK_SHARD_MAPPINGS_GLOBAL =
      "__ShardManagement.spLockOrUnlockShardMappingsGlobal";

  /**
   * GetAllShardingSchemaInfosGlobal stored procedure.
   */
  public static final String SP_GET_ALL_SHARDING_SCHEMA_INFOS_GLOBAL =
      "__ShardManagement.spGetAllShardingSchemaInfosGlobal";

  /**
   * FindShardingSchemaInfoByNameGlobal stored procedure.
   */
  public static final String SP_FIND_SHARDING_SCHEMA_INFO_BY_NAME_GLOBAL =
      "__ShardManagement.spFindShardingSchemaInfoByNameGlobal";

  /**
   * AddShardingSchemaInfoGlobal stored procedure.
   */
  public static final String SP_ADD_SHARDING_SCHEMA_INFO_GLOBAL =
      "__ShardManagement.spAddShardingSchemaInfoGlobal";

  /**
   * RemoveShardingSchemaInfoGlobal stored procedure.
   */
  public static final String SP_REMOVE_SHARDING_SCHEMA_INFO_GLOBAL =
      "__ShardManagement.spRemoveShardingSchemaInfoGlobal";

  /**
   * UpdateShardingSchemaInfoGlobal stored procedure.
   */
  public static final String SP_UPDATE_SHARDING_SCHEMA_INFO_GLOBAL =
      "__ShardManagement.spUpdateShardingSchemaInfoGlobal";

  /**
   * AttachShardGlobal stored procedure.
   */
  public static final String SP_ATTACH_SHARD_GLOBAL = "__ShardManagement.spAttachShardGlobal";

  /**
   * DetachShardGlobal stored procedure.
   */
  public static final String SP_DETACH_SHARD_GLOBAL = "__ShardManagement.spDetachShardGlobal";

  /**
   * ReplaceShardMappingsGlobal stored procedure.
   */
  public static final String SP_REPLACE_SHARD_MAPPINGS_GLOBAL =
      "__ShardManagement.spReplaceShardMappingsGlobal";

  ///#endregion GSM Stored Procedures

  ///#region LSM Stored Procedures
  /**
   * GetAllShardsLocal stored procedure.
   */
  public static final String SP_GET_ALL_SHARDS_LOCAL = "__ShardManagement.spGetAllShardsLocal";

  /**
   * ValidateShardLocal stored procedure.
   */
  public static final String SP_VALIDATE_SHARD_LOCAL = "__ShardManagement.spValidateShardLocal";

  /**
   * AddShardLocal stored procedure.
   */
  public static final String SP_ADD_SHARD_LOCAL = "__ShardManagement.spAddShardLocal";

  /**
   * RemoveShardLocal stored procedure.
   */
  public static final String SP_REMOVE_SHARD_LOCAL = "__ShardManagement.spRemoveShardLocal";

  /**
   * UpdateShardLocal stored procedure.
   */
  public static final String SP_UPDATE_SHARD_LOCAL = "__ShardManagement.spUpdateShardLocal";

  /**
   * GetAllShardMappingsLocal stored procedure.
   */
  public static final String SP_GET_ALL_SHARD_MAPPINGS_LOCAL =
      "__ShardManagement.spGetAllShardMappingsLocal";

  /**
   * FindShardMappingByKeyLocal stored procedure.
   */
  public static final String SP_FIND_SHARD_MAPPING_BY_KEY_LOCAL =
      "__ShardManagement.spFindShardMappingByKeyLocal";

  /**
   * ValidateShardMappingLocal stored procedure.
   */
  public static final String SP_VALIDATE_SHARD_MAPPING_LOCAL =
      "__ShardManagement.spValidateShardMappingLocal";

  /**
   * BulkOperationShardMappingsLocal stored procedure.
   */
  public static final String SP_BULK_OPERATION_SHARD_MAPPINGS_LOCAL =
      "__ShardManagement.spBulkOperationShardMappingsLocal";

  /**
   * KillSessionsForShardMappingLocal stored procedure.
   */
  public static final String SP_KILL_SESSIONS_FOR_SHARD_MAPPING_LOCAL =
      "__ShardManagement.spKillSessionsForShardMappingLocal";

  ///#endregion LSM Stored Procedures

  ///#region Methods

  /**
   * Find operation log entry by Id from GSM.
   *
   * @param operationId Operation Id.
   * @param undoStartState Minimum start from which to start undo operation.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> findAndUpdateOperationLogEntryByIdGlobal(
      UUID operationId, StoreOperationState undoStartState) {
    QName rootElementName = new QName("FindAndUpdateOperationLogEntryByIdGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withOperationId(operationId)
        .withUndoStartState(undoStartState)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to get all shard maps from GSM.
   *
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> getAllShardMapsGlobal() {
    QName rootElementName = new QName("GetAllShardMapsGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to get shard map with the given name from GSM.
   *
   * @param shardMapName Name of shard map.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> findShardMapByNameGlobal(String shardMapName) {
    QName rootElementName = new QName("FindShardMapByNameGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withShardMap(new StoreShardMap(null, shardMapName, null, null))
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to get all distinct shard locations from GSM.
   *
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> getAllDistinctShardLocationsGlobal() {
    QName rootElementName = new QName("GetAllDistinctShardLocationsGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to add shard map to GSM.
   *
   * @param shardMap ShardId map to add.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> addShardMapGlobal(StoreShardMap shardMap) {
    QName rootElementName = new QName("AddShardMapGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to remove shard map from GSM.
   *
   * @param shardMap ShardId map to remove.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> removeShardMapGlobal(StoreShardMap shardMap) {
    QName rootElementName = new QName("RemoveShardMapGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to get all shards for a shard map from GSM.
   *
   * @param shardMap ShardId map for which to get all shards.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> getAllShardsGlobal(StoreShardMap shardMap) {
    QName rootElementName = new QName("GetAllShardsGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to get shard with specified location for a shard map from GSM.
   *
   * @param shardMap ShardId map for which to get shard.
   * @param location Location for which to find shard.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> findShardByLocationGlobal(StoreShardMap shardMap,
      ShardLocation location) {
    QName rootElementName = new QName("FindShardByLocationGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withLocation(location)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to add shard to given shard map in GSM.
   *
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> addShardGlobal(UUID operationId,
      StoreOperationCode operationCode, boolean undo, StoreShardMap shardMap, StoreShard shard) {
    List<StoreOperationInput> steps = new ArrayList<>();
    steps.add(new StoreOperationInput.Builder()
        .withStepId(1)
        .withStoreOperationStepKind(StoreOperationStepKind.Add)
        .withShard(shard == null ? StoreShard.NULL : shard)
        .build());

    QName rootElementName = new QName("BulkOperationShardsGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withOperationId(operationId)
        .withOperationCode(operationCode)
        .withUndo(undo)
        .withStepsCount(1)
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withSteps(steps)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to remove shard from given shard map in GSM.
   *
   * @param operationId Operation Id
   * @param operationCode Operation code.
   * @param undo Whether this is an undo request.
   * @param shardMap ShardId map for which operation is being requested.
   * @param shard ShardId to remove.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> removeShardGlobal(UUID operationId,
      StoreOperationCode operationCode, boolean undo, StoreShardMap shardMap, StoreShard shard) {
    List<StoreOperationInput> steps = new ArrayList<>();
    steps.add(new StoreOperationInput.Builder()
        .withStepId(1)
        .withStoreOperationStepKind(StoreOperationStepKind.Remove)
        .withShard(shard == null ? StoreShard.NULL : shard)
        .build());

    QName rootElementName = new QName("BulkOperationShardsGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withOperationId(operationId)
        .withOperationCode(operationCode)
        .withUndo(undo)
        .withStepsCount(1)
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withSteps(steps)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to update shard in given shard map in GSM.
   *
   * @param operationId Operation Id
   * @param operationCode Operation code.
   * @param undo Whether this is an undo request.
   * @param shardMap ShardId map for which operation is being requested.
   * @param shardOld ShardId to update.
   * @param shardNew Updated shard.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> updateShardGlobal(UUID operationId,
      StoreOperationCode operationCode, boolean undo, StoreShardMap shardMap, StoreShard shardOld,
      StoreShard shardNew) {
    StoreOperationInput innerInput = new StoreOperationInput.Builder().withShard(shardNew).build();

    List<StoreOperationInput> steps = new ArrayList<>();
    steps.add(new StoreOperationInput.Builder()
        .withStepId(1)
        .withStoreOperationStepKind(StoreOperationStepKind.Update)
        .withShard(shardOld == null ? StoreShard.NULL : shardOld)
        .withUpdate(innerInput)
        .build());

    QName rootElementName = new QName("BulkOperationShardsGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withOperationId(operationId)
        .withOperationCode(operationCode)
        .withUndo(undo)
        .withStepsCount(1)
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withSteps(steps)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to get all shard mappings from GSM for a particular shard map
   * and optional shard and range.
   *
   * @param shardMap ShardId map whose mappings are being requested.
   * @param shard Optional shard for which mappings are being requested.
   * @param range Optional range for which mappings are being requested.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> getAllShardMappingsGlobal(StoreShardMap shardMap,
      StoreShard shard, ShardRange range) {
    QName rootElementName = new QName("GetAllShardMappingsGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withShard(shard == null ? StoreShard.NULL : shard)
        .withShardRange(range == null ? ShardRange.NULL : range)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to get mapping from GSM for a particular key belonging to a shard map.
   *
   * @param shardMap ShardId map whose mappings are being requested.
   * @param key Key being searched.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> findShardMappingByKeyGlobal(StoreShardMap shardMap,
      ShardKey key) {
    QName rootElementName = new QName("FindShardMappingByKeyGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withShardKey(key)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to get mapping from GSM for a particular mapping Id.
   *
   * @param shardMap ShardId map whose mappings are being requested.
   * @param mapping Mapping to look up.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> findShardMappingByIdGlobal(StoreShardMap shardMap,
      StoreMapping mapping) {
    QName rootElementName = new QName("FindShardMappingByIdGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withMapping(mapping)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to add shard to given shard map in GSM.
   *
   * @param operationId Operation Id.
   * @param operationCode Operation code.
   * @param undo Whether this is an undo request.
   * @param shardMap ShardId map for which operation is being requested.
   * @param mapping Mapping to add.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> addShardMappingGlobal(UUID operationId,
      StoreOperationCode operationCode, boolean undo, StoreShardMap shardMap,
      StoreMapping mapping) {
    StoreOperationInput innerInput = new StoreOperationInput.Builder()
        .withShard(mapping.getStoreShard()).build();

    List<StoreOperationInput> steps = new ArrayList<>();
    steps.add(new StoreOperationInput.Builder()
        .withStepId(1)
        .withValidation(true)
        .withStoreOperationStepKind(StoreOperationStepKind.Add)
        .withMapping(mapping)
        .build());

    QName rootElementName = new QName("BulkOperationShardMappingsGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withOperationId(operationId)
        .withOperationCode(operationCode)
        .withUndo(undo)
        .withStepsCount(1)
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withRemoves(innerInput)
        .withAdds(innerInput)
        .withSteps(steps)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to remove shard from given shard map in GSM.
   *
   * @param operationId Operation Id.
   * @param operationCode Operation code.
   * @param undo Whether this is an undo request.
   * @param shardMap ShardId map for which operation is being requested.
   * @param mapping Mapping to remove.
   * @param lockOwnerId Lock owner.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> removeShardMappingGlobal(UUID operationId,
      StoreOperationCode operationCode, boolean undo, StoreShardMap shardMap, StoreMapping mapping,
      UUID lockOwnerId) {
    StoreOperationInput innerInput = new StoreOperationInput.Builder()
        .withShard(mapping.getStoreShard()).build();

    List<StoreOperationInput> steps = new ArrayList<>();
    steps.add(new StoreOperationInput.Builder()
        .withStepId(1)
        .withValidation(false)
        .withStoreOperationStepKind(StoreOperationStepKind.Remove)
        .withMapping(mapping)
        .withLock(new Lock(lockOwnerId == null ? new UUID(0L, 0L) : lockOwnerId, 0))
        .build());

    QName rootElementName = new QName("BulkOperationShardMappingsGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withOperationId(operationId)
        .withStoreOperationCode(operationCode)
        .withUndo(undo)
        .withStepsCount(1)
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withRemoves(innerInput)
        .withAdds(innerInput)
        .withSteps(steps)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to update mapping in given shard map in GSM.
   *
   * @param operationId Operation Id.
   * @param operationCode Operation code.
   * @param undo Whether this is an undo request.
   * @param patternForKill Pattern to use for kill connection.
   * @param shardMap ShardId map for which operation is being requested.
   * @param mappingSource ShardId to update.
   * @param mappingTarget Updated shard.
   * @param lockOwnerId Lock owner.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> updateShardMappingGlobal(UUID operationId,
      StoreOperationCode operationCode, boolean undo, String patternForKill, StoreShardMap shardMap,
      StoreMapping mappingSource, StoreMapping mappingTarget, UUID lockOwnerId) {
    StoreOperationInput innerInputMapping = new StoreOperationInput.Builder()
        .withMapping(mappingTarget).build();

    StoreOperationInput innerInputRemoves = new StoreOperationInput.Builder()
        .withShard(mappingSource.getStoreShard()).build();

    StoreOperationInput innerInputAdds = new StoreOperationInput.Builder()
        .withShard(mappingTarget.getStoreShard()).build();

    List<StoreOperationInput> steps = new ArrayList<>();
    steps.add(new StoreOperationInput.Builder()
        .withStepId(1)
        .withValidation(false)
        .withStoreOperationStepKind(StoreOperationStepKind.Update)
        .withMapping(mappingSource)
        .withLock(new Lock(lockOwnerId == null ? new UUID(0L, 0L) : lockOwnerId, 0))
        .withUpdate(innerInputMapping)
        .build());

    QName rootElementName = new QName("BulkOperationShardMappingsGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withOperationId(operationId)
        .withStoreOperationCode(operationCode)
        .withUndo(undo)
        .withStepsCount(1)
        .withPatternForKill(patternForKill)
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withRemoves(innerInputRemoves)
        .withAdds(innerInputAdds)
        .withSteps(steps)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to replace mappings in given shard map in GSM.
   *
   * @param operationId Operation Id.
   * @param operationCode Operation code.
   * @param undo Whether this is an undo request.
   * @param shardMap ShardId map for which operation is being requested.
   * @param mappingsSource Original mappings.
   * @param mappingsTarget New mappings.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> replaceShardMappingsGlobal(UUID operationId,
      StoreOperationCode operationCode, boolean undo, StoreShardMap shardMap,
      List<Pair<StoreMapping, UUID>> mappingsSource,
      List<Pair<StoreMapping, UUID>> mappingsTarget) {

    if (mappingsSource.size() > 0 && mappingsTarget.size() > 0) {
      StoreOperationInput innerInputRemoves = new StoreOperationInput.Builder()
          .withShard(mappingsSource.get(0).getLeft().getStoreShard())
          .build();

      StoreOperationInput innerInputAdds = new StoreOperationInput.Builder()
          .withShard(mappingsTarget.get(0).getLeft().getStoreShard())
          .build();

      List<StoreOperationInput> steps = new ArrayList<>();
      int id = 0;
      for (Pair<StoreMapping, UUID> pair : mappingsSource) {
        id++;
        steps.add(new StoreOperationInput.Builder()
            .withStepId(id)
            .withValidation(false)
            .withStoreOperationStepKind(StoreOperationStepKind.Remove)
            .withMapping(pair.getLeft())
            .withLock(new Lock(pair.getRight(), 0))
            .build());
      }
      for (Pair<StoreMapping, UUID> pair : mappingsTarget) {
        id++;
        steps.add(new StoreOperationInput.Builder()
            .withStepId(id)
            .withValidation(false)
            .withStoreOperationStepKind(StoreOperationStepKind.Add)
            .withMapping(pair.getLeft())
            .withLock(new Lock(pair.getRight(), 0))
            .build());
      }

      QName rootElementName = new QName("BulkOperationShardMappingsGlobal");
      StoreOperationInput input = new StoreOperationInput.Builder()
          .withOperationId(operationId)
          .withOperationCode(operationCode)
          .withUndo(undo)
          .withStepsCount(id)
          .withGsmVersion()
          .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
          .withRemoves(innerInputRemoves)
          .withAdds(innerInputAdds)
          .withSteps(steps)
          .build();
      return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
    }

    return null;
  }

  /**
   * Request to lock or unlock mappings in GSM.
   *
   * @param shardMap ShardId map whose mappings are being requested.
   * @param mapping Mapping being locked or unlocked.
   * @param lockId Lock Id.
   * @param lockOpType Lock operation code.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> lockOrUnLockShardMappingsGlobal(
      StoreShardMap shardMap, StoreMapping mapping, UUID lockId, LockOwnerIdOpType lockOpType) {

    if (mapping != null || (lockOpType == LockOwnerIdOpType.UnlockAllMappingsForId
        || lockOpType == LockOwnerIdOpType.UnlockAllMappings)) {
      Lock lock = new Lock(lockId, lockOpType.getValue());
      QName rootElementName = new QName("LockOrUnlockShardMappingsGlobal");
      StoreOperationInput input = new StoreOperationInput.Builder()
          .withGsmVersion()
          .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
          .withMapping(mapping)
          .withLock(lock)
          .build();
      return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
    }

    return null;
  }

  /**
   * Request to get all schema info objects from GSM.
   *
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> getAllShardingSchemaInfosGlobal() {
    QName rootElementName = new QName("GetAllShardingSchemaInfosGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to find schema info in GSM.
   *
   * @param name Schema info name to find.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> findShardingSchemaInfoGlobal(String name) {
    QName rootElementName = new QName("FindShardingSchemaInfoGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withSchemaInfo(new StoreSchemaInfo(name, null))
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to add schema info to GSM.
   *
   * @param schemaInfo Schema info object to add
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> addShardingSchemaInfoGlobal(
      StoreSchemaInfo schemaInfo) {
    QName rootElementName = new QName("AddShardingSchemaInfoGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withSchemaInfo(schemaInfo)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to delete schema info object from GSM.
   *
   * @param name Name of schema info to delete.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> removeShardingSchemaInfoGlobal(String name) {
    QName rootElementName = new QName("RemoveShardingSchemaInfoGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withSchemaInfo(new StoreSchemaInfo(name, null))
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to update schema info to GSM.
   *
   * @param schemaInfo Schema info object to update
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> updateShardingSchemaInfoGlobal(
      StoreSchemaInfo schemaInfo) {
    QName rootElementName = new QName("UpdateShardingSchemaInfoGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withSchemaInfo(schemaInfo)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to attach shard to GSM.
   *
   * @param shardMap ShardId map to attach.
   * @param shard ShardId to attach.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> attachShardGlobal(StoreShardMap shardMap,
      StoreShard shard) {
    QName rootElementName = new QName("AttachShardGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withShard(shard == null ? StoreShard.NULL : shard)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to detach shard to GSM.
   *
   * @param shardMapName Optional shard map name to detach.
   * @param location Location to detach.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> detachShardGlobal(String shardMapName,
      ShardLocation location) {
    QName rootElementName = new QName("DetachShardGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withShardMap(shardMapName == null ? StoreShardMap.NULL
            : new StoreShardMap(null, shardMapName, null, null))
        .withLocation(location)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to replace mappings in given shard map in GSM without logging.
   *
   * @param shardMap ShardId map for which operation is being requested.
   * @param mappingsSource Original mappings.
   * @param mappingsTarget New mappings.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> replaceShardMappingsGlobalWithoutLogging(
      StoreShardMap shardMap, StoreMapping[] mappingsSource, StoreMapping[] mappingsTarget) {
    // Expecting at least one mapping for ReplaceMappingsGlobalWithoutLogging
    if (mappingsSource.length + mappingsTarget.length > 0) {
      QName rootElementName = new QName("ReplaceShardMappingsGlobal");
      StoreOperationInput input = new StoreOperationInput.Builder()
          .withGsmVersion()
          .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
          .withRemoveStepsCount(mappingsSource.length)
          .withAddStepsCount(mappingsTarget.length)
          .withRemoveSteps(getStepsFromStoreMappings(mappingsSource))
          .withAddSteps(getStepsFromStoreMappings(mappingsTarget))
          .build();
      return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
    }

    return null;
  }

  private static StoreOperationInput getStepsFromStoreMappings(StoreMapping[] mappings) {
    List<StoreOperationInput> steps = new ArrayList<>();
    StoreOperationInput.Builder builder = new StoreOperationInput.Builder();
    if (mappings.length > 0) {
      int stepId = 0;
      for (StoreMapping mapping : mappings) {
        stepId++;
        steps.add(new StoreOperationInput.Builder()
            .withStepId(stepId)
            .withStoreMapping(mapping)
            .build());
      }
      builder.withShard(mappings[0].getStoreShard()).withAddSteps(steps);
    }
    return builder.build();
  }

  /**
   * Request to get all shards and shard maps from LSM.
   *
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> getAllShardsLocal() {
    QName rootElementName = new QName("GetAllShardsLocal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withLsmVersion()
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Validation request for shard for LSM.
   *
   * @param shardMapId ShardId map Id.
   * @param shardId ShardId Id.
   * @param shardVersion ShardId version.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> validateShardLocal(UUID shardMapId, UUID shardId,
      UUID shardVersion) {
    QName rootElementName = new QName("ValidateShardLocal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withLsmVersion()
        .withShardMapId(shardMapId)
        .withShardId(shardId)
        .withShardVersion(shardVersion)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to add shard to given shard map in LSM.
   *
   * @param operationId Operation Id.
   * @param shardMap ShardId map to add shard to.
   * @param shard ShardId to add.
   * @param undo Whether this is undo request.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> addShardLocal(UUID operationId, boolean undo,
      StoreShardMap shardMap, StoreShard shard) {
    QName rootElementName = new QName("AddShardLocal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withLsmVersion()
        .withOperationId(operationId)
        .withUndo(undo)
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withShard(shard == null ? StoreShard.NULL : shard)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to remove shard from given shard map in LSM.
   *
   * @param operationId Operation Id.
   * @param shardMap ShardId map to remove shard from.
   * @param shard ShardId to remove.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> removeShardLocal(UUID operationId,
      StoreShardMap shardMap, StoreShard shard) {
    QName rootElementName = new QName("RemoveShardLocal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withLsmVersion()
        .withOperationId(operationId)
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withShard(shard == null ? StoreShard.NULL : shard)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to update shard in given shard map in LSM.
   *
   * @param operationId Operation Id.
   * @param shardMap ShardId map to remove shard from.
   * @param shard ShardId to update.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> updateShardLocal(UUID operationId,
      StoreShardMap shardMap, StoreShard shard) {
    QName rootElementName = new QName("UpdateShardLocal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withLsmVersion()
        .withOperationId(operationId)
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withShard(shard == null ? StoreShard.NULL : shard)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to get all shard mappings from LSM for a particular shard map
   * and optional shard and range.
   *
   * @param shardMap ShardId map whose mappings are being requested.
   * @param shard Optional shard for which mappings are being requested.
   * @param range Optional range for which mappings are being requested.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> getAllShardMappingsLocal(StoreShardMap shardMap,
      StoreShard shard, ShardRange range) {
    QName rootElementName = new QName("GetAllShardMappingsLocal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withLsmVersion()
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withShard(shard == null ? StoreShard.NULL : shard)
        .withShardRange(range)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to get mapping from LSM for a particular key belonging to a shard map.
   *
   * @param shardMap ShardId map whose mappings are being requested.
   * @param key Key being searched.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> findShardMappingByKeyLocal(StoreShardMap shardMap,
      ShardKey key) {
    QName rootElementName = new QName("FindShardMappingByKeyLocal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withLsmVersion()
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withShardKey(key)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Validation request for shard mapping for LSM.
   *
   * @param shardMapId ShardId map Id.
   * @param mappingId ShardId mapping Id.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> validateShardMappingLocal(UUID shardMapId,
      UUID mappingId) {
    QName rootElementName = new QName("ValidateShardMappingLocal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withLsmVersion()
        .withShardMapId(shardMapId)
        .withMappingId(mappingId)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to add mapping to given shard map in LSM.
   *
   * @param operationId Operation Id.
   * @param shardMap ShardId map for which operation is being requested.
   * @param mapping Mapping to add.
   * @param undo Whether this is undo request.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> addShardMappingLocal(UUID operationId,
      boolean undo, StoreShardMap shardMap, StoreMapping mapping) {
    List<StoreOperationInput> steps = new ArrayList<>();
    steps.add(new StoreOperationInput.Builder()
        .withStepId(1)
        .withStoreOperationStepKind(StoreOperationStepKind.Add)
        .withMapping(mapping)
        .build());

    QName rootElementName = new QName("BulkOperationShardMappingsLocal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withLsmVersion()
        .withOperationId(operationId)
        .withUndo(undo)
        .withStepsCount(1)
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withShard(mapping.getStoreShard())
        .withSteps(steps)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to remove mapping from given shard map in LSM.
   *
   * @param operationId Operation Id.
   * @param shardMap ShardId map for which operation is being requested.
   * @param mapping Mapping to remove.
   * @param undo Whether this is undo operation.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> removeShardMappingLocal(UUID operationId,
      boolean undo, StoreShardMap shardMap, StoreMapping mapping) {
    List<StoreOperationInput> steps = new ArrayList<>();
    steps.add(new StoreOperationInput.Builder()
        .withStepId(1)
        .withStoreOperationStepKind(StoreOperationStepKind.Remove)
        .withMapping(mapping)
        .build());

    QName rootElementName = new QName("BulkOperationShardMappingsLocal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withLsmVersion()
        .withOperationId(operationId)
        .withUndo(undo)
        .withStepsCount(1)
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withShard(mapping.getStoreShard())
        .withSteps(steps)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to update mapping in given shard map in LSM.
   *
   * @param operationId Operation Id.
   * @param shardMap ShardId map for which operation is being requested.
   * @param mappingSource Mapping to update.
   * @param mappingTarget Updated mapping.
   * @param undo Whether this is undo request.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> updateShardMappingLocal(UUID operationId,
      boolean undo, StoreShardMap shardMap, StoreMapping mappingSource,
      StoreMapping mappingTarget) {
    List<StoreOperationInput> steps = new ArrayList<>();
    steps.add(new StoreOperationInput.Builder()
        .withStepId(1)
        .withStoreOperationStepKind(StoreOperationStepKind.Remove)
        .withMapping(mappingSource)
        .build());
    steps.add(new StoreOperationInput.Builder()
        .withStepId(2)
        .withStoreOperationStepKind(StoreOperationStepKind.Add)
        .withMapping(mappingTarget)
        .build());

    QName rootElementName = new QName("BulkOperationShardMappingsLocal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withLsmVersion()
        .withOperationId(operationId)
        .withUndo(undo)
        .withStepsCount(2)
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withShard(mappingTarget.getStoreShard())
        .withSteps(steps)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to replace mapping in given shard map in LSM.
   *
   * @param operationId Operation Id.
   * @param shardMap ShardId map for which operation is being requested.
   * @param mappingsSource Mappings to remove.
   * @param mappingsTarget Mappings to add.
   * @param undo Whether this is undo request.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> replaceShardMappingsLocal(UUID operationId,
      boolean undo, StoreShardMap shardMap, StoreMapping[] mappingsSource,
      StoreMapping[] mappingsTarget) {

    if (mappingsSource.length + mappingsTarget.length > 0) {
      List<StoreOperationInput> steps = new ArrayList<>();
      int id = 0;
      for (StoreMapping mapping : mappingsSource) {
        id++;
        steps.add(new StoreOperationInput.Builder()
            .withStepId(id)
            .withStoreOperationStepKind(StoreOperationStepKind.Remove)
            .withMapping(mapping)
            .build());
      }
      for (StoreMapping mapping : mappingsTarget) {
        id++;
        steps.add(new StoreOperationInput.Builder()
            .withStepId(id)
            .withStoreOperationStepKind(StoreOperationStepKind.Add)
            .withMapping(mapping)
            .build());
      }

      QName rootElementName = new QName("BulkOperationShardMappingsLocal");
      StoreOperationInput input = new StoreOperationInput.Builder()
          .withOperationId(operationId)
          .withUndo(undo)
          .withStepsCount(id)
          .withLsmVersion()
          .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
          .withShard(mappingsTarget.length > 0
              ? mappingsTarget[0].getStoreShard()
              : mappingsSource[0].getStoreShard())
          .withSteps(steps)
          .build();
      return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
    }

    return null;
  }

  /**
   * Request to kill sessions with given application name pattern in LSM.
   *
   * @param pattern Pattern for application name.
   * @return Xml formatted request.
   */
  public static JAXBElement<StoreOperationInput> killSessionsForShardMappingLocal(String pattern) {
    QName rootElementName = new QName("KillSessionsForShardMappingLocal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withLsmVersion()
        .withPattern(pattern)
        .build();
    return new JAXBElement<>(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Print the created XML to verify. Usage: return printLogAndReturn(new
   * JAXBElement(rootElementName, StoreOperationInput.class, input));
   *
   * @return JAXBElement - same object which came as input.
   */
  private static JAXBElement printLogAndReturn(JAXBElement jaxbElement) {
    try {
      //Create a String writer object which will be
      //used to write jaxbElment XML to string
      StringWriter writer = new StringWriter();

      // create JAXBContext which will be used to update writer
      JAXBContext context = JAXBContext.newInstance(StoreOperationInput.class);

      // marshall or convert jaxbElement containing student to xml format
      context.createMarshaller().marshal(jaxbElement, writer);

      //print XML string representation of Student object
      System.out.println(writer.toString());
    } catch (JAXBException e) {
      e.printStackTrace();
    }
    return jaxbElement;
  }

  ///#endregion Methods

  static class Steps {

    @XmlElement(name = "Step")
    private List<StoreOperationInput> steps;

    Steps() {
    }

    Steps(List<StoreOperationInput> steps) {
      this.steps = steps;
    }
  }

  static class Lock {

    @XmlElement(name = "Id")
    private UUID lockId;

    @XmlElement(name = "Operation")
    private int lockOpType;

    Lock() {
    }

    Lock(UUID lockId, int lockOpType) {
      this.lockId = lockId;
      this.lockOpType = lockOpType;
    }
  }
}