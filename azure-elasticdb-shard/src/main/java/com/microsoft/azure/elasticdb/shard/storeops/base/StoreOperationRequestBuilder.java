package com.microsoft.azure.elasticdb.shard.storeops.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

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

  /**
   * FindAndUpdateOperationLogEntryByIdGlobal stored procedure.
   */
  public static final String SpFindAndUpdateOperationLogEntryByIdGlobal = "__ShardManagement.spFindAndUpdateOperationLogEntryByIdGlobal";

  /**
   * GetAllShardMapsGlobal stored procedure.
   */
  public static final String SpGetAllShardMapsGlobal = "__ShardManagement.spGetAllShardMapsGlobal";

  /**
   * FindShardMapByNameGlobal stored procedure.
   */
  public static final String SpFindShardMapByNameGlobal = "__ShardManagement.spFindShardMapByNameGlobal";

  ///#region GSM Stored Procedures

  /**
   * GetAllDistinctShardLocationsGlobal stored procedure.
   */
  public static final String SpGetAllDistinctShardLocationsGlobal = "__ShardManagement.spGetAllDistinctShardLocationsGlobal";

  /**
   * AddShardMapGlobal stored procedure.
   */
  public static final String SpAddShardMapGlobal = "__ShardManagement.spAddShardMapGlobal";

  /**
   * RemoveShardMapGlobal stored procedure.
   */
  public static final String SpRemoveShardMapGlobal = "__ShardManagement.spRemoveShardMapGlobal";

  /**
   * GetAllShardsGlobal stored procedure.
   */
  public static final String SpGetAllShardsGlobal = "__ShardManagement.spGetAllShardsGlobal";

  /**
   * FindShardByLocationGlobal stored procedure.
   */
  public static final String SpFindShardByLocationGlobal = "__ShardManagement.spFindShardByLocationGlobal";

  /**
   * BulkOperationShardsGlobalBegin stored procedure.
   */
  public static final String SpBulkOperationShardsGlobalBegin = "__ShardManagement.spBulkOperationShardsGlobalBegin";

  /**
   * BulkOperationShardsGlobalEnd stored procedure.
   */
  public static final String SpBulkOperationShardsGlobalEnd = "__ShardManagement.spBulkOperationShardsGlobalEnd";

  /**
   * GetAllShardMappingsGlobal stored procedure.
   */
  public static final String SpGetAllShardMappingsGlobal = "__ShardManagement.spGetAllShardMappingsGlobal";

  /**
   * FindShardMappingByKeyGlobal stored procedure.
   */
  public static final String SpFindShardMappingByKeyGlobal = "__ShardManagement.spFindShardMappingByKeyGlobal";

  /**
   * FindShardMappingByIdGlobal stored procedure.
   */
  public static final String SpFindShardMappingByIdGlobal = "__ShardManagement.spFindShardMappingByIdGlobal";

  /**
   * BulkShardMappingOperationsGlobalBegin stored procedure.
   */
  public static final String SpBulkOperationShardMappingsGlobalBegin = "__ShardManagement.spBulkOperationShardMappingsGlobalBegin";

  /**
   * BulkShardMappingOperationsGlobalEnd stored procedure.
   */
  public static final String SpBulkOperationShardMappingsGlobalEnd = "__ShardManagement.spBulkOperationShardMappingsGlobalEnd";

  /**
   * LockOrUnLockShardMappingsGlobal stored procedure.
   */
  public static final String SpLockOrUnLockShardMappingsGlobal = "__ShardManagement.spLockOrUnlockShardMappingsGlobal";

  /**
   * GetAllShardingSchemaInfosGlobal stored procedure.
   */
  public static final String SpGetAllShardingSchemaInfosGlobal = "__ShardManagement.spGetAllShardingSchemaInfosGlobal";

  /**
   * FindShardingSchemaInfoByNameGlobal stored procedure.
   */
  public static final String SpFindShardingSchemaInfoByNameGlobal = "__ShardManagement.spFindShardingSchemaInfoByNameGlobal";

  /**
   * AddShardingSchemaInfoGlobal stored procedure.
   */
  public static final String SpAddShardingSchemaInfoGlobal = "__ShardManagement.spAddShardingSchemaInfoGlobal";

  /**
   * RemoveShardingSchemaInfoGlobal stored procedure.
   */
  public static final String SpRemoveShardingSchemaInfoGlobal = "__ShardManagement.spRemoveShardingSchemaInfoGlobal";

  /**
   * UpdateShardingSchemaInfoGlobal stored procedure.
   */
  public static final String SpUpdateShardingSchemaInfoGlobal = "__ShardManagement.spUpdateShardingSchemaInfoGlobal";

  /**
   * AttachShardGlobal stored procedure.
   */
  public static final String SpAttachShardGlobal = "__ShardManagement.spAttachShardGlobal";

  /**
   * DetachShardGlobal stored procedure.
   */
  public static final String SpDetachShardGlobal = "__ShardManagement.spDetachShardGlobal";

  /**
   * ReplaceShardMappingsGlobal stored procedure.
   */
  public static final String SpReplaceShardMappingsGlobal = "__ShardManagement.spReplaceShardMappingsGlobal";

  /**
   * GetAllShardsLocal stored procedure.
   */
  public static final String SpGetAllShardsLocal = "__ShardManagement.spGetAllShardsLocal";

  /**
   * ValidateShardLocal stored procedure.
   */
  public static final String SpValidateShardLocal = "__ShardManagement.spValidateShardLocal";

  /**
   * AddShardLocal stored procedure.
   */
  public static final String SpAddShardLocal = "__ShardManagement.spAddShardLocal";

  ///#endregion GSM Stored Procedures

  ///#region LSM Stored Procedures

  /**
   * RemoveShardLocal stored procedure.
   */
  public static final String SpRemoveShardLocal = "__ShardManagement.spRemoveShardLocal";

  /**
   * UpdateShardLocal stored procedure.
   */
  public static final String SpUpdateShardLocal = "__ShardManagement.spUpdateShardLocal";

  /**
   * GetAllShardMappingsLocal stored procedure.
   */
  public static final String SpGetAllShardMappingsLocal = "__ShardManagement.spGetAllShardMappingsLocal";

  /**
   * FindShardMappingByKeyLocal stored procedure.
   */
  public static final String SpFindShardMappingByKeyLocal = "__ShardManagement.spFindShardMappingByKeyLocal";

  /**
   * ValidateShardMappingLocal stored procedure.
   */
  public static final String SpValidateShardMappingLocal = "__ShardManagement.spValidateShardMappingLocal";

  /**
   * BulkOperationShardMappingsLocal stored procedure.
   */
  public static final String SpBulkOperationShardMappingsLocal = "__ShardManagement.spBulkOperationShardMappingsLocal";

  /**
   * KillSessionsForShardMappingLocal stored procedure.
   */
  public static final String SpKillSessionsForShardMappingLocal = "__ShardManagement.spKillSessionsForShardMappingLocal";

  /**
   * Find operation log entry by Id from GSM.
   *
   * @param operationId Operation Id.
   * @param undoStartState Minimum start from which to start undo operation.
   * @return Xml formatted request.
   */
  public static JAXBElement FindAndUpdateOperationLogEntryByIdGlobal(UUID operationId,
      StoreOperationState undoStartState) {
    QName rootElementName = new QName("FindAndUpdateOperationLogEntryByIdGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withOperationId(operationId)
        .withUndoStartState(undoStartState)
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
  }

  ///#endregion LSM Stored Procedures

  ///#region Methods

  /**
   * Request to get all shard maps from GSM.
   *
   * @return Xml formatted request.
   */
  public static JAXBElement GetAllShardMapsGlobal() {
    QName rootElementName = new QName("GetAllShardMapsGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to get shard map with the given name from GSM.
   *
   * @param shardMapName Name of shard map.
   * @return Xml formatted request.
   */
  public static JAXBElement FindShardMapByNameGlobal(String shardMapName) {
    QName rootElementName = new QName("FindShardMapByNameGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withShardMap(new StoreShardMap(null, shardMapName, null, null))
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to get all distinct shard locations from GSM.
   *
   * @return Xml formatted request.
   */
  public static JAXBElement GetAllDistinctShardLocationsGlobal() {
    QName rootElementName = new QName("GetAllDistinctShardLocationsGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to add shard map to GSM.
   *
   * @param shardMap ShardId map to add.
   * @return Xml formatted request.
   */
  public static JAXBElement AddShardMapGlobal(StoreShardMap shardMap) {
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
  public static JAXBElement RemoveShardMapGlobal(StoreShardMap shardMap) {
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
  public static JAXBElement GetAllShardsGlobal(StoreShardMap shardMap) {
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
  public static JAXBElement FindShardByLocationGlobal(StoreShardMap shardMap,
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
  public static JAXBElement AddShardGlobal(UUID operationId, StoreOperationCode operationCode,
      boolean undo, StoreShardMap shardMap, StoreShard shard) {
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
  public static JAXBElement RemoveShardGlobal(UUID operationId, StoreOperationCode operationCode,
      boolean undo, StoreShardMap shardMap, StoreShard shard) {
    QName rootElementName = new QName("BulkOperationShardsGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withOperationId(operationId)
        .withOperationCode(operationCode)
        .withUndo(undo)
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withShard(shard == null ? StoreShard.NULL : shard)
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
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
  public static JAXBElement UpdateShardGlobal(UUID operationId, StoreOperationCode operationCode,
      boolean undo, StoreShardMap shardMap, StoreShard shardOld, StoreShard shardNew) {
    QName rootElementName = new QName("BulkOperationShardsGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withOperationId(operationId)
        .withOperationCode(operationCode)
        .withUndo(undo)
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withIStoreShardOld(shardOld == null ? StoreShard.NULL : shardOld)
        .withShard(shardNew == null ? StoreShard.NULL : shardNew)
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
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
  public static JAXBElement GetAllShardMappingsGlobal(StoreShardMap shardMap, StoreShard shard,
      ShardRange range) {
    QName rootElementName = new QName("GetAllShardMappingsGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withShard(shard == null ? StoreShard.NULL : shard)
        .withShardRange(range == null ? ShardRange.NULL : range)
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to get mapping from GSM for a particular key belonging to a shard map.
   *
   * @param shardMap ShardId map whose mappings are being requested.
   * @param key Key being searched.
   * @return Xml formatted request.
   */
  public static JAXBElement FindShardMappingByKeyGlobal(StoreShardMap shardMap, ShardKey key) {
    QName rootElementName = new QName("FindShardMappingByKeyGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withShardKey(key)
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to get mapping from GSM for a particular mapping Id.
   *
   * @param shardMap ShardId map whose mappings are being requested.
   * @param mapping Mapping to look up.
   * @return Xml formatted request.
   */
  public static JAXBElement FindShardMappingByIdGlobal(StoreShardMap shardMap,
      StoreMapping mapping) {
    QName rootElementName = new QName("FindShardMappingByKeyGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withMapping(mapping)
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
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
  public static JAXBElement AddShardMappingGlobal(UUID operationId,
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
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
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
  public static JAXBElement RemoveShardMappingGlobal(UUID operationId,
      StoreOperationCode operationCode, boolean undo, StoreShardMap shardMap, StoreMapping mapping,
      UUID lockOwnerId) {
    QName rootElementName = new QName("BulkOperationShardMappingsGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withOperationId(operationId)
        .withStoreOperationCode(operationCode)
        .withUndo(undo)
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withIStoreMapping(mapping)
        .withLockOwnerId(lockOwnerId)
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
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
  public static JAXBElement UpdateShardMappingGlobal(UUID operationId,
      StoreOperationCode operationCode, boolean undo, String patternForKill, StoreShardMap shardMap,
      StoreMapping mappingSource, StoreMapping mappingTarget, UUID lockOwnerId) {
    QName rootElementName = new QName("BulkOperationShardMappingsGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withOperationId(operationId)
        .withStoreOperationCode(operationCode)
        .withUndo(undo)
        .withPatternForKill(patternForKill)
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withIStoreMapping(mappingSource)
        .withIStoreMappingTarget(mappingTarget)
        .withLockOwnerId(lockOwnerId)
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
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
  public static JAXBElement ReplaceShardMappingsGlobal(UUID operationId,
      StoreOperationCode operationCode, boolean undo, StoreShardMap shardMap,
      Pair<StoreMapping, UUID>[] mappingsSource, Pair<StoreMapping, UUID>[] mappingsTarget) {

    QName rootElementName = new QName("BulkOperationShardMappingsGlobal");

    StoreOperationInput.Builder builder = new StoreOperationInput.Builder();
    if (mappingsSource.length > 0) {
      builder.withMappingsSource(mappingsSource);
    }
    if (mappingsTarget.length > 0) {
      builder.withMappingsTarget(mappingsTarget);
    }
    StoreOperationInput input = builder
        .withGsmVersion()
        .withOperationId(operationId)
        .withOperationCode(operationCode)
        .withUndo(undo)
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .build();

    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
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
  public static JAXBElement LockOrUnLockShardMappingsGlobal(StoreShardMap shardMap,
      StoreMapping mapping, UUID lockId, LockOwnerIdOpType lockOpType) {

    QName rootElementName = new QName("LockOrUnlockShardMappingsGlobal");
    StoreOperationInput input;
    StoreOperationInput.Builder builder = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withLockOwnerId(lockId);

    if (mapping != null || (lockOpType == LockOwnerIdOpType.UnlockAllMappingsForId
        || lockOpType == LockOwnerIdOpType.UnlockAllMappings)) {
      builder.withMapping(mapping);
      builder.withLockOwnerIdOpType(lockOpType);
    }
    input = builder.build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);

  }

  /**
   * Request to get all schema info objects from GSM.
   *
   * @return Xml formatted request.
   */
  public static JAXBElement GetAllShardingSchemaInfosGlobal() {
    QName rootElementName = new QName("GetAllShardingSchemaInfosGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to find schema info in GSM.
   *
   * @param name Schema info name to find.
   * @return Xml formatted request.
   */
  public static JAXBElement FindShardingSchemaInfoGlobal(String name) {
    QName rootElementName = new QName("FindShardingSchemaInfoGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withSchemaInfo(new StoreSchemaInfo(name, null))
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to add schema info to GSM.
   *
   * @param schemaInfo Schema info object to add
   * @return Xml formatted request.
   */
  public static JAXBElement AddShardingSchemaInfoGlobal(StoreSchemaInfo schemaInfo) {
    QName rootElementName = new QName("AddShardingSchemaInfoGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withSchemaInfo(schemaInfo)
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to delete schema info object from GSM.
   *
   * @param name Name of schema info to delete.
   * @return Xml formatted request.
   */
  public static JAXBElement RemoveShardingSchemaInfoGlobal(String name) {
    QName rootElementName = new QName("RemoveShardingSchemaInfoGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withSchemaInfo(new StoreSchemaInfo(name, null))
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to update schema info to GSM.
   *
   * @param schemaInfo Schema info object to update
   * @return Xml formatted request.
   */
  public static JAXBElement UpdateShardingSchemaInfoGlobal(StoreSchemaInfo schemaInfo) {
    QName rootElementName = new QName("AddShardingSchemaInfoGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withSchemaInfo(schemaInfo)
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to attach shard to GSM.
   *
   * @param shardMap ShardId map to attach.
   * @param shard ShardId to attach.
   * @return Xml formatted request.
   */
  public static JAXBElement AttachShardGlobal(StoreShardMap shardMap, StoreShard shard) {
    QName rootElementName = new QName("AttachShardGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withShard(shard == null ? StoreShard.NULL : shard)
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to detach shard to GSM.
   *
   * @param shardMapName Optional shard map name to detach.
   * @param location Location to detach.
   * @return Xml formatted request.
   */
  public static JAXBElement DetachShardGlobal(String shardMapName, ShardLocation location) {
    QName rootElementName = new QName("DetachShardGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withShardMap(shardMapName == null ? StoreShardMap.NULL
            : new StoreShardMap(null, shardMapName, null, null))
        .withLocation(location)
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to replace mappings in given shard map in GSM without logging.
   *
   * @param shardMap ShardId map for which operation is being requested.
   * @param mappingsSource Original mappings.
   * @param mappingsTarget New mappings.
   * @return Xml formatted request.
   */
  public static JAXBElement ReplaceShardMappingsGlobalWithoutLogging(StoreShardMap shardMap,
      StoreMapping[] mappingsSource, StoreMapping[] mappingsTarget) {
    //Debug.Assert(mappingsSource.length + mappingsTarget.length > 0, "Expecting at least one mapping for ReplaceMappingsGlobalWithoutLogging.");

    QName rootElementName = new QName("DetachShardGlobal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withGsmVersion()
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withMappingsSourceArray(mappingsSource)
        .withMappingsTargetArray(mappingsTarget)
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to get all shards and shard maps from LSM.
   *
   * @return Xml formatted request.
   */
  public static JAXBElement GetAllShardsLocal() {
    QName rootElementName = new QName("GetAllShardsLocal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withLsmVersion()
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Validation request for shard for LSM.
   *
   * @param shardMapId ShardId map Id.
   * @param shardId ShardId Id.
   * @param shardVersion ShardId version.
   * @return Xml formatted request.
   */
  public static JAXBElement ValidateShardLocal(UUID shardMapId, UUID shardId, UUID shardVersion) {
    QName rootElementName = new QName("ValidateShardLocal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withLsmVersion()
        .withShardMapId(shardMapId)
        .withShardId(shardId)
        .withShardVersion(shardVersion)
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
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
  public static JAXBElement AddShardLocal(UUID operationId, boolean undo, StoreShardMap shardMap,
      StoreShard shard) {
    QName rootElementName = new QName("AddShardLocal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withLsmVersion()
        .withOperationId(operationId)
        .withUndo(undo)
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withShard(shard == null ? StoreShard.NULL : shard)
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to remove shard from given shard map in LSM.
   *
   * @param operationId Operation Id.
   * @param shardMap ShardId map to remove shard from.
   * @param shard ShardId to remove.
   * @return Xml formatted request.
   */
  public static JAXBElement RemoveShardLocal(UUID operationId, StoreShardMap shardMap,
      StoreShard shard) {
    QName rootElementName = new QName("RemoveShardLocal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withLsmVersion()
        .withOperationId(operationId)
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withShard(shard == null ? StoreShard.NULL : shard)
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to update shard in given shard map in LSM.
   *
   * @param operationId Operation Id.
   * @param shardMap ShardId map to remove shard from.
   * @param shard ShardId to update.
   * @return Xml formatted request.
   */
  public static JAXBElement UpdateShardLocal(UUID operationId, StoreShardMap shardMap,
      StoreShard shard) {
    QName rootElementName = new QName("UpdateShardLocal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withLsmVersion()
        .withOperationId(operationId)
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withShard(shard == null ? StoreShard.NULL : shard)
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
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
  public static JAXBElement GetAllShardMappingsLocal(StoreShardMap shardMap, StoreShard shard,
      ShardRange range) {
    QName rootElementName = new QName("GetAllShardMappingsLocal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withLsmVersion()
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withShard(shard == null ? StoreShard.NULL : shard)
        .withShardRange(range)
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to get mapping from LSM for a particular key belonging to a shard map.
   *
   * @param shardMap ShardId map whose mappings are being requested.
   * @param key Key being searched.
   * @return Xml formatted request.
   */
  public static JAXBElement FindShardMappingByKeyLocal(StoreShardMap shardMap, ShardKey key) {
    QName rootElementName = new QName("FindShardMappingByKeyLocal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withLsmVersion()
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withShardKey(key)
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Validation request for shard mapping for LSM.
   *
   * @param shardMapId ShardId map Id.
   * @param mappingId ShardId mapping Id.
   * @return Xml formatted request.
   */
  public static JAXBElement ValidateShardMappingLocal(UUID shardMapId, UUID mappingId) {
    QName rootElementName = new QName("ValidateShardMappingLocal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withLsmVersion()
        .withShardMapId(shardMapId)
        .withMappingId(mappingId)
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
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
  public static JAXBElement AddShardMappingLocal(UUID operationId, boolean undo,
      StoreShardMap shardMap, StoreMapping mapping) {
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
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
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
  public static JAXBElement RemoveShardMappingLocal(UUID operationId, boolean undo,
      StoreShardMap shardMap, StoreMapping mapping) {
    QName rootElementName = new QName("BulkOperationShardMappingsLocal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withLsmVersion()
        .withOperationId(operationId)
        .withUndo(undo)
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withMapping(mapping)
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
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
  public static JAXBElement UpdateShardMappingLocal(UUID operationId, boolean undo,
      StoreShardMap shardMap, StoreMapping mappingSource, StoreMapping mappingTarget) {
    QName rootElementName = new QName("BulkOperationShardMappingsLocal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withLsmVersion()
        .withOperationId(operationId)
        .withUndo(undo)
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withMapping(mappingSource)
        .withMappingTarget(mappingTarget)
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
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
  public static JAXBElement ReplaceShardMappingsLocal(UUID operationId, boolean undo,
      StoreShardMap shardMap, StoreMapping[] mappingsSource, StoreMapping[] mappingsTarget) {
    QName rootElementName = new QName("BulkOperationShardMappingsLocal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withLsmVersion()
        .withOperationId(operationId)
        .withUndo(undo)
        .withShardMap(shardMap == null ? StoreShardMap.NULL : shardMap)
        .withMappingsSourceArray(mappingsSource)
        .withMappingsTargetArray(mappingsTarget)
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
  }

  /**
   * Request to kill sessions with given application name pattern in LSM.
   *
   * @param pattern Pattern for application name.
   * @return Xml formatted request.
   */
  public static JAXBElement KillSessionsForShardMappingLocal(String pattern) {
    QName rootElementName = new QName("KillSessionsForShardMappingLocal");
    StoreOperationInput input = new StoreOperationInput.Builder()
        .withLsmVersion()
        .withPatternForKill(pattern)
        .build();
    return new JAXBElement(rootElementName, StoreOperationInput.class, input);
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
}