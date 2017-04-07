package com.microsoft.azure.elasticdb.shard.storeops.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.*;
import com.microsoft.azure.elasticdb.shard.map.ShardMapType;
import com.microsoft.azure.elasticdb.shard.store.*;
import com.microsoft.azure.elasticdb.shard.utils.GlobalConstants;
import com.microsoft.azure.elasticdb.shard.utils.XElement;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.Marshaller;
import javax.xml.namespace.QName;
import java.util.UUID;

/**
 * Constructs requests for store operations.
 */
public final class StoreOperationRequestBuilder {
    public static final IStoreShardMap s_NullShardMap = new IStoreShardMap() {
        @Override
        public UUID getId() {
            return null;
        }

        @Override
        public String getName() {
            return null;
        }

        @Override
        public ShardMapType getMapType() {
            return null;
        }

        @Override
        public ShardKeyType getKeyType() {
            return null;
        }

        @Override
        public int isNull() {
            return 1;
        }
    };

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
     * Element representing GSM version.
     */
    private static final XElement s_gsmVersion = new XElement("GsmVersion", new XElement("MajorVersion", GlobalConstants.GsmVersionClient.getMajor()), new XElement("MinorVersion", GlobalConstants.GsmVersionClient.getMinor()));
    /**
     * Element representing LSM version.
     */
    private static final XElement s_lsmVersion = new XElement("LsmVersion", new XElement("MajorVersion", GlobalConstants.LsmVersionClient.getMajor()), new XElement("MinorVersion", GlobalConstants.LsmVersionClient.getMinor()));

    /**
     * Find operation log entry by Id from GSM.
     *
     * @param operationId    Operation Id.
     * @param undoStartState Minimum start from which to start undo operation.
     * @return Xml formatted request.
     */
    public static JAXBElement FindAndUpdateOperationLogEntryByIdGlobal(UUID operationId, StoreOperationState undoStartState) {
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
                .withShardMap(new DefaultStoreShardMap(null, shardMapName, null, null))
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
     * @param shardMap Shard map to add.
     * @return Xml formatted request.
     */
    public static JAXBElement AddShardMapGlobal(IStoreShardMap shardMap) {
        QName rootElementName = new QName("AddShardMapGlobal");
        StoreOperationInput input = new StoreOperationInput.Builder()
                .withGsmVersion()
                .withShardMap(shardMap == null ? s_NullShardMap : shardMap)
                .build();
        return new JAXBElement(rootElementName, StoreOperationInput.class, input);
    }

    /**
     * Request to remove shard map from GSM.
     *
     * @param shardMap Shard map to remove.
     * @return Xml formatted request.
     */
    public static JAXBElement RemoveShardMapGlobal(IStoreShardMap shardMap) {
        QName rootElementName = new QName("RemoveShardMapGlobal");
        StoreOperationInput input = new StoreOperationInput.Builder()
                .withGsmVersion()
                .withShardMap(shardMap == null ? s_NullShardMap : shardMap)
                .build();
        return new JAXBElement(rootElementName, StoreOperationInput.class, input);
    }

    /**
     * Request to get all shards for a shard map from GSM.
     *
     * @param shardMap Shard map for which to get all shards.
     * @return Xml formatted request.
     */
    public static JAXBElement GetAllShardsGlobal(IStoreShardMap shardMap) {
        QName rootElementName = new QName("GetAllShardsGlobal");
        StoreOperationInput input = new StoreOperationInput.Builder()
                .withGsmVersion()
                .withShardMap(shardMap == null ? s_NullShardMap : shardMap)
                .build();
        return new JAXBElement(rootElementName, StoreOperationInput.class, input);
    }

    /**
     * Request to get shard with specified location for a shard map from GSM.
     *
     * @param shardMap Shard map for which to get shard.
     * @param location Location for which to find shard.
     * @return Xml formatted request.
     */
    public static JAXBElement FindShardByLocationGlobal(IStoreShardMap shardMap, ShardLocation location) {
        QName rootElementName = new QName("FindShardByLocationGlobal");
        StoreOperationInput input = new StoreOperationInput.Builder()
                .withGsmVersion()
                .withShardMap(shardMap == null ? s_NullShardMap : shardMap)
                .withLocation(location)
                .build();
        return new JAXBElement(rootElementName, StoreOperationInput.class, input);
    }

    /**
     * Request to add shard to given shard map in GSM.
     * @param operationId
     * @param operationCode
     * @param undo
     * @param shardMap
     * @param shard
     * @return Xml formatted request.
     */
    public static JAXBElement AddShardGlobal(UUID operationId, StoreOperationCode operationCode, boolean undo, IStoreShardMap shardMap, IStoreShard shard) {
    	QName rootElementName = new QName("BulkOperationShardsGlobal");
    	StoreOperationInput input = new StoreOperationInput.Builder()
    			                    .withGsmVersion()
    			                    .withOperationId(operationId)
    			                    .withStoreOperationCode(operationCode)
    			                    .withUndo(undo)
    			                    .withShardMap(shardMap == null ? s_NullShardMap : shardMap)
    			                    .withIStoreShard(shard)
    			                    .build();
    	return new JAXBElement(rootElementName, StoreOperationInput.class, input);
    			                 
    }

    /**
     * Request to remove shard from given shard map in GSM.
     *
     * @param operationId   Operation Id
     * @param operationCode Operation code.
     * @param undo          Whether this is an undo request.
     * @param shardMap      Shard map for which operation is being requested.
     * @param shard         Shard to remove.
     * @return Xml formatted request.
     */
    public static JAXBElement RemoveShardGlobal(UUID operationId, StoreOperationCode operationCode, boolean undo, IStoreShardMap shardMap, IStoreShard shard) {
    	QName rootElementName = new QName("BulkOperationShardsGlobal");
    	StoreOperationInput input = new StoreOperationInput.Builder()
    			                    .withGsmVersion()
    			                    .withOperationId(operationId)
    			                    .withStoreOperationCode(operationCode)
    			                    .withUndo(undo)
    			                    .withShardMap(shardMap == null ? s_NullShardMap : shardMap)
    			                    .withIStoreShard(shard)
    			                    .build();
    	return new JAXBElement(rootElementName, StoreOperationInput.class, input);
    }

    /**
     * Request to update shard in given shard map in GSM.
     *
     * @param operationId   Operation Id
     * @param operationCode Operation code.
     * @param undo          Whether this is an undo request.
     * @param shardMap      Shard map for which operation is being requested.
     * @param shardOld      Shard to update.
     * @param shardNew      Updated shard.
     * @return Xml formatted request.
     */
    public static JAXBElement UpdateShardGlobal(UUID operationId, StoreOperationCode operationCode, boolean undo, IStoreShardMap shardMap, IStoreShard shardOld, IStoreShard shardNew) {
    	QName rootElementName = new QName("BulkOperationShardsGlobal");
    	StoreOperationInput input = new StoreOperationInput.Builder()
    			                    .withGsmVersion()
    			                    .withOperationId(operationId)
    			                    .withStoreOperationCode(operationCode)
    			                    .withUndo(undo)
    			                    .withShardMap(shardMap == null ? s_NullShardMap : shardMap)
    			                    .withIStoreShardOld(shardOld)
    			                    .withIStoreShard(shardNew)
    			                    .build();
    	return new JAXBElement(rootElementName, StoreOperationInput.class, input);
    }

    /**
     * Request to get all shard mappings from GSM for a particular shard map
     * and optional shard and range.
     *
     * @param shardMap Shard map whose mappings are being requested.
     * @param shard    Optional shard for which mappings are being requested.
     * @param range    Optional range for which mappings are being requested.
     * @return Xml formatted request.
     */
    public static JAXBElement GetAllShardMappingsGlobal(IStoreShardMap shardMap, IStoreShard shard, ShardRange range) {
    	QName rootElementName = new QName("GetAllShardMappingsGlobal");
    	StoreOperationInput input = new StoreOperationInput.Builder()
    			                    .withGsmVersion()
    			                    .withShardMap(shardMap == null ? s_NullShardMap : shardMap)
    			                    .withIStoreShard(shard)
    			                    .withShardRange(range)
    			                    .build();
    	return new JAXBElement(rootElementName, StoreOperationInput.class, input);
    }

    /**
     * Request to get mapping from GSM for a particular key belonging to a shard map.
     *
     * @param shardMap Shard map whose mappings are being requested.
     * @param key      Key being searched.
     * @return Xml formatted request.
     */
    public static JAXBElement FindShardMappingByKeyGlobal(IStoreShardMap shardMap, ShardKey key) {
    	QName rootElementName = new QName("FindShardMappingByKeyGlobal");
    	StoreOperationInput input = new StoreOperationInput.Builder()
    			                    .withGsmVersion()
    			                    .withShardMap(shardMap == null ? s_NullShardMap : shardMap)
    			                    .withShardKey(key)
    			                    .build();
    	return new JAXBElement(rootElementName, StoreOperationInput.class, input);
    }

    /**
     * Request to get mapping from GSM for a particular mapping Id.
     *
     * @param shardMap Shard map whose mappings are being requested.
     * @param mapping  Mapping to look up.
     * @return Xml formatted request.
     */
    public static JAXBElement FindShardMappingByIdGlobal(IStoreShardMap shardMap, IStoreMapping mapping) {
    	QName rootElementName = new QName("FindShardMappingByKeyGlobal");
    	StoreOperationInput input = new StoreOperationInput.Builder()
    			                    .withGsmVersion()
    			                    .withShardMap(shardMap == null ? s_NullShardMap : shardMap)
    			                    .withIStoreMapping(mapping)
    			                    .build();
    	return new JAXBElement(rootElementName, StoreOperationInput.class, input);
    }

    /**
     * Request to add shard to given shard map in GSM.
     *
     * @param operationId   Operation Id.
     * @param operationCode Operation code.
     * @param undo          Whether this is an undo request.
     * @param shardMap      Shard map for which operation is being requested.
     * @param mapping       Mapping to add.
     * @return Xml formatted request.
     */
    public static JAXBElement AddShardMappingGlobal(UUID operationId, StoreOperationCode operationCode, boolean undo, IStoreShardMap shardMap, IStoreMapping mapping) {
    	QName rootElementName = new QName("BulkOperationShardMappingsGlobal");
    	StoreOperationInput input = new StoreOperationInput.Builder()
    			                    .withGsmVersion()
    			                    .withOperationId(operationId)
    			                    .withStoreOperationCode(operationCode)
    			                    .withUndo(undo)
    			                    .withShardMap(shardMap == null ? s_NullShardMap : shardMap)
    			                    .withIStoreMapping(mapping)
    			                    .build();
    	return new JAXBElement(rootElementName, StoreOperationInput.class, input);
    }

    /**
     * Request to remove shard from given shard map in GSM.
     *
     * @param operationId   Operation Id.
     * @param operationCode Operation code.
     * @param undo          Whether this is an undo request.
     * @param shardMap      Shard map for which operation is being requested.
     * @param mapping       Mapping to remove.
     * @param lockOwnerId   Lock owner.
     * @return Xml formatted request.
     */
    public static JAXBElement RemoveShardMappingGlobal(UUID operationId, StoreOperationCode operationCode, boolean undo, IStoreShardMap shardMap, IStoreMapping mapping, UUID lockOwnerId) {
    	QName rootElementName = new QName("BulkOperationShardMappingsGlobal");
    	StoreOperationInput input = new StoreOperationInput.Builder()
    			                    .withGsmVersion()
    			                    .withOperationId(operationId)
    			                    .withStoreOperationCode(operationCode)
    			                    .withUndo(undo)
    			                    .withShardMap(shardMap == null ? s_NullShardMap : shardMap)
    			                    .withIStoreMapping(mapping)
    			                    .withLockOwnerId(lockOwnerId)
    			                    .build();
    	return new JAXBElement(rootElementName, StoreOperationInput.class, input);
    }

    /**
     * Request to update mapping in given shard map in GSM.
     *
     * @param operationId    Operation Id.
     * @param operationCode  Operation code.
     * @param undo           Whether this is an undo request.
     * @param patternForKill Pattern to use for kill connection.
     * @param shardMap       Shard map for which operation is being requested.
     * @param mappingSource  Shard to update.
     * @param mappingTarget  Updated shard.
     * @param lockOwnerId    Lock owner.
     * @return Xml formatted request.
     */
    public static JAXBElement UpdateShardMappingGlobal(UUID operationId, StoreOperationCode operationCode, boolean undo, String patternForKill, IStoreShardMap shardMap, IStoreMapping mappingSource, IStoreMapping mappingTarget, UUID lockOwnerId) {
    	QName rootElementName = new QName("BulkOperationShardMappingsGlobal");
    	StoreOperationInput input = new StoreOperationInput.Builder()
    			                    .withGsmVersion()
    			                    .withOperationId(operationId)
    			                    .withStoreOperationCode(operationCode)
    			                    .withUndo(undo)
    			                    .withPatternForKill(patternForKill)
    			                    .withShardMap(shardMap == null ? s_NullShardMap : shardMap)
    			                    .withIStoreMapping(mappingSource)
    			                    .withIStoreMappingTarget(mappingTarget)
    			                    .withLockOwnerId(lockOwnerId)
    			                    .build();
    	return new JAXBElement(rootElementName, StoreOperationInput.class, input);
    }

    /**
     * Request to replace mappings in given shard map in GSM.
     *
     * @param operationId    Operation Id.
     * @param operationCode  Operation code.
     * @param undo           Whether this is an undo request.
     * @param shardMap       Shard map for which operation is being requested.
     * @param mappingsSource Original mappings.
     * @param mappingsTarget New mappings.
     * @return Xml formatted request.
     */
    public static XElement ReplaceShardMappingsGlobal(UUID operationId, StoreOperationCode operationCode, boolean undo, IStoreShardMap shardMap, Tuple<IStoreMapping, UUID>[] mappingsSource, Tuple<IStoreMapping, UUID>[] mappingsTarget) {
        assert mappingsSource.length > 0;
        assert mappingsTarget.length > 0;

        return new XElement("BulkOperationShardMappingsGlobal", StoreOperationRequestBuilder.OperationId(operationId), StoreOperationRequestBuilder.OperationCode(operationCode), StoreOperationRequestBuilder.Undo(undo), StoreOperationRequestBuilder.StepsCount(mappingsSource.length + mappingsTarget.length), StoreOperationRequestBuilder.s_gsmVersion, StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap), new XElement("Removes", StoreObjectFormatterXml.WriteIStoreShard("Shard", mappingsSource[0].Item1.StoreShard)), new XElement("Adds", StoreObjectFormatterXml.WriteIStoreShard("Shard", mappingsTarget[0].Item1.StoreShard)), new XElement("Steps", mappingsSource.Select((mapping, i) -> new XElement("Step", StoreOperationRequestBuilder.Validate(false), StoreOperationRequestBuilder.StepKind(StoreOperationStepKind.Remove), new XAttribute("Id", i + 1), StoreObjectFormatterXml.WriteLock(mapping.Item2), StoreObjectFormatterXml.WriteIStoreMapping("Mapping", mapping.Item1))), mappingsTarget.Select((mapping, i) -> new XElement("Step", StoreOperationRequestBuilder.Validate(false), StoreOperationRequestBuilder.StepKind(StoreOperationStepKind.Add), new XAttribute("Id", mappingsSource.length + i + 1), StoreObjectFormatterXml.WriteLock(mapping.Item2), StoreObjectFormatterXml.WriteIStoreMapping("Mapping", mapping.Item1)))));
    }

    /**
     * Request to lock or unlock mappings in GSM.
     *
     * @param shardMap   Shard map whose mappings are being requested.
     * @param mapping    Mapping being locked or unlocked.
     * @param lockId     Lock Id.
     * @param lockOpType Lock operation code.
     * @return Xml formatted request.
     */
    public static XElement LockOrUnLockShardMappingsGlobal(IStoreShardMap shardMap, IStoreMapping mapping, UUID lockId, LockOwnerIdOpType lockOpType) {
        assert mapping != null || (lockOpType == LockOwnerIdOpType.UnlockAllMappingsForId || lockOpType == LockOwnerIdOpType.UnlockAllMappings);
        return new XElement("LockOrUnlockShardMappingsGlobal", StoreOperationRequestBuilder.s_gsmVersion, StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap), mapping == null ? null : StoreObjectFormatterXml.WriteIStoreMapping("Mapping", mapping), new XElement("Lock", new XElement("Id", lockId), new XElement("Operation", (int) lockOpType)));
    }

    /**
     * Request to get all schema info objects from GSM.
     *
     * @return Xml formatted request.
     */
    public static XElement GetAllShardingSchemaInfosGlobal() {
        return new XElement("GetAllShardingSchemaInfosGlobal", StoreOperationRequestBuilder.s_gsmVersion);
    }

    /**
     * Request to find schema info in GSM.
     *
     * @param name Schema info name to find.
     * @return Xml formatted request.
     */
    public static XElement FindShardingSchemaInfoGlobal(String name) {
        return new XElement("FindShardingSchemaInfoGlobal", StoreOperationRequestBuilder.s_gsmVersion, new XElement("SchemaInfo", new XElement("Name", name)));
    }

    /**
     * Request to add schema info to GSM.
     *
     * @param schemaInfo Schema info object to add
     * @return Xml formatted request.
     */
    public static XElement AddShardingSchemaInfoGlobal(IStoreSchemaInfo schemaInfo) {
        return new XElement("AddShardingSchemaInfoGlobal", StoreOperationRequestBuilder.s_gsmVersion, StoreObjectFormatterXml.WriteIStoreSchemaInfo("SchemaInfo", schemaInfo));
    }

    /**
     * Request to delete schema info object from GSM.
     *
     * @param name Name of schema info to delete.
     * @return Xml formatted request.
     */
    public static XElement RemoveShardingSchemaInfoGlobal(String name) {
        return new XElement("RemoveShardingSchemaInfoGlobal", StoreOperationRequestBuilder.s_gsmVersion, new XElement("SchemaInfo", new XElement("Name", name)));
    }

    /**
     * Request to update schema info to GSM.
     *
     * @param schemaInfo Schema info object to update
     * @return Xml formatted request.
     */
    public static XElement UpdateShardingSchemaInfoGlobal(IStoreSchemaInfo schemaInfo) {
        return new XElement("UpdateShardingSchemaInfoGlobal", StoreOperationRequestBuilder.s_gsmVersion, StoreObjectFormatterXml.WriteIStoreSchemaInfo("SchemaInfo", schemaInfo));
    }

    /**
     * Request to attach shard to GSM.
     *
     * @param shardMap Shard map to attach.
     * @param shard    Shard to attach.
     * @return Xml formatted request.
     */
    public static XElement AttachShardGlobal(IStoreShardMap shardMap, IStoreShard shard) {
        return new XElement("AttachShardGlobal", StoreOperationRequestBuilder.s_gsmVersion, StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap), StoreObjectFormatterXml.WriteIStoreShard("Shard", shard));
    }

    /**
     * Request to detach shard to GSM.
     *
     * @param shardMapName Optional shard map name to detach.
     * @param location     Location to detach.
     * @return Xml formatted request.
     */
    public static XElement DetachShardGlobal(String shardMapName, ShardLocation location) {
        return new XElement("DetachShardGlobal", StoreOperationRequestBuilder.s_gsmVersion, shardMapName == null ? new XElement("ShardMap", new XAttribute("Null", 1)) : new XElement("ShardMap", new XAttribute("Null", 0), new XElement("Name", shardMapName)), StoreObjectFormatterXml.WriteShardLocation("Location", location));
    }

    /**
     * Request to replace mappings in given shard map in GSM without logging.
     *
     * @param shardMap       Shard map for which operation is being requested.
     * @param mappingsSource Original mappings.
     * @param mappingsTarget New mappings.
     * @return Xml formatted request.
     */
    public static XElement ReplaceShardMappingsGlobalWithoutLogging(IStoreShardMap shardMap, IStoreMapping[] mappingsSource, IStoreMapping[] mappingsTarget) {
        //Debug.Assert(mappingsSource.length + mappingsTarget.length > 0, "Expecting at least one mapping for ReplaceMappingsGlobalWithoutLogging.");

        return new XElement("ReplaceShardMappingsGlobal", StoreOperationRequestBuilder.RemoveStepsCount(mappingsSource.length), StoreOperationRequestBuilder.AddStepsCount(mappingsTarget.length), StoreOperationRequestBuilder.s_gsmVersion, StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap), mappingsSource.length > 0 ? new XElement("RemoveSteps", StoreObjectFormatterXml.WriteIStoreShard("Shard", mappingsSource[0].StoreShard), mappingsSource.Select((mapping, i) -> new XElement("Step", new XAttribute("Id", i + 1), StoreObjectFormatterXml.WriteIStoreMapping("Mapping", mapping)))) : new XElement("RemoveSteps"), mappingsTarget.length > 0 ? new XElement("AddSteps", StoreObjectFormatterXml.WriteIStoreShard("Shard", mappingsTarget[0].StoreShard), mappingsTarget.Select((mapping, i) -> new XElement("Step", new XAttribute("Id", i + 1), StoreObjectFormatterXml.WriteIStoreMapping("Mapping", mapping)))) : new XElement("AddSteps"));
    }

    /**
     * Request to get all shards and shard maps from LSM.
     *
     * @return Xml formatted request.
     */
    public static XElement GetAllShardsLocal() {
        return new XElement("GetAllShardsLocal", StoreOperationRequestBuilder.s_lsmVersion);
    }

    /**
     * Validation request for shard for LSM.
     *
     * @param shardMapId   Shard map Id.
     * @param shardId      Shard Id.
     * @param shardVersion Shard version.
     * @return Xml formatted request.
     */
    public static XElement ValidateShardLocal(UUID shardMapId, UUID shardId, UUID shardVersion) {
        return new XElement("ValidateShardLocal", StoreOperationRequestBuilder.s_lsmVersion, new XElement("ShardMapId", shardMapId), new XElement("ShardId", shardId), new XElement("ShardVersion", shardVersion));
    }

    /**
     * Request to add shard to given shard map in LSM.
     *
     * @param operationId Operation Id.
     * @param shardMap    Shard map to add shard to.
     * @param shard       Shard to add.
     * @param undo        Whether this is undo request.
     * @return Xml formatted request.
     */
    public static XElement AddShardLocal(UUID operationId, boolean undo, IStoreShardMap shardMap, IStoreShard shard) {
        return new XElement("AddShardLocal", StoreOperationRequestBuilder.OperationId(operationId), StoreOperationRequestBuilder.Undo(undo), StoreOperationRequestBuilder.s_lsmVersion, StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap), StoreObjectFormatterXml.WriteIStoreShard("Shard", shard));
    }

    /**
     * Request to remove shard from given shard map in LSM.
     *
     * @param operationId Operation Id.
     * @param shardMap    Shard map to remove shard from.
     * @param shard       Shard to remove.
     * @return Xml formatted request.
     */
    public static XElement RemoveShardLocal(UUID operationId, IStoreShardMap shardMap, IStoreShard shard) {
        return new XElement("RemoveShardLocal", StoreOperationRequestBuilder.OperationId(operationId), StoreOperationRequestBuilder.s_lsmVersion, StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap), StoreObjectFormatterXml.WriteIStoreShard("Shard", shard));
    }

    /**
     * Request to update shard in given shard map in LSM.
     *
     * @param operationId Operation Id.
     * @param shardMap    Shard map to remove shard from.
     * @param shard       Shard to update.
     * @return Xml formatted request.
     */
    public static XElement UpdateShardLocal(UUID operationId, IStoreShardMap shardMap, IStoreShard shard) {
        return new XElement("UpdateShardLocal", StoreOperationRequestBuilder.OperationId(operationId), StoreOperationRequestBuilder.s_lsmVersion, StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap), StoreObjectFormatterXml.WriteIStoreShard("Shard", shard));
    }

    /**
     * Request to get all shard mappings from LSM for a particular shard map
     * and optional shard and range.
     *
     * @param shardMap Shard map whose mappings are being requested.
     * @param shard    Optional shard for which mappings are being requested.
     * @param range    Optional range for which mappings are being requested.
     * @return Xml formatted request.
     */
    public static XElement GetAllShardMappingsLocal(IStoreShardMap shardMap, IStoreShard shard, ShardRange range) {
        return new XElement("GetAllShardMappingsLocal", StoreOperationRequestBuilder.s_lsmVersion, StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap), StoreObjectFormatterXml.WriteIStoreShard("Shard", shard), StoreObjectFormatterXml.WriteShardRange(range));
    }

    /**
     * Request to get mapping from LSM for a particular key belonging to a shard map.
     *
     * @param shardMap Shard map whose mappings are being requested.
     * @param key      Key being searched.
     * @return Xml formatted request.
     */
    public static XElement FindShardMappingByKeyLocal(IStoreShardMap shardMap, ShardKey key) {
        return new XElement("FindShardMappingByKeyLocal", StoreOperationRequestBuilder.s_lsmVersion, StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap), StoreObjectFormatterXml.WriteShardKey(key));
    }

    /**
     * Validation request for shard mapping for LSM.
     *
     * @param shardMapId Shard map Id.
     * @param mappingId  Shard mapping Id.
     * @return Xml formatted request.
     */
    public static XElement ValidateShardMappingLocal(UUID shardMapId, UUID mappingId) {
        return new XElement("ValidateShardMappingLocal", StoreOperationRequestBuilder.s_lsmVersion, new XElement("ShardMapId", shardMapId), new XElement("MappingId", mappingId));
    }

    /**
     * Request to add mapping to given shard map in LSM.
     *
     * @param operationId Operation Id.
     * @param shardMap    Shard map for which operation is being requested.
     * @param mapping     Mapping to add.
     * @param undo        Whether this is undo request.
     * @return Xml formatted request.
     */
    public static XElement AddShardMappingLocal(UUID operationId, boolean undo, IStoreShardMap shardMap, IStoreMapping mapping) {
        return new XElement("BulkOperationShardMappingsLocal", StoreOperationRequestBuilder.OperationId(operationId), StoreOperationRequestBuilder.Undo(undo), StoreOperationRequestBuilder.StepsCount(1), StoreOperationRequestBuilder.s_lsmVersion, StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap), StoreObjectFormatterXml.WriteIStoreShard("Shard", mapping.StoreShard), new XElement("Steps", new XElement("Step", new XAttribute("Id", 1), StoreOperationRequestBuilder.StepKind(StoreOperationStepKind.Add), StoreObjectFormatterXml.WriteIStoreMapping("Mapping", mapping))));
    }

    /**
     * Request to remove mapping from given shard map in LSM.
     *
     * @param operationId Operation Id.
     * @param shardMap    Shard map for which operation is being requested.
     * @param mapping     Mapping to remove.
     * @param undo        Whether this is undo operation.
     * @return Xml formatted request.
     */
    public static XElement RemoveShardMappingLocal(UUID operationId, boolean undo, IStoreShardMap shardMap, IStoreMapping mapping) {
        return new XElement("BulkOperationShardMappingsLocal", StoreOperationRequestBuilder.OperationId(operationId), StoreOperationRequestBuilder.Undo(undo), StoreOperationRequestBuilder.StepsCount(1), StoreOperationRequestBuilder.s_lsmVersion, StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap), StoreObjectFormatterXml.WriteIStoreShard("Shard", mapping.StoreShard), new XElement("Steps", new XElement("Step", new XAttribute("Id", 1), StoreOperationRequestBuilder.StepKind(StoreOperationStepKind.Remove), StoreObjectFormatterXml.WriteIStoreMapping("Mapping", mapping))));
    }

    /**
     * Request to update mapping in given shard map in LSM.
     *
     * @param operationId   Operation Id.
     * @param shardMap      Shard map for which operation is being requested.
     * @param mappingSource Mapping to update.
     * @param mappingTarget Updated mapping.
     * @param undo          Whether this is undo request.
     * @return Xml formatted request.
     */
    public static XElement UpdateShardMappingLocal(UUID operationId, boolean undo, IStoreShardMap shardMap, IStoreMapping mappingSource, IStoreMapping mappingTarget) {
        return new XElement("BulkOperationShardMappingsLocal", StoreOperationRequestBuilder.OperationId(operationId), StoreOperationRequestBuilder.Undo(undo), StoreOperationRequestBuilder.StepsCount(2), StoreOperationRequestBuilder.s_lsmVersion, StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap), StoreObjectFormatterXml.WriteIStoreShard("Shard", mappingTarget.StoreShard), new XElement("Steps", new XElement("Step", new XAttribute("Id", 1), StoreOperationRequestBuilder.StepKind(StoreOperationStepKind.Remove), StoreObjectFormatterXml.WriteIStoreMapping("Mapping", mappingSource)), new XElement("Step", new XAttribute("Id", 2), StoreOperationRequestBuilder.StepKind(StoreOperationStepKind.Add), StoreObjectFormatterXml.WriteIStoreMapping("Mapping", mappingTarget))));
    }

    /**
     * Request to replace mapping in given shard map in LSM.
     *
     * @param operationId    Operation Id.
     * @param shardMap       Shard map for which operation is being requested.
     * @param mappingsSource Mappings to remove.
     * @param mappingsTarget Mappings to add.
     * @param undo           Whether this is undo request.
     * @return Xml formatted request.
     */
    public static XElement ReplaceShardMappingsLocal(UUID operationId, boolean undo, IStoreShardMap shardMap, IStoreMapping[] mappingsSource, IStoreMapping[] mappingsTarget) {
        //Debug.Assert(mappingsSource.length + mappingsTarget.length > 0, "Expecting at least one mapping for ReplaceMappingsLocal.");
        return new XElement("BulkOperationShardMappingsLocal", StoreOperationRequestBuilder.OperationId(operationId), StoreOperationRequestBuilder.Undo(undo), StoreOperationRequestBuilder.StepsCount(mappingsSource.length + mappingsTarget.length), StoreOperationRequestBuilder.s_lsmVersion, StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap), StoreObjectFormatterXml.WriteIStoreShard("Shard", mappingsTarget.length > 0 ? mappingsTarget[0].StoreShard : mappingsSource[0].StoreShard), new XElement("Steps", mappingsSource.Select((m, i) -> new XElement("Step", new XAttribute("Id", i + 1), StoreOperationRequestBuilder.StepKind(StoreOperationStepKind.Remove), StoreObjectFormatterXml.WriteIStoreMapping("Mapping", m))), mappingsTarget.Select((m, i) -> new XElement("Step", new XAttribute("Id", mappingsSource.length + i + 1), StoreOperationRequestBuilder.StepKind(StoreOperationStepKind.Add), StoreObjectFormatterXml.WriteIStoreMapping("Mapping", m)))));
    }

    /**
     * Request to kill sessions with given application name pattern in LSM.
     *
     * @param pattern Pattern for application name.
     * @return Xml formatted request.
     */
    public static XElement KillSessionsForShardMappingLocal(String pattern) {
        return new XElement("KillSessionsForShardMappingLocal", StoreOperationRequestBuilder.s_lsmVersion, new XElement("Pattern", pattern));
    }

    /**
     * Adds OperationId attribute.
     *
     * @param operationId Id of operation.
     * @return XAttribute for the operationId.
     */
    private static XAttribute OperationId(UUID operationId) {
        return new XAttribute("OperationId", operationId);
    }

    /**
     * Adds UndoStartState attribute.
     *
     * @param undoStartState Number of remove steps.
     * @return XAttribute for the removeStepsCount.
     */
    private static XAttribute UndoStartState(StoreOperationState undoStartState) {
        return new XAttribute("UndoStartState", (int) undoStartState);
    }

    /**
     * Adds OperationCode attribute.
     *
     * @param operationCode Code of operation.
     * @return XAttribute for the operationCode.
     */
    private static XAttribute OperationCode(StoreOperationCode operationCode) {
        return new XAttribute("OperationCode", (int) operationCode);
    }

    /**
     * Adds StepsCount attribute.
     *
     * @param stepsCount Number of steps.
     * @return XAttribute for the StepsCount.
     */
    private static XAttribute StepsCount(int stepsCount) {
        return new XAttribute("StepsCount", stepsCount);
    }

    /**
     * Adds StepKind attribute.
     *
     * @param kind Type of step.
     * @return XAttribute for the StepKind.
     */
    private static XAttribute StepKind(StoreOperationStepKind kind) {
        return new XAttribute("Kind", kind.getValue());
    }

    /**
     * Adds RemoveStepsCount attribute.
     *
     * @param removeStepsCount Number of remove steps.
     * @return XAttribute for the removeStepsCount.
     */
    private static XAttribute RemoveStepsCount(int removeStepsCount) {
        return new XAttribute("RemoveStepsCount", removeStepsCount);
    }

    /**
     * Adds AddStepsCount attribute.
     *
     * @param addStepsCount Number of add steps.
     * @return XAttribute for the addStepsCount.
     */
    private static XAttribute AddStepsCount(int addStepsCount) {
        return new XAttribute("AddStepsCount", addStepsCount);
    }

    /**
     * Adds Undo attribute.
     *
     * @param undo Undo request.
     * @return XAttribute for the undo.
     */
    private static XAttribute Undo(boolean undo) {
        return new XAttribute("Undo", undo ? 1 : 0);
    }

    /**
     * Adds Validate attribute.
     *
     * @param validate Validate request.
     * @return XAttribute for the validation.
     */
    private static XAttribute Validate(boolean validate) {
        return new XAttribute("Validate", validate ? 1 : 0);
    }

    /**
     * Step kind for for Bulk Operations.
     */
    private enum StoreOperationStepKind {
        /**
         * Remove operation.
         */
        Remove(1),

        /**
         * Update operation.
         */
        Update(2),

        /**
         * Add operation.
         */
        Add(3);

        public static final int SIZE = java.lang.Integer.SIZE;
        private static java.util.HashMap<Integer, StoreOperationStepKind> mappings;
        private int intValue;

        private StoreOperationStepKind(int value) {
            intValue = value;
            getMappings().put(value, this);
        }

        private static java.util.HashMap<Integer, StoreOperationStepKind> getMappings() {
            if (mappings == null) {
                synchronized (StoreOperationStepKind.class) {
                    if (mappings == null) {
                        mappings = new java.util.HashMap<Integer, StoreOperationStepKind>();
                    }
                }
            }
            return mappings;
        }

        public static StoreOperationStepKind forValue(int value) {
            return getMappings().get(value);
        }

        public int getValue() {
            return intValue;
        }
    }

    ///#endregion Methods
}