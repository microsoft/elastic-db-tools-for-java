package com.microsoft.azure.elasticdb.shard.storeops.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.LockOwnerIdOpType;
import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.store.*;
import com.microsoft.azure.elasticdb.shard.utils.GlobalConstants;
import org.apache.commons.lang3.tuple.Pair;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.UUID;

@XmlRootElement
public class StoreOperationInput {
    public static final Version s_global
            = new Version(GlobalConstants.GsmVersionClient.getMajor(), GlobalConstants.GsmVersionClient.getMinor());

    public static final Version s_local
            = new Version(GlobalConstants.LsmVersionClient.getMajor(), GlobalConstants.LsmVersionClient.getMinor());

    @XmlElement(name = "GsmVersion")
    private Version gsmVersion;

    @XmlElement(name = "LsmVersion")
    private Version lsmVersion;

    @XmlAttribute(name = "OperationId")
    private UUID operationId;

    @XmlAttribute(name = "UndoStartState")
    private Integer undoStartState;

    @XmlElement(name = "ShardMap")
    private StoreShardMap shardMap;

    @XmlElement(name = "Location")
    private ShardLocation location;

    @XmlElement(name = "OperationCode")
    private StoreOperationCode operationCode;

    @XmlElement(name = "Undo")
    private Boolean undo;

    @XmlElement(name = "Shard")
    private StoreShard shard;

    @XmlElement(name = "ShardOld")
    private StoreShard shardOld;

    @XmlElement(name = "ShardRange")
    private ShardRange range;

    @XmlElement(name = "Key")
    private ShardKey key;

    @XmlElement(name = "Mapping")
    private StoreMapping mapping;

    @XmlElement(name = "MappingTarget")
    private StoreMapping mappingTarget;

    @XmlElement(name = "LockOwnerId")
    private UUID lockOwnerId;

    @XmlElement(name = "PatternForKill")
    private String patternForKill;

    @XmlElement(name = "MappingsSource")
    private Pair<StoreMapping, UUID>[] mappingsSource;

    @XmlElement(name = "MappingsTarget")
    private Pair<StoreMapping, UUID>[] mappingsTarget;

    @XmlElement(name = "LockOpType")
    private LockOwnerIdOpType lockOpType;

    @XmlElement(name = "SchemaInfo")
    private StoreSchemaInfo schemaInfo;

    @XmlElement(name = "ShardMapId")
    private UUID shardMapId;

    @XmlElement(name = "ShardId")
    private UUID shardId;

    @XmlElement(name = "ShardVersion")
    private UUID shardVersion;

    @XmlElement(name = "MappingId")
    private UUID mappingId;

    @XmlElement(name = "MappingsSourceArray")
    private StoreMapping[] mappingsSourceArray;

    @XmlElement(name = "MappingsTargetArray")
    private StoreMapping[] mappingsTargetArray;

    @XmlElement(name = "StepsCount")
    private Integer stepsCount;


    private StoreOperationInput() {
    }

    public static class Builder {
        private StoreOperationInput input = new StoreOperationInput();

        public Builder() {
        }

        public Builder withGsmVersion() {
            input.gsmVersion = s_global;
            return this;
        }

        public Builder withLsmVersion() {
            input.lsmVersion = s_local;
            return this;
        }

        public Builder withOperationId(UUID operationId) {
            input.operationId = operationId;
            return this;
        }

        public Builder withUndoStartState(StoreOperationState undoStartState) {
            input.undoStartState = undoStartState.getValue();
            return this;
        }

        public Builder withShardMap(StoreShardMap shardMap) {
            input.shardMap = shardMap;
            return this;
        }

        public Builder withLocation(ShardLocation location) {
            input.location = location;
            return this;
        }

        public Builder withOperationCode(StoreOperationCode operationCode) {
            input.operationCode = operationCode;
            return this;
        }

        public Builder withUndo(boolean undo) {
            input.undo = undo;
            return this;
        }

        public Builder withShard(StoreShard shard) {
            input.shard = shard;
            return this;
        }

        public Builder withIStoreShardOld(StoreShard shardOld) {
            input.shardOld = shardOld;
            return this;
        }

        public Builder withShardRange(ShardRange range) {
            input.range = range;
            return this;
        }

        public Builder withShardKey(ShardKey key) {
            input.key = key;
            return this;
        }

        public Builder withMapping(StoreMapping mapping) {
            input.mapping = mapping;
            return this;
        }

        public Builder withMappingTarget(StoreMapping mappingTarget) {
            input.mappingTarget = mappingTarget;
            return this;
        }

        public Builder withLockOwnerId(UUID lockOwnerId) {
            input.lockOwnerId = lockOwnerId;
            return this;
        }

        public Builder withPatternForKill(String patternForKill) {
            input.patternForKill = patternForKill;
            return this;
        }

        public Builder withMappingsSource(Pair<StoreMapping, UUID>[] mappingsSource) {
            input.mappingsSource = mappingsSource;
            return this;
        }

        public Builder withMappingsTarget(Pair<StoreMapping, UUID>[] mappingsTarget) {
            input.mappingsTarget = mappingsTarget;
            return this;
        }

        public Builder withLockOwnerIdOpType(LockOwnerIdOpType lockOpType) {
            input.lockOpType = lockOpType;
            return this;
        }

        public Builder withSchemaInfo(StoreSchemaInfo schemaInfo) {
            input.schemaInfo = schemaInfo;
            return this;
        }

        public Builder withShardMapId(UUID shardMapId) {
            input.shardMapId = shardMapId;
            return this;
        }

        public Builder withShardId(UUID shardId) {
            input.shardId = shardId;
            return this;
        }

        public Builder withShardVersion(UUID shardVersion) {
            input.shardVersion = shardVersion;
            return this;
        }

        public Builder withMappingId(UUID mappingId) {
            input.mappingId = mappingId;
            return this;
        }

        public Builder withMappingsSourceArray(StoreMapping[] mappingsSourceArray) {
            input.mappingsSourceArray = mappingsSourceArray;
            return this;
        }

        public Builder withMappingsTargetArray(StoreMapping[] mappingsTargetArray) {
            input.mappingsTargetArray = mappingsTargetArray;
            return this;
        }

        public Builder withStepsCount(int stepsCount) {
            input.stepsCount = stepsCount;
            return this;
        }

        public Builder withStoreOperationCode(StoreOperationCode operationCode) {
            input.operationCode = operationCode;
            return this;
        }

        public Builder withIStoreShard(StoreShard shard) {
            input.shard = shard;
            return this;
        }

        public Builder withIStoreMapping(StoreMapping mapping) {
            input.mapping = mapping;
            return this;
        }

        public Builder withIStoreMappingTarget(StoreMapping mappingTarget) {
            input.mappingTarget = mappingTarget;
            return this;
        }

        public StoreOperationInput build() {
            return input;
        }
    }
}


