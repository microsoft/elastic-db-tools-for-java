package com.microsoft.azure.elasticdb.shard.storeops.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.LockOwnerIdOpType;
import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.store.IStoreMapping;
import com.microsoft.azure.elasticdb.shard.store.IStoreSchemaInfo;
import com.microsoft.azure.elasticdb.shard.store.IStoreShard;
import com.microsoft.azure.elasticdb.shard.store.IStoreShardMap;
import com.microsoft.azure.elasticdb.shard.utils.GlobalConstants;
import org.apache.commons.lang3.tuple.Pair;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@XmlRootElement
public class StoreOperationInput {

    @XmlElement(name = "GsmVersion")
    private Version gsmVersion;

    @XmlElement(name = "LsmVersion")
    private Version lsmVersion;

    @XmlAttribute(name = "OperationId")
    private UUID operationId;

    @XmlAttribute(name = "UndoStartState")
    private int undoStartState;

    @XmlElement(name = "ShardMap")
    private IStoreShardMap shardMap;

    @XmlElement(name = "Location")
    private ShardLocation location;

    @XmlElement(name = "OperationCode")
    private StoreOperationCode operationCode;

    @XmlElement(name = "Undo")
    private boolean undo;

    @XmlElement(name = "Shard")
    private IStoreShard shard;

    @XmlElement(name = "ShardOld")
    private IStoreShard shardOld;

    @XmlElement(name = "ShardRange")
    private ShardRange range;

    @XmlElement(name = "ShardKey")
    private ShardKey key;

    @XmlElement(name = "Mapping")
    private IStoreMapping mapping;

    @XmlElement(name = "MappingTarget")
    private IStoreMapping mappingTarget;

    @XmlElement(name = "LockOwnerId")
    private UUID lockOwnerId;

    @XmlElement(name = "PatternForKill")
    private String patternForKill;

    @XmlElement(name = "MappingsSource")
    private Pair<IStoreMapping, UUID>[] mappingsSource;

    @XmlElement(name = "MappingsTarget")
    private Pair<IStoreMapping, UUID>[] mappingsTarget;

    @XmlElement(name = "LockOpType")
    private LockOwnerIdOpType lockOpType;

    @XmlElement(name = "SchemaInfo")
    private IStoreSchemaInfo schemaInfo;

    @XmlElement(name = "ShardMapId")
    private UUID shardMapId;

    @XmlElement(name = "ShardId")
    private UUID shardId;

    @XmlElement(name = "ShardVersion")
    private UUID shardVersion;

    @XmlElement(name = "MappingId")
    private UUID mappingId;

    @XmlElement(name = "MappingsSourceArray")
    private IStoreMapping[] mappingsSourceArray;

    @XmlElement(name = "MappingsTargetArray")
    private IStoreMapping[] mappingsTargetArray;

    @XmlElement(name = "StepsCount")
    private int stepsCount;

    @XmlElement(name = "Steps")
    private Map<Integer, StoreOperationStepKind> steps;

    private StoreOperationInput() {
    }

    public static class Builder {
        private StoreOperationInput input = new StoreOperationInput();

        public Builder() {
        }

        public Builder withGsmVersion() {
            input.gsmVersion = Version.s_global;
            return this;
        }

        public Builder withLsmVersion() {
            input.lsmVersion = Version.s_local;
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

        public Builder withShardMap(IStoreShardMap shardMap) {
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

        public Builder withShard(IStoreShard shard) {
            input.shard = shard;
            return this;
        }

        public Builder withIStoreShardOld(IStoreShard shardOld) {
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

        public Builder withMapping(IStoreMapping mapping) {
            input.mapping = mapping;
            return this;
        }

        public Builder withMappingTarget(IStoreMapping mappingTarget) {
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

        public Builder withMappingsSource(Pair<IStoreMapping, UUID>[] mappingsSource) {
            input.mappingsSource = mappingsSource;
            return this;
        }

        public Builder withMappingsTarget(Pair<IStoreMapping, UUID>[] mappingsTarget) {
            input.mappingsTarget = mappingsTarget;
            return this;
        }

        public Builder withLockOwnerIdOpType(LockOwnerIdOpType lockOpType) {
            input.lockOpType = lockOpType;
            return this;
        }

        public Builder withSchemaInfo(IStoreSchemaInfo schemaInfo) {
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

        public Builder withMappingsSourceArray(IStoreMapping[] mappingsSourceArray) {
            input.mappingsSourceArray = mappingsSourceArray;
            return this;
        }

        public Builder withMappingsTargetArray(IStoreMapping[] mappingsTargetArray) {
            input.mappingsTargetArray = mappingsTargetArray;
            return this;
        }

        public Builder withStepsCount(int stepsCount) {
            input.stepsCount = stepsCount;
            return this;
        }

        public Builder withSteps(HashMap<Integer, StoreOperationStepKind> steps) {

            input.steps = steps;
            return this;
        }

        public Builder withStoreOperationCode(StoreOperationCode operationCode) {
            input.operationCode = operationCode;
            return this;
        }

        public Builder withIStoreShard(IStoreShard shard) {
            input.shard = shard;
            return this;
        }

        public Builder withIStoreMapping(IStoreMapping mapping) {
            input.mapping = mapping;
            return this;
        }

        public Builder withIStoreMappingTarget(IStoreMapping mappingTarget) {
            input.mappingTarget = mappingTarget;
            return this;
        }

        public StoreOperationInput build() {
            return input;
        }
    }

    static class Version {
        public static final Version s_global
                = new Version(GlobalConstants.GsmVersionClient.getMajor(), GlobalConstants.GsmVersionClient.getMinor());

        public static final Version s_local
                = new Version(GlobalConstants.LsmVersionClient.getMajor(), GlobalConstants.LsmVersionClient.getMinor());
        @XmlElement(name = "MajorVersion")
        private int majorVersion;
        @XmlElement(name = "MinorVersion")
        private int minorVersion;

        public Version(int majorVersion, int minorVersion) {
            this.majorVersion = majorVersion;
            this.minorVersion = minorVersion;
        }
    }

}


