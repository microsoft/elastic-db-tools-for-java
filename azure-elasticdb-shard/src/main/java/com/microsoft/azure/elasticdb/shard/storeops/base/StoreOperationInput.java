package com.microsoft.azure.elasticdb.shard.storeops.base;

import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.store.IStoreMapping;
import com.microsoft.azure.elasticdb.shard.store.IStoreShard;
import com.microsoft.azure.elasticdb.shard.store.IStoreShardMap;
import com.microsoft.azure.elasticdb.shard.utils.GlobalConstants;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
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

        public Builder withStoreOperationCode(StoreOperationCode operationCode) {
            input.operationCode = operationCode;
            return this;
        }

        public Builder withUndo(boolean undo) {
            input.undo = undo;
            return this;
        }

        public Builder withIStoreShard(IStoreShard shard) {
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

        public Builder withIStoreMapping(IStoreMapping mapping) {
            input.mapping = mapping;
            return this;
        }

        public Builder withIStoreMappingTarget(IStoreMapping mappingTarget) {
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


