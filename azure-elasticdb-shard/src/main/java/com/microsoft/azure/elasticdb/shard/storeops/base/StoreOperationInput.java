package com.microsoft.azure.elasticdb.shard.storeops.base;

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.store.IStoreShardMap;
import com.microsoft.azure.elasticdb.shard.utils.GlobalConstants;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import java.util.UUID;

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

    private StoreOperationInput(){}

    public static class Builder {
        private StoreOperationInput input = new StoreOperationInput();
        public Builder() {}
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
        public StoreOperationInput build() {
            return input;
        }
    }

    static class Version {
        public static final Version s_global
                = new Version(GlobalConstants.GsmVersionClient.getMajor(), GlobalConstants.GsmVersionClient.getMinor());

        public static final Version s_local
                = new Version(GlobalConstants.LsmVersionClient.getMajor(), GlobalConstants.LsmVersionClient.getMinor());

        public Version(int majorVersion, int minorVersion) {
            this.majorVersion = majorVersion;
            this.minorVersion = minorVersion;
        }

        @XmlElement(name = "MajorVersion")
        private int majorVersion;

        @XmlElement(name = "MinorVersion")
        private int minorVersion;
    }

}


