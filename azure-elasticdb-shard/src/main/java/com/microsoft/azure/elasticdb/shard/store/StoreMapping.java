package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.Shard;

import javax.xml.bind.annotation.*;
import java.util.UUID;

/**
 * Storage representation of a mapping b/w key ranges and shards.
 */
@XmlAccessorType(XmlAccessType.NONE)
public class StoreMapping {
    /**
     * Mapping Id.
     */
    @XmlElement(name = "Id")
    private UUID Id;

    /**
     * Shard map Id.
     */
    @XmlElement(name = "ShardMapId")
    private UUID ShardMapId;

    /**
     * Min value.
     */
    @XmlElement(name = "MinValue")
    private BinaryValue MinValue;

    /**
     * Max value.
     */
    @XmlElement(name = "MaxValue")
    private BinaryValue MaxValue;

    /**
     * Mapping status.
     */
    @XmlElement(name = "Status")
    private int Status;

    /**
     * The lock owner id of the mapping
     */
    private UUID LockOwnerId;

    /**
     * Shard referenced by mapping.
     */
    private StoreShard storeShard;

    public StoreMapping() {
    }

    /**
     * Constructs the storage representation from client side objects.
     *
     * @param id       Identify of mapping.
     * @param s        Shard being converted.
     * @param minValue Min key value.
     * @param maxValue Max key value.
     * @param status   Mapping status.
     */
    public StoreMapping(UUID id, Shard s, byte[] minValue, byte[] maxValue, int status) {
        this(id, s.getShardMapId(), minValue, maxValue, status, null, s.getStoreShard());
    }

    /**
     * Constructs the storage representation from client side objects.
     *
     * @param id          Identify of mapping.
     * @param shardMapId  Id of parent shardmap.
     * @param storeShard  StoreShard
     * @param minValue    Min key value.
     * @param maxValue    Max key value.
     * @param status      Mapping status.
     * @param lockOwnerId Lock owner id.
     */
    public StoreMapping(UUID id, UUID shardMapId, byte[] minValue, byte[] maxValue, int status, UUID lockOwnerId, StoreShard storeShard) {
        this.setId(id);
        this.setShardMapId(shardMapId);
        this.setStoreShard(storeShard);
        this.setMinValue(minValue);
        this.setMaxValue(maxValue);
        this.setStatus(status);
        this.setLockOwnerId(lockOwnerId);
    }

    public UUID getId() {
        return Id;
    }

    private void setId(UUID value) {
        Id = value;
    }

    public UUID getShardMapId() {
        return ShardMapId;
    }

    private void setShardMapId(UUID value) {
        ShardMapId = value;
    }

    public byte[] getMinValue() {
        return MinValue.getValue();
    }

    private void setMinValue(byte[] value) {
        MinValue = new BinaryValue(value);
    }

    public byte[] getMaxValue() {
        return MaxValue.getValue();
    }

    private void setMaxValue(byte[] value) {
        MaxValue = new BinaryValue(value);
    }

    public int getStatus() {
        return Status;
    }

    private void setStatus(int value) {
        Status = value;
    }

    public UUID getLockOwnerId() {
        return LockOwnerId;
    }

    private void setLockOwnerId(UUID value) {
        LockOwnerId = value;
    }

    public StoreShard getStoreShard() {
        return storeShard;
    }

    private void setStoreShard(StoreShard value) {
        storeShard = value;
    }

    @XmlElement(name = "LockOwnerId")
    public String getLockOwnerIdString() {
        return this.LockOwnerId == null ? new UUID(0L, 0L).toString() : this.LockOwnerId.toString();
    }

    static class BinaryValue {
        private byte[] value;

        @XmlAttribute(name = "Null")
        private int isNull;

        BinaryValue() {
        }

        BinaryValue(byte[] value) {
            this.value = value;
            isNull = value == null ? 1 : 0;
        }

        public byte[] getValue() {
            return this.value;
        }

        @XmlValue
        public String getValueString() {
            return this.toString();
        }

        @Override
        public String toString() {
            if (this.value != null) {
                StringBuilder sb = new StringBuilder();
                int i = 0;
                sb.append("0x");
                for (byte b : this.value) {
                    sb.append(String.format("%02x", b));
                }
                return sb.toString();
            } else {
                isNull = 1;
                return "";
            }
        }
    }
}