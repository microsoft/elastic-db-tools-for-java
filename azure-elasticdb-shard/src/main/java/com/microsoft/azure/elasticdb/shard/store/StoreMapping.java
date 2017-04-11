package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.Shard;

import java.util.UUID;

/**
 * Storage representation of a mapping b/w key ranges and shards.
 */
public class StoreMapping {
    /**
     * Mapping Id.
     */
    private UUID Id;

    /**
     * Shard map Id.
     */
    private UUID ShardMapId;

    /**
     * Min value.
     */
    private byte[] MinValue;

    /**
     * Max value.
     */
    private byte[] MaxValue;

    /**
     * Mapping status.
     */
    private int Status;

    /**
     * The lock owner id of the mapping
     */
    private UUID LockOwnerId;

    /**
     * Shard referenced by mapping.
     */
    private StoreShard storeShard;

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
        this.setId(id);
        this.setShardMapId(s.getShardMapId());
        this.setMinValue(minValue);
        this.setMaxValue(maxValue);
        this.setStatus(status);
        this.setLockOwnerId(null);
        this.setStoreShard(s.getStoreShard());
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
    public StoreMapping(UUID id, UUID shardMapId, StoreShard storeShard, byte[] minValue, byte[] maxValue, int status, UUID lockOwnerId) {
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
        return MinValue;
    }

    private void setMinValue(byte[] value) {
        MinValue = value;
    }

    public byte[] getMaxValue() {
        return MaxValue;
    }

    private void setMaxValue(byte[] value) {
        MaxValue = value;
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
}