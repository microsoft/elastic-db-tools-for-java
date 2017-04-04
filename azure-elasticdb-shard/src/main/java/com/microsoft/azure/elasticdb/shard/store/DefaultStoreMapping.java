package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.Shard;

import java.util.UUID;

/**
 * Used for generating storage representation from client side mapping objects.
 */
public final class DefaultStoreMapping implements IStoreMapping {
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
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: private byte[] MinValue;
    private byte[] MinValue;
    /**
     * Max value.
     */
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: private byte[] MaxValue;
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
    private IStoreShard StoreShard;

    /**
     * Constructs the storage representation from client side objects.
     *
     * @param id       Identify of mapping.
     * @param s        Shard being converted.
     * @param minValue Min key value.
     * @param maxValue Max key value.
     * @param status   Mapping status.
     */
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: internal DefaultStoreMapping(Guid id, Shard s, byte[] minValue, byte[] maxValue, int status)
    public DefaultStoreMapping(UUID id, Shard s, byte[] minValue, byte[] maxValue, int status) {
        this.setId(id);
        this.setShardMapId(s.getShardMapId());
        this.setMinValue(minValue);
        this.setMaxValue(maxValue);
        this.setStatus(status);
        this.setLockOwnerId(null);

        this.setStoreShard(s.StoreShard);
    }

    /**
     * Constructs the storage representation from client side objects.
     *
     * @param id          Identify of mapping.
     * @param shardMapId  Id of parent shardmap.
     * @param storeShard  IStoreShard
     * @param minValue    Min key value.
     * @param maxValue    Max key value.
     * @param status      Mapping status.
     * @param lockOwnerId Lock owner id.
     */
//WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: internal DefaultStoreMapping(Guid id, Guid shardMapId, IStoreShard storeShard, byte[] minValue, byte[] maxValue, int status, Guid lockOwnerId)
    public DefaultStoreMapping(UUID id, UUID shardMapId, IStoreShard storeShard, byte[] minValue, byte[] maxValue, int status, UUID lockOwnerId) {
        this.setId(id);
        this.setShardMapId(shardMapId);
        this.setMinValue(minValue);
        this.setMaxValue(maxValue);
        this.setStatus(status);
        this.setLockOwnerId(lockOwnerId);

        this.setStoreShard(storeShard);
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

    //WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: public byte[] getMinValue()
    public byte[] getMinValue() {
        return MinValue;
    }

    //WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: private void setMinValue(byte[] value)
    private void setMinValue(byte[] value) {
        MinValue = value;
    }

    //WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: public byte[] getMaxValue()
    public byte[] getMaxValue() {
        return MaxValue;
    }

    //WARNING: Unsigned integer types have no direct equivalent in Java:
//ORIGINAL LINE: private void setMaxValue(byte[] value)
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

    public IStoreShard getStoreShard() {
        return StoreShard;
    }

    private void setStoreShard(IStoreShard value) {
        StoreShard = value;
    }
}