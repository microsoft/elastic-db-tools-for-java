package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;

import java.util.UUID;

/**
 * Used for generating storage representation from client side shard objects.
 */
public final class DefaultStoreShard implements IStoreShard {
    /**
     * Shard Id.
     */
    private UUID Id;
    /**
     * Shard version.
     */
    private UUID Version;
    /**
     * Containing shard map's Id.
     */
    private UUID ShardMapId;
    /**
     * Data source location.
     */
    private ShardLocation Location;
    /**
     * Shard status.
     */
    private int Status;

    /**
     * Constructs the storage representation from client side objects.
     *
     * @param id         Shard Id.
     * @param version    Shard version.
     * @param shardMapId Identify of shard map.
     * @param location   Data source location.
     * @param status     Status of the shard.
     */
    public DefaultStoreShard(UUID id, UUID version, UUID shardMapId, ShardLocation location, int status) {
        this.setId(id);
        this.setVersion(version);
        this.setShardMapId(shardMapId);
        this.setLocation(location);
        this.setStatus(status);
    }

    public UUID getId() {
        return Id;
    }

    private void setId(UUID value) {
        Id = value;
    }

    public UUID getVersion() {
        return Version;
    }

    private void setVersion(UUID value) {
        Version = value;
    }

    public UUID getShardMapId() {
        return ShardMapId;
    }

    private void setShardMapId(UUID value) {
        ShardMapId = value;
    }

    public ShardLocation getLocation() {
        return Location;
    }

    private void setLocation(ShardLocation value) {
        Location = value;
    }

    public int getStatus() {
        return Status;
    }

    private void setStatus(int value) {
        Status = value;
    }
}