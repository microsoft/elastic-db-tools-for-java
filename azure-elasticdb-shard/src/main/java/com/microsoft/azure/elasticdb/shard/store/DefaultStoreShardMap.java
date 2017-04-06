package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.map.ShardMapType;
import java.util.UUID;

/**
 * Storage representation of a shard map.
 */
public final class DefaultStoreShardMap implements IStoreShardMap {
    /**
     * Shard map's identity.
     */
    private UUID Id;
    /**
     * Shard map name.
     */
    private String Name;
    /**
     * Type of shard map.
     */
    private ShardMapType MapType;
    /**
     * Key type.
     */
    private ShardKeyType KeyType;
    /**
     * The id of the local shardmap. Null if a global shardmap.
     */
    private UUID ShardId = null;

    /**
     * Constructs an instance of DefaultStoreShardMap used for creating new shard maps.
     *
     * @param id      Shard map Id.
     * @param name    Shard map name.
     * @param mapType Shard map kind.
     * @param keyType Shard map key type.
     * @param shardId Optional argument for shardId if this instance is for a local shardmap.
     */

    public DefaultStoreShardMap(UUID id, String name, ShardMapType mapType, ShardKeyType keyType) {
        this(id, name, mapType, keyType, null);
    }

    public DefaultStoreShardMap(UUID id, String name, ShardMapType mapType, ShardKeyType keyType, UUID shardId) {
        this.setId(id);
        this.setName(name);
        this.setMapType(mapType);
        this.setKeyType(keyType);
        this.setShardId(shardId);
    }

    public UUID getId() {
        return Id;
    }

    private void setId(UUID value) {
        Id = value;
    }

    public String getName() {
        return Name;
    }

    private void setName(String value) {
        Name = value;
    }

    public ShardMapType getMapType() {
        return MapType;
    }

    private void setMapType(ShardMapType value) {
        MapType = value;
    }

    public ShardKeyType getKeyType() {
        return KeyType;
    }

    @Override
    public int isNull() {
        return 0;
    }

    private void setKeyType(ShardKeyType value) {
        KeyType = value;
    }

    public UUID getShardId() {
        return ShardId;
    }

    private void setShardId(UUID value) {
        ShardId = value;
    }
}