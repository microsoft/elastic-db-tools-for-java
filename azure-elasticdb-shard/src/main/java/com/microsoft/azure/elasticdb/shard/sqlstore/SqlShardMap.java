package com.microsoft.azure.elasticdb.shard.sqlstore;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.map.ShardMapType;
import com.microsoft.azure.elasticdb.shard.store.IStoreShardMap;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

/**
 * SQL based storage representation of a shard map.
 */
public class SqlShardMap implements IStoreShardMap {
    /**
     * Shard map's identity.
     */
    private UUID Id;
    /**
     * Shard map name.
     */
    private String Name;
    /**
     * Kind of shard map.
     */
    private ShardMapType MapType;
    /**
     * Key type.
     */
    private ShardKeyType KeyType;

    /**
     * Constructs an instance of IStoreShardMap using a row from SqlDataReader starting at specified offset.
     *
     * @param reader SqlDataReader whose row has shard map information.
     * @param offset Reader offset for column that begins shard map information..
     */
    public SqlShardMap(ResultSet reader, int offset) throws SQLException {
        this.setId(UUID.fromString(reader.getString(offset)));
        this.setName(reader.getString(offset + 1));
        this.setMapType(ShardMapType.forValue(reader.getInt(offset + 2)));
        this.setKeyType(ShardKeyType.forValue(reader.getInt(offset + 3)));
    }

    public final UUID getId() {
        return Id;
    }

    private void setId(UUID value) {
        Id = value;
    }

    public final String getName() {
        return Name;
    }

    private void setName(String value) {
        Name = value;
    }

    public final ShardMapType getMapType() {
        return MapType;
    }

    private void setMapType(ShardMapType value) {
        MapType = value;
    }

    public final ShardKeyType getKeyType() {
        return KeyType;
    }

    private void setKeyType(ShardKeyType value) {
        KeyType = value;
    }

    @Override
    public int isNull() {
        return 0;
    }
}