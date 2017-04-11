package com.microsoft.azure.elasticdb.shard.sqlstore;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.map.ShardMapType;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

/**
 * SQL based storage representation of a shard map.
 */
public class SqlShardMap extends StoreShardMap {
    /**
     * Constructs an instance of StoreShardMap using a row from SqlDataReader starting at specified offset.
     *
     * @param reader SqlDataReader whose row has shard map information.
     * @param offset Reader offset for column that begins shard map information..
     */
    public SqlShardMap(ResultSet reader, int offset) throws SQLException {
        super(UUID.fromString(reader.getString(offset)),
                reader.getString(offset + 1),
                ShardMapType.forValue(reader.getInt(offset + 2)),
                ShardKeyType.forValue(reader.getInt(offset + 3)));
    }

    public final UUID getId() {
        return super.getId();
    }

    public final String getName() {
        return super.getName();
    }

    public final ShardMapType getMapType() {
        return super.getMapType();
    }

    public final ShardKeyType getKeyType() {
        return super.getKeyType();
    }
}