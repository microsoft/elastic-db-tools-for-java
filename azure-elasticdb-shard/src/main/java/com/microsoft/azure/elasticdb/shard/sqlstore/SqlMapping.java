package com.microsoft.azure.elasticdb.shard.sqlstore;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreShard;
import com.microsoft.azure.elasticdb.shard.utils.SqlUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

/**
 * SQL backed storage representation of a mapping b/w key ranges and shards.
 */
public final class SqlMapping extends StoreMapping {
    /**
     * Constructs an instance of StoreMapping using a row from SqlDataReader.
     *
     * @param reader SqlDataReader whose row has mapping information.
     * @param offset Reader offset for column that begins mapping information.
     */
    public SqlMapping(ResultSet reader, int offset) throws SQLException {
        super(UUID.fromString(reader.getString(offset)),
                UUID.fromString(reader.getString(offset + 1)),
                SqlShard.newInstance(reader, offset + 6),
                SqlUtils.ReadSqlBytes(reader, offset + 2),
                SqlUtils.ReadSqlBytes(reader, offset + 3),
                reader.getInt(offset + 4),
                UUID.fromString(reader.getString(offset + 5)));
    }

    public UUID getId() {
        return super.getId();
    }

    public UUID getShardMapId() {
        return super.getShardMapId();
    }

    public byte[] getMinValue() {
        return super.getMinValue();
    }

    public byte[] getMaxValue() {
        return super.getMaxValue();
    }

    public int getStatus() {
        return super.getStatus();
    }

    public UUID getLockOwnerId() {
        return super.getLockOwnerId();
    }

    public StoreShard getStoreShard() {
        return super.getStoreShard();
    }
}