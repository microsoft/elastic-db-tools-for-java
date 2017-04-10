package com.microsoft.azure.elasticdb.shard.sqlstore;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.SqlProtocol;
import com.microsoft.azure.elasticdb.shard.store.IStoreLocation;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * SQL backed storage representation of a location.
 */
public final class SqlLocation implements IStoreLocation {
    /**
     * Data source location.
     */
    private ShardLocation location;

    /**
     * Constructs an instance of IStoreLocation using parts of a row from SqlDataReader.
     * Used for creating the shard location instance.
     *
     * @param reader SqlDataReader whose row has shard information.
     * @param offset Reader offset for column that begins shard information.
     */
    public SqlLocation(ResultSet reader, int offset) throws SQLException {
        this.setLocation(new ShardLocation(
                reader.getString(offset + 1),
                reader.getString(offset + 3),
                SqlProtocol.forValue(reader.getInt(offset)),
                reader.getInt(offset + 2)));
    }

    public ShardLocation getLocation() {
        return location;
    }

    private void setLocation(ShardLocation value) {
        location = value;
    }
}