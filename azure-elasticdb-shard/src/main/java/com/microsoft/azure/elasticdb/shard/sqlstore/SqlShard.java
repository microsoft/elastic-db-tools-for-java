package com.microsoft.azure.elasticdb.shard.sqlstore;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.store.StoreShard;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

/**
 * SQL backed storage representation of a shard.
 */
public final class SqlShard {

    /**
     * Constructs an instance of StoreShard using parts of a row from SqlDataReader.
     * Used for creating the shard instance for a mapping.
     *
     * @param reader SqlDataReader whose row has shard information.
     * @param offset Reader offset for column that begins shard information.
     */
    public static StoreShard newInstance(ResultSet reader, int offset) throws SQLException {
        return new StoreShard(UUID.fromString(reader.getString((offset)))
                , UUID.fromString(reader.getString(offset + 1))
                , UUID.fromString(reader.getString(offset + 2))
                , new SqlLocation(reader, offset + 3).getLocation()
                , reader.getInt(offset + 7));
    }

}