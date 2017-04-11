package com.microsoft.azure.elasticdb.shard.sqlstore;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.store.StoreSchemaInfo;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;

/**
 * SQL backed storage representation of a schema info object.
 */
public final class SqlSchemaInfo extends StoreSchemaInfo {
    /**
     * Constructs an instance of StoreSchemaInfo using parts of a row from SqlDataReader.
     *
     * @param reader SqlDataReader whose row has shard information.
     * @param offset Reader offset for column that begins shard information.
     */
    public SqlSchemaInfo(ResultSet reader, int offset) throws SQLException {
        super(reader.getString(offset), reader.getSQLXML(offset + 1));
    }

    public String getName() {
        return super.getName();
    }

    public SQLXML getShardingSchemaInfo() {
        return super.getShardingSchemaInfo();
    }
}