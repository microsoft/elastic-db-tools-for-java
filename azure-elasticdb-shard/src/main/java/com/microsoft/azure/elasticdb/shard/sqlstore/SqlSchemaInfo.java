package com.microsoft.azure.elasticdb.shard.sqlstore;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.store.IStoreSchemaInfo;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;

/**
 * SQL backed storage representation of a schema info object.
 */
public final class SqlSchemaInfo implements IStoreSchemaInfo {
    /**
     * Schema info name.
     */
    private String Name;
    /**
     * Schema info represented in XML.
     */
    private SQLXML ShardingSchemaInfo;

    /**
     * Constructs an instance of IStoreSchemaInfo using parts of a row from SqlDataReader.
     *
     * @param reader SqlDataReader whose row has shard information.
     * @param offset Reader offset for column that begins shard information.
     */
    public SqlSchemaInfo(ResultSet reader, int offset) throws SQLException {
        this.setName(reader.getString(offset));
        this.setShardingSchemaInfo(reader.getSQLXML(offset + 1));
    }

    public String getName() {
        return Name;
    }

    private void setName(String value) {
        Name = value;
    }

    public SQLXML getShardingSchemaInfo() {
        return ShardingSchemaInfo;
    }

    private void setShardingSchemaInfo(SQLXML value) {
        ShardingSchemaInfo = value;
    }
}