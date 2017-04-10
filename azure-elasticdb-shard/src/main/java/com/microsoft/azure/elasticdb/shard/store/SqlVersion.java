package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.utils.Version;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * SQL backed storage representation of shard map manager store version.
 */
public class SqlVersion implements IStoreVersion {
    /**
     * Store version.
     */
    private Version _version;

    /**
     * Constructs an instance of IStoreVersion using parts of a row from SqlDataReader.
     *
     * @param reader SqlDataReader whose row has shard information.
     * @param offset Reader offset for column that begins shard information.
     */
    public SqlVersion(ResultSet reader, int offset) throws SQLException {
        int Major = reader.getInt(offset);
        int Minor = (reader.getMetaData().getColumnCount() > offset) ? reader.getInt(offset + 1) : 0;
        this.setVersion(new Version(Major, Minor));
    }

    public final Version getVersion() {
        return _version;
    }

    private void setVersion(Version value) {
        _version = value;
    }
}