package com.microsoft.azure.elasticdb.shard.sqlstore;

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.SqlProtocol;
import com.microsoft.azure.elasticdb.shard.store.IStoreLocation;

import java.sql.ResultSet;
import java.sql.SQLException;

class SqlLocation implements IStoreLocation {
    private ShardLocation location;

    SqlLocation(ResultSet rs, int offset) throws SQLException {
        location = new ShardLocation(
                rs.getString(offset + 1),
                rs.getString(offset + 3),
                SqlProtocol.forValue(rs.getInt(offset)),
                rs.getInt(offset + 2));
    }

    @Override
    public ShardLocation getLocation() {
        return location;
    }
}
