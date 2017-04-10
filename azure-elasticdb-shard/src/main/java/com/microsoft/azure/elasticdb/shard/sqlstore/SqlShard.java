package com.microsoft.azure.elasticdb.shard.sqlstore;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.store.IStoreShard;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

/**
 * SQL backed storage representation of a shard.
 */
public final class SqlShard implements IStoreShard {
    /**
     * Shard Id.
     */
    private UUID Id;
    /**
     * Shard version.
     */
    private UUID Version;
    /**
     * Containing shard map's Id.
     */
    private UUID ShardMapId;
    /**
     * Data source location.
     */
    private ShardLocation Location;
    /**
     * Shard status.
     */
    private int Status;

    /**
     * Constructs an instance of IStoreShard using parts of a row from SqlDataReader.
     * Used for creating the shard instance for a mapping.
     *
     * @param reader SqlDataReader whose row has shard information.
     * @param offset Reader offset for column that begins shard information.
     */
    public SqlShard(ResultSet reader, int offset) throws SQLException {
        this.setId(UUID.fromString(reader.getString((offset))));
        this.setVersion(UUID.fromString(reader.getString(offset + 1)));
        this.setShardMapId(UUID.fromString(reader.getString(offset + 2)));
        this.setLocation((new SqlLocation(reader, offset + 3)).getLocation());
        this.setStatus(reader.getInt(offset + 7));
    }

    public UUID getId() {
        return Id;
    }

    private void setId(UUID value) {
        Id = value;
    }

    public UUID getVersion() {
        return Version;
    }

    private void setVersion(UUID value) {
        Version = value;
    }

    public UUID getShardMapId() {
        return ShardMapId;
    }

    private void setShardMapId(UUID value) {
        ShardMapId = value;
    }

    public ShardLocation getLocation() {
        return Location;
    }

    private void setLocation(ShardLocation value) {
        Location = value;
    }

    public int getStatus() {
        return Status;
    }

    private void setStatus(int value) {
        Status = value;
    }

    @Override
    public int isNull() {
        return 0;
    }
}