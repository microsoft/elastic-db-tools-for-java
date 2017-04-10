package com.microsoft.azure.elasticdb.shard.sqlstore;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.store.IStoreMapping;
import com.microsoft.azure.elasticdb.shard.store.IStoreShard;
import com.microsoft.azure.elasticdb.shard.utils.SqlUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

/**
 * SQL backed storage representation of a mapping b/w key ranges and shards.
 */
public final class SqlMapping implements IStoreMapping {
    /**
     * Mapping Id.
     */
    private UUID Id;
    /**
     * Shard map Id.
     */
    private UUID ShardMapId;
    /**
     * Min value.
     */
    private byte[] MinValue;
    /**
     * Max value.
     */
    private byte[] MaxValue;
    /**
     * Mapping status.
     */
    private int Status;
    /**
     * The lock owner id of this mapping
     */
    private UUID LockOwnerId;
    /**
     * Shard referenced by mapping. Null value means this mapping is local.
     */
    private IStoreShard StoreShard;

    /**
     * Constructs an instance of IStoreMapping using a row from SqlDataReader.
     *
     * @param reader SqlDataReader whose row has mapping information.
     * @param offset Reader offset for column that begins mapping information.
     */
    public SqlMapping(ResultSet reader, int offset) throws SQLException {
        this.setId(UUID.fromString(reader.getString(offset)));
        this.setShardMapId(UUID.fromString(reader.getString(offset + 1)));
        this.setMinValue(SqlUtils.ReadSqlBytes(reader, offset + 2));
        this.setMaxValue(SqlUtils.ReadSqlBytes(reader, offset + 3));
        this.setStatus(reader.getInt(offset + 4));
        this.setLockOwnerId(UUID.fromString(reader.getString(offset + 5)));
        this.setStoreShard(new SqlShard(reader, offset + 6));
    }

    public UUID getId() {
        return Id;
    }

    private void setId(UUID value) {
        Id = value;
    }

    public UUID getShardMapId() {
        return ShardMapId;
    }

    private void setShardMapId(UUID value) {
        ShardMapId = value;
    }

    public byte[] getMinValue() {
        return MinValue;
    }

    private void setMinValue(byte[] value) {
        MinValue = value;
    }

    public byte[] getMaxValue() {
        return MaxValue;
    }

    private void setMaxValue(byte[] value) {
        MaxValue = value;
    }

    public int getStatus() {
        return Status;
    }

    private void setStatus(int value) {
        Status = value;
    }

    public UUID getLockOwnerId() {
        return LockOwnerId;
    }

    private void setLockOwnerId(UUID value) {
        LockOwnerId = value;
    }

    public IStoreShard getStoreShard() {
        return StoreShard;
    }

    private void setStoreShard(IStoreShard value) {
        StoreShard = value;
    }
}