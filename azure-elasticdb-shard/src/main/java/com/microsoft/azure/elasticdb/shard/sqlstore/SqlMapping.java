package com.microsoft.azure.elasticdb.shard.sqlstore;

import com.microsoft.azure.elasticdb.shard.store.IStoreMapping;
import com.microsoft.azure.elasticdb.shard.store.IStoreShard;

import java.sql.ResultSet;
import java.util.UUID;

class SqlMapping implements IStoreMapping {
    public SqlMapping(ResultSet rs, int offset) {
    }

    @Override
    public UUID getId() {
        return null;
    }

    @Override
    public UUID getShardMapId() {
        return null;
    }

    @Override
    public byte[] getMinValue() {
        return new byte[0];
    }

    @Override
    public byte[] getMaxValue() {
        return new byte[0];
    }

    @Override
    public int getStatus() {
        return 0;
    }

    @Override
    public UUID getLockOwnerId() {
        return null;
    }

    @Override
    public IStoreShard getStoreShard() {
        return null;
    }
}
