package com.microsoft.azure.elasticdb.shard.sqlstore;

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.store.IStoreShard;

import java.sql.ResultSet;
import java.util.UUID;

class SqlShard implements IStoreShard {
    public SqlShard(ResultSet rs, int offset) {
    }

    @Override
    public UUID getId() {
        return null;
    }

    @Override
    public UUID getVersion() {
        return null;
    }

    @Override
    public UUID getShardMapId() {
        return null;
    }

    @Override
    public ShardLocation getLocation() {
        return null;
    }

    @Override
    public int getStatus() {
        return 0;
    }

    @Override
    public int isNull() {
        return 0;
    }
}
