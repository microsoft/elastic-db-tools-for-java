package com.microsoft.azure.elasticdb.shard.sqlstore;

import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.map.ShardMapType;
import com.microsoft.azure.elasticdb.shard.store.IStoreShardMap;

import java.sql.ResultSet;
import java.util.UUID;

class SqlShardMap implements IStoreShardMap {
    public SqlShardMap(ResultSet rs, int offset) {
    }

    @Override
    public UUID getId() {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public ShardMapType getMapType() {
        return null;
    }

    @Override
    public ShardKeyType getKeyType() {
        return null;
    }

    @Override
    public int isNull() {
        return 0;
    }
}
