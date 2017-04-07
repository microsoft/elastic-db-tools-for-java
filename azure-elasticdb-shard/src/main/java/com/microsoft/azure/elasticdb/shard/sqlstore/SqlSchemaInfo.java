package com.microsoft.azure.elasticdb.shard.sqlstore;

import com.microsoft.azure.elasticdb.shard.store.IStoreSchemaInfo;

import java.sql.ResultSet;
import java.sql.SQLXML;

class SqlSchemaInfo implements IStoreSchemaInfo {
    public SqlSchemaInfo(ResultSet rs, int offset) {
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public SQLXML getShardingSchemaInfo() {
        return null;
    }
}
