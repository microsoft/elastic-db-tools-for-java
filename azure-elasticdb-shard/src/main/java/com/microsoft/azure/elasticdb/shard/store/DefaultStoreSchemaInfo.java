package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.


/**
 * Used for generating storage representation from client side mapping objects.
 */
public final class DefaultStoreSchemaInfo implements IStoreSchemaInfo {
    /**
     * Schema info name.
     */
    private String Name;
    /**
     * Schema info represented in XML.
     */
    private SqlXml ShardingSchemaInfo;

    /**
     * Constructs the storage representation from client side objects.
     *
     * @param name               Schema info name.
     * @param shardingSchemaInfo Schema info represented in XML.
     */
    public DefaultStoreSchemaInfo(String name, SqlXml shardingSchemaInfo) {
        this.setName(name);
        this.setShardingSchemaInfo(shardingSchemaInfo);
    }

    public String getName() {
        return Name;
    }

    private void setName(String value) {
        Name = value;
    }

    public SqlXml getShardingSchemaInfo() {
        return ShardingSchemaInfo;
    }

    private void setShardingSchemaInfo(SqlXml value) {
        ShardingSchemaInfo = value;
    }
}