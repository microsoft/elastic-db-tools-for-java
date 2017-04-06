package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.


import java.sql.SQLXML;

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
    private SQLXML ShardingSchemaInfo;

    /**
     * Constructs the storage representation from client side objects.
     *
     * @param name               Schema info name.
     * @param shardingSchemaInfo Schema info represented in XML.
     */
    public DefaultStoreSchemaInfo(String name, SQLXML shardingSchemaInfo) {
        this.setName(name);
        this.setShardingSchemaInfo(shardingSchemaInfo);
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