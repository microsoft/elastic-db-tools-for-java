package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import java.sql.SQLXML;

/**
 * Storage representation of a shard schema info.
 */
public class StoreSchemaInfo {
    public static final StoreSchemaInfo NULL = new StoreSchemaInfo("", null);
    /**
     * Schema info name.
     */
    private String Name;
    /**
     * Schema info represented in XML.
     */
    private SQLXML shardingSchemaInfo;

    public StoreSchemaInfo(){}
    /**
     * Constructs the storage representation from client side objects.
     *
     * @param name               Schema info name.
     * @param shardingSchemaInfo Schema info represented in XML.
     */
    public StoreSchemaInfo(String name, SQLXML shardingSchemaInfo) {
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
        return shardingSchemaInfo;
    }

    private void setShardingSchemaInfo(SQLXML shardingSchemaInfo) {
        this.shardingSchemaInfo = shardingSchemaInfo;
    }
}