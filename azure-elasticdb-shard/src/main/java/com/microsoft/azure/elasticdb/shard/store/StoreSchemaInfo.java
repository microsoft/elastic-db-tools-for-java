package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.schema.SchemaInfo;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

/**
 * Storage representation of a shard schema info.
 */
@XmlAccessorType(XmlAccessType.NONE)
public class StoreSchemaInfo {
    public static final StoreSchemaInfo NULL = new StoreSchemaInfo("", null);
    /**
     * Schema info name.
     */
    @XmlElement(name = "Name")
    private String Name;
    /**
     * Schema info represented in XML.
     */
    @XmlElement(name = "Info")
    private SchemaInfo shardingSchemaInfo;

    public StoreSchemaInfo() {
    }

    /**
     * Constructs the storage representation from client side objects.
     *
     * @param name               Schema info name.
     * @param shardingSchemaInfo Schema info represented in XML.
     */
    public StoreSchemaInfo(String name, SchemaInfo shardingSchemaInfo) {
        this.setName(name);
        this.setShardingSchemaInfo(shardingSchemaInfo);
    }

    public String getName() {
        return Name;
    }

    private void setName(String value) {
        Name = value;
    }

    public SchemaInfo getShardingSchemaInfo() {
        return shardingSchemaInfo;
    }

    private void setShardingSchemaInfo(SchemaInfo shardingSchemaInfo) {
        this.shardingSchemaInfo = shardingSchemaInfo;
    }
}