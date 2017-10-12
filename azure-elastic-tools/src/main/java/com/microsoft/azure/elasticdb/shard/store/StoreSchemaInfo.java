package com.microsoft.azure.elasticdb.shard.store;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.shard.schema.SchemaInfo;

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
    private String name;
    /**
     * Schema info represented in XML.
     */
    @XmlElement(name = "Info")
    private Info shardingSchemaInfo;

    public StoreSchemaInfo() {
    }

    /**
     * Constructs the storage representation from client side objects.
     *
     * @param name
     *            Schema info name.
     * @param shardingSchemaInfo
     *            Schema info represented in XML.
     */
    public StoreSchemaInfo(String name,
            SchemaInfo shardingSchemaInfo) {
        this.setName(name);
        this.shardingSchemaInfo = new Info(shardingSchemaInfo);
    }

    public String getName() {
        return name;
    }

    private void setName(String value) {
        name = value;
    }

    public SchemaInfo getShardingSchemaInfo() {
        return shardingSchemaInfo.getSchemaInfo();
    }

    static class Info {

        @XmlElement(name = "Schema")
        private SchemaInfo schemaInfo;

        public Info() {
        }

        public Info(SchemaInfo schemaInfo) {
            this.schemaInfo = schemaInfo;
        }

        public SchemaInfo getSchemaInfo() {
            return schemaInfo;
        }
    }
}
