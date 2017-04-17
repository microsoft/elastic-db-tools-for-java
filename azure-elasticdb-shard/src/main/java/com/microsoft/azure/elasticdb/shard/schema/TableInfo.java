package com.microsoft.azure.elasticdb.shard.schema;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import javax.xml.bind.annotation.XmlElement;

/**
 * Repesents a table in a database.
 */
public abstract class TableInfo {
    /**
     * Table's schema name.
     */
    @XmlElement(name = "SchemaName")
    private String SchemaName;
    /**
     * Table name.
     */
    @XmlElement(name = "TableName")
    private String TableName;

    public final String getSchemaName() {
        return SchemaName;
    }

    protected final void setSchemaName(String value) {
        SchemaName = value;
    }

    public final String getTableName() {
        return TableName;
    }

    protected final void setTableName(String value) {
        TableName = value;
    }
}