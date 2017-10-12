package com.microsoft.azure.elasticdb.shard.schema;

import java.io.Serializable;

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

import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;

/**
 * Represents information about a single sharded table.
 */
@XmlAccessorType(XmlAccessType.NONE)
public class ShardedTableInfo extends TableInfo implements Serializable {

    /**
     * Name of the shard key column.
     */
    @XmlElement(name = "KeyColumnName")
    private String keyColumnName;

    /**
     * Initializes a new instance of the <see cref="ShardedTableInfo"/> class.
     *
     * @param tableName
     *            Sharded table name.
     * @param columnName
     *            Shard key column name.
     */
    public ShardedTableInfo(String tableName,
            String columnName) {
        ExceptionUtils.disallowNullOrEmptyStringArgument(tableName, "tableName");
        ExceptionUtils.disallowNullOrEmptyStringArgument(columnName, "columnName");

        this.setSchemaName("dbo");
        this.setTableName(tableName);
        this.setKeyColumnName(columnName);
    }

    /**
     * Initializes a new instance of the <see cref="ShardedTableInfo"/> class.
     *
     * @param schemaName
     *            Schema name of the sharded table.
     * @param tableName
     *            Sharded table name.
     * @param columnName
     *            Shard key column name.
     */
    public ShardedTableInfo(String schemaName,
            String tableName,
            String columnName) {
        ExceptionUtils.disallowNullOrEmptyStringArgument(schemaName, "columnName");
        ExceptionUtils.disallowNullOrEmptyStringArgument(tableName, "tableName");
        ExceptionUtils.disallowNullOrEmptyStringArgument(columnName, "columnName");

        this.setSchemaName(schemaName);
        this.setTableName(tableName);
        this.setKeyColumnName(columnName);
    }

    public ShardedTableInfo() {
    }

    public final String getKeyColumnName() {
        return keyColumnName;
    }

    private void setKeyColumnName(String value) {
        keyColumnName = value;
    }

    /**
     * Determines whether the specified ShardedTableInfo object is equal to the current object.
     *
     * @param other
     *            The ShardedTableInfo object to compare with the current object.
     * @return true if the specified ShardedTableInfo object is equal to the current object; otherwise, false.
     */
    public final boolean equals(ShardedTableInfo other) {
        return other != null && (this == other || this.getSchemaName().equals(other.getSchemaName())
                && this.getTableName().equals(other.getTableName()) && this.getKeyColumnName().equals(other.getKeyColumnName()));

    }

    /**
     * Overrides the Equals() method of Object class. Determines whether the specified object is equal to the current object.
     *
     * @param obj
     *            The object to compare with the current ShardedTableInfo object.
     * @return true if the specified object is equal to the current ShardedTableInfo object; otherwise, false.
     */
    @Override
    public boolean equals(Object obj) {
        ShardedTableInfo shardedTableInfo = (ShardedTableInfo) ((obj instanceof ShardedTableInfo) ? obj : null);
        return shardedTableInfo != null && this.equals(shardedTableInfo);
    }

    /**
     * Calculates the hash code for this instance.
     *
     * @return Hash code for the object.
     */
    @Override
    public int hashCode() {
        return this.getTableName().hashCode();
    }
}