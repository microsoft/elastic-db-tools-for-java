package com.microsoft.azure.elasticdb.shard.schema;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.google.common.collect.ComparisonChain;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;

/**
 * Represents information about a single reference table.
 */
@XmlAccessorType(XmlAccessType.NONE)
public class ReferenceTableInfo extends TableInfo implements Serializable {

    public ReferenceTableInfo() {
    }

    /**
     * Initializes a new instance of the <see cref="ReferenceTableInfo"/> class.
     *
     * @param tableName
     *            Reference table name.
     */
    public ReferenceTableInfo(String tableName) {
        ExceptionUtils.disallowNullOrEmptyStringArgument(tableName, "tableName");

        this.setSchemaName("dbo");
        this.setTableName(tableName);
    }

    /**
     * Initializes a new instance of the <see cref="ReferenceTableInfo"/> class.
     *
     * @param schemaName
     *            Schema name of the reference table.
     * @param tableName
     *            Reference table name.
     */
    public ReferenceTableInfo(String schemaName,
            String tableName) {
        ExceptionUtils.disallowNullOrEmptyStringArgument(schemaName, "schemaName");
        ExceptionUtils.disallowNullOrEmptyStringArgument(tableName, "tableName");

        this.setSchemaName(schemaName);
        this.setTableName(tableName);
    }

    /**
     * Overrides the Equals() method of Object class. Determines whether the specified object is equal to the current object.
     *
     * @param obj
     *            The object to compare with the current ReferenceTableInfo object.
     * @return true if the specified object is equal to the current ReferenceTableInfo object; otherwise, false.
     */
    @Override
    public boolean equals(Object obj) {
        ReferenceTableInfo refTableInfo = (ReferenceTableInfo) ((obj instanceof ReferenceTableInfo) ? obj : null);
        return refTableInfo != null && ComparisonChain.start().compare(this.getSchemaName(), refTableInfo.getSchemaName())
                .compare(this.getTableName(), refTableInfo.getTableName()).result() == 0;
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