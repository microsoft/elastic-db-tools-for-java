package com.microsoft.azure.elasticdb.shard.schema;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.google.common.collect.ComparisonChain;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import java.io.Serializable;

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
   * @param tableName Reference table name.
   */
  public ReferenceTableInfo(String tableName) {
    ExceptionUtils.disallowNullOrEmptyStringArgument(tableName, "tableName");

    this.setSchemaName("dbo");
    this.setTableName(tableName);
  }

  /**
   * Initializes a new instance of the <see cref="ReferenceTableInfo"/> class.
   *
   * @param schemaName Schema name of the reference table.
   * @param tableName Reference table name.
   */
  public ReferenceTableInfo(String schemaName, String tableName) {
    ExceptionUtils.disallowNullOrEmptyStringArgument(schemaName, "schemaName");
    ExceptionUtils.disallowNullOrEmptyStringArgument(tableName, "tableName");

    this.setSchemaName(schemaName);
    this.setTableName(tableName);
  }

  /**
   * Overrides the Equals() method of Object class. Determines whether the specified object
   * is equal to the current object.
   *
   * @param obj The object to compare with the current ReferenceTableInfo object.
   * @return true if the specified object is equal to the current ReferenceTableInfo object;
   * otherwise, false.
   */
  @Override
  public boolean equals(Object obj) {
    ReferenceTableInfo refTableInfo = (ReferenceTableInfo) ((obj instanceof ReferenceTableInfo)
        ? obj : null);
    return refTableInfo != null
        && ComparisonChain.start()
            .compare(this.getSchemaName(), refTableInfo.getSchemaName())
            .compare(this.getTableName(), refTableInfo.getTableName())
            .result() == 0;
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