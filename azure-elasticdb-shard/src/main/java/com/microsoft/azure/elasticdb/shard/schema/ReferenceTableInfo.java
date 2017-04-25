package com.microsoft.azure.elasticdb.shard.schema;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import java.io.Serializable;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

/**
 * Represents information about a single reference table.
 */
//TODO: IEquatable<ReferenceTableInfo>,
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
    ExceptionUtils.DisallowNullOrEmptyStringArgument(tableName, "tableName");

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
    ExceptionUtils.DisallowNullOrEmptyStringArgument(schemaName, "schemaName");
    ExceptionUtils.DisallowNullOrEmptyStringArgument(tableName, "tableName");

    this.setSchemaName(schemaName);
    this.setTableName(tableName);
  }

  /**
   * Determines whether the specified ReferenceTableInfo object is equal to the current object.
   *
   * @param other The ReferenceTableInfo object to compare with the current object.
   * @return true if the specified ReferenceTableInfo object is equal to the current object;
   * otherwise, false.
   */
  public final boolean equals(ReferenceTableInfo other) {
    if (other == null) {
      return false;
    }

    if (this == other) {
      return true;
    }

    return this.getSchemaName().equals(other.getSchemaName()) && this.getTableName()
        .equals(other.getTableName());
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
    return refTableInfo != null && this.equals(refTableInfo);
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