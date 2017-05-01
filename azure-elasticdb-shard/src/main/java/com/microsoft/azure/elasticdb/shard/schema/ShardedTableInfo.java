package com.microsoft.azure.elasticdb.shard.schema;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import java.io.Serializable;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

/**
 * Represents information about a single sharded table.
 */
//TODO: IEquatable<ShardedTableInfo>,
@XmlAccessorType(XmlAccessType.NONE)
public class ShardedTableInfo extends TableInfo implements Serializable {

  /**
   * Name of the shard key column.
   */
  @XmlElement(name = "KeyColumnName")
  private String KeyColumnName;

  /**
   * Initializes a new instance of the <see cref="ShardedTableInfo"/> class.
   *
   * @param tableName Sharded table name.
   * @param columnName Shard key column name.
   */
  public ShardedTableInfo(String tableName, String columnName) {
    ExceptionUtils.DisallowNullOrEmptyStringArgument(tableName, "tableName");
    ExceptionUtils.DisallowNullOrEmptyStringArgument(columnName, "columnName");

    this.setSchemaName("dbo");
    this.setTableName(tableName);
    this.setKeyColumnName(columnName);
  }

  /**
   * Initializes a new instance of the <see cref="ShardedTableInfo"/> class.
   *
   * @param schemaName Schema name of the sharded table.
   * @param tableName Sharded table name.
   * @param columnName Shard key column name.
   */
  public ShardedTableInfo(String schemaName, String tableName, String columnName) {
    ExceptionUtils.DisallowNullOrEmptyStringArgument(schemaName, "columnName");
    ExceptionUtils.DisallowNullOrEmptyStringArgument(tableName, "tableName");
    ExceptionUtils.DisallowNullOrEmptyStringArgument(columnName, "columnName");

    this.setSchemaName(schemaName);
    this.setTableName(tableName);
    this.setKeyColumnName(columnName);
  }

  public ShardedTableInfo() {
  }

  public final String getKeyColumnName() {
    return KeyColumnName;
  }

  private void setKeyColumnName(String value) {
    KeyColumnName = value;
  }

  /**
   * Determines whether the specified ShardedTableInfo object is equal to the current object.
   *
   * @param other The ShardedTableInfo object to compare with the current object.
   * @return true if the specified ShardedTableInfo object is equal to the current object;
   * otherwise, false.
   */
  public final boolean equals(ShardedTableInfo other) {
    if (other == null) {
      return false;
    }

    if (this == other) {
      return true;
    }

    return this.getSchemaName().equals(other.getSchemaName()) && this.getTableName()
        .equals(other.getTableName()) && this.getKeyColumnName().equals(other.getKeyColumnName());
  }

  /**
   * Overrides the Equals() method of Object class. Determines whether the specified object
   * is equal to the current object.
   *
   * @param obj The object to compare with the current ShardedTableInfo object.
   * @return true if the specified object is equal to the current ShardedTableInfo object;
   * otherwise, false.
   */
  @Override
  public boolean equals(Object obj) {
    ShardedTableInfo shardedTableInfo = (ShardedTableInfo) ((obj instanceof ShardedTableInfo) ? obj
        : null);
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