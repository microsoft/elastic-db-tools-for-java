package com.microsoft.azure.elasticdb.shard.schema;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

/**
 * Repesents a table in a database.
 */
@XmlAccessorType(XmlAccessType.NONE)
public abstract class TableInfo {

  /**
   * Table's schema name.
   */
  @XmlElement(name = "SchemaName")
  private String schemaName;
  /**
   * Table name.
   */
  @XmlElement(name = "TableName")
  private String tableName;

  public final String getSchemaName() {
    return schemaName;
  }

  protected final void setSchemaName(String value) {
    schemaName = value;
  }

  public final String getTableName() {
    return tableName;
  }

  protected final void setTableName(String value) {
    tableName = value;
  }
}