package com.microsoft.azure.elasticdb.query.multishard;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import java.sql.Connection;

public class DbCommand implements Cloneable {

  private Connection connection;

  public Connection getConnection() {
    return this.connection;
  }

  public void setConnection(Connection connection) {
    this.connection = connection;
  }

  @Override
  public DbCommand clone() {
    return new DbCommand();
  }
}
