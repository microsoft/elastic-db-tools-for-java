package com.microsoft.azure.elasticdb.query.multishard;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.sqlserver.jdbc.SQLServerConnection;

public class DbCommand implements Cloneable {

  //TODO: mimic .NET's System.Data.Common.DbCommand

  private SQLServerConnection connection;

  public SQLServerConnection getConnection() {
    return this.connection;
  }

  public void setConnection(SQLServerConnection connection) {
    this.connection = connection;
  }

  @Override
  public DbCommand clone() {
    return new DbCommand();
  }
}
