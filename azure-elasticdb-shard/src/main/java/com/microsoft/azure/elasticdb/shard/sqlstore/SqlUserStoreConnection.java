package com.microsoft.azure.elasticdb.shard.sqlstore;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.store.IUserStoreConnection;
import com.microsoft.azure.elasticdb.shard.store.StoreException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Instance of a User Sql Store Connection.
 */
public class SqlUserStoreConnection implements IUserStoreConnection {

  /**
   * Underlying connection.
   */
  private Connection conn;

  /**
   * Creates a new instance of user store connection.
   *
   * @param connectionString Connection string.
   */
  public SqlUserStoreConnection(String connectionString) {
    try {
      conn = DriverManager.getConnection(connectionString);
    } catch (SQLException e) {
      e.printStackTrace();
      throw new StoreException(e.getMessage(), e);
    }
  }

  /**
   * Underlying SQL server connection.
   */
  public final Connection getConnection() {
    return conn;
  }

  @Override
  public void close() {
    try {
      if (!conn.isClosed()) {
        conn.close();
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}