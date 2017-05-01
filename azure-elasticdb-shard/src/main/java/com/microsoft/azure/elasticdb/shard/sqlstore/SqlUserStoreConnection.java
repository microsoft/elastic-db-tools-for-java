package com.microsoft.azure.elasticdb.shard.sqlstore;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.store.IUserStoreConnection;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.Callable;

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
    }
  }

  /**
   * Underlying SQL server connection.
   */
  public final Connection getConnection() {
    return conn;
  }

  /**
   * Opens the connection.
   */
  public final void open() {
    //conn.Open();
  }

  /**
   * Asynchronously opens the connection.
   *
   * @return Task to await completion of the Open
   */
  public final Callable openAsync() {
    return null;
    //TODO: return conn.OpenAsync();
  }

  ///#region IDisposable

  /**
   * Disposes the object.
   */
  public final void dispose() {
    this.dispose(true);
    //TODO: GC.SuppressFinalize(this);
  }

  /**
   * Performs actual Dispose of resources.
   *
   * @param disposing Whether the invocation was from IDisposable.Dipose method.
   */
  protected void dispose(boolean disposing) {
    if (disposing) {
      //TODO: conn.Dispose();
      conn = null;
    }
  }

  @Override
  public void close() throws IOException {

  }

  ///#endregion IDisposable
}