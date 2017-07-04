package com.microsoft.azure.elasticdb.query.unittests;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.query.helpers.Action0Param;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Purpose: Mocks SQL Connection.
 */
public class MockSqlConnection {

  private Connection connection;
  private String connectionString;
  private String setDatabase;
  /**
   * Aciton to execute on opening the connection to be set by the test.
   */
  private Action0Param executeOnOpen;

  public MockSqlConnection(String connectionString, Action0Param executeOnOpen) {
    this.connectionString = connectionString;
    this.executeOnOpen = executeOnOpen;
  }

  public final Connection getConnection() {
    return connection;
  }

  public final Action0Param getExecuteOnOpen() {
    return executeOnOpen;
  }

  public final void setExecuteOnOpen(Action0Param value) {
    executeOnOpen = value;
  }

  public final String getSetDatabase() {
    return setDatabase;
  }

  public final void setSetDatabase(String value) {
    setDatabase = value;
  }

  public final void open() throws SQLException {
    connection = DriverManager.getConnection(this.connectionString);
    executeOnOpen.invoke();
  }

  /**
   * @return mock SQL Statement instance.
   */
  public final MockSqlStatement createCommand() {
    return new MockSqlStatement();
  }
}