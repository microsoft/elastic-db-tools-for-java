package com.microsoft.azure.elasticdb.query.unittests;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.query.helpers.Action1Param;
import com.microsoft.azure.elasticdb.query.helpers.Func1Param;
import java.sql.ResultSet;
import java.util.concurrent.Callable;

/**
 * Purpose: Mocks SQL ResultSet. Inherits from ResultSet.
 */
public class MockSqlResultSet {

  private boolean isClosed = false;
  /**
   * Callback to execute when ReadAsync is invoked
   */
  private Func1Param<MockSqlResultSet, Callable<Boolean>> executeOnReadAsync;
  private Action1Param<MockSqlResultSet> executeOnGetColumn;
  /**
   * The name of this reader
   */
  private String name;
  /**
   * The data served by this reader
   */
  private ResultSet resultSet;

  public MockSqlResultSet() {
    this("MockReader", null);
  }

  public MockSqlResultSet(String name) {
    this(name, null);
    setName(name);
  }

  public MockSqlResultSet(String name, ResultSet dataTable) {
    setName(name);
    setDataTable(dataTable);
  }

  public final Func1Param<MockSqlResultSet, Callable<Boolean>> getExecuteOnReadAsync() {
    return executeOnReadAsync;
  }

  public final void setExecuteOnReadAsync(
      Func1Param<MockSqlResultSet, Callable<Boolean>> value) {
    executeOnReadAsync = value;
  }

  public final Action1Param<MockSqlResultSet> getExecuteOnGetColumn() {
    return executeOnGetColumn;
  }

  public final void setExecuteOnGetColumn(Action1Param<MockSqlResultSet> value) {
    executeOnGetColumn = value;
  }

  public final String getName() {
    return name;
  }

  public final void setName(String value) {
    name = value;
  }

  public final ResultSet getDataTable() {
    return resultSet;
  }

  public final void setDataTable(ResultSet value) {
    resultSet = value;
  }
}