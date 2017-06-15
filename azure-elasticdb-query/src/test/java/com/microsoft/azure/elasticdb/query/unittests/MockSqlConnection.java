package com.microsoft.azure.elasticdb.query.unittests;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

/**
 * Purpose:
 * Mocks SqlConnection
 */
public class MockSqlConnection /*extends Connection*/ {
//
//  private ConnectionState _state = ConnectionState.Closed;
//
//  public MockSqlConnection(String connectionString, Action0Param executeOnOpen) {
//    setConnectionString(connectionString);
//    setExecuteOnOpen(() -> executeOnOpen.invoke());
//  }
//
//  /**
//   * The current state of the connection
//   * Closed by default.
//   */
//  @Override
//  public ConnectionState getState() {
//    return _state;
//  }
//
//  public final ConnectionState getMockConnectionState() {
//    return _state;
//  }
//
//  public final void setMockConnectionState(ConnectionState value) {
//    _state = value;
//  }
//
//  /**
//   * The time in seconds to wait for connections
//   * to ALL shards to be opened.
//   * Default timeout is 300 seconds.
//   *
//   * Value of 0 indicates that we wait forever
//   */
//  private int ConnectionTimeout;
//
//  public final int getConnectionTimeout() {
//    return ConnectionTimeout;
//  }
//
//  public final void setConnectionTimeout(int value) {
//    ConnectionTimeout = value;
//  }
//
//  /**
//   */
//  private String ConnectionString;
//
//  @Override
//  public String getConnectionString() {
//    return ConnectionString;
//  }
//
//  @Override
//  public void setConnectionString(String value) {
//    ConnectionString = value;
//  }
//
//  /**
//   * The server version
//   */
//  @Override
//  public String getServerVersion() {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * the data source
//   */
//  @Override
//  public String getDataSource() {
//    return "SomeSource";
//  }
//
//  private String SetDatabase;
//
//  public final String getSetDatabase() {
//    return SetDatabase;
//  }
//
//  public final void setSetDatabase(String value) {
//    SetDatabase = value;
//  }
//
//  /**
//   * Name of this database
//   */
//  @Override
//  public String getDatabase() {
//    return getSetDatabase();
//  }
//
//  /**
//   * Aciton to execute on opening the connection to be set by the test.
//   */
//  private Action0Param ExecuteOnOpen;
//
//  public final Action0Param getExecuteOnOpen() {
//    return ExecuteOnOpen;
//  }
//
//  public final void setExecuteOnOpen(Action0Param value) {
//    ExecuteOnOpen = () -> value.invoke();
//  }
//
//  /**
//   * @return mock sqlcommand instance
//   */
//  public final MockSqlStatement CreateCommand() {
//    return new MockSqlStatement();
//  }
//
//  /**
//   * Creates and returns a DbCommand associated with this connection
//   */
//  @Override
//  protected DbCommand CreateDbCommand() {
//    return CreateCommand();
//  }
//
//  /**
//   * Mocks opening a SqlConnection
//   */
//  @Override
//  public void Open() {
//    _state = ConnectionState.Open;
//
//    ExecuteOnOpen();
//  }
//
//  /**
//   * Mocks opening a SqlConnection asynchronously
//   *
//   * @param cancellationToken A cancellation token for the user to cancel this async call if
//   * necessary
//   * @return A task that conveys failure/success in openining connections to shards
//   */
//  @Override
//  public Task OpenAsync(CancellationToken outerCancellationToken) {
//    return Task.Run(() -> {
//      outerCancellationToken.ThrowIfCancellationRequested();
//      ExecuteOnOpen();
//      _state = ConnectionState.Open;
//      outerCancellationToken.ThrowIfCancellationRequested();
//    });
//  }
//
//  /**
//   * Closes this connection
//   */
//  @Override
//  public void Close() {
//    _state = ConnectionState.Closed;
//  }
//
//  /**
//   * Disposes off any managed and unmanaged resources
//   */
//  @Override
//  protected void Dispose(boolean disposing) {
//    super.Dispose(disposing);
//  }
//
//  /**
//
//   @param isolationLevel
//   @return
//   */
//  @Override
//  protected DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//
//   @param isolationLevel
//   @return
//   */
//  @Override
//  public void EnlistTransaction(System.Transactions.Transaction transaction) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//
//   @param databaseName
//   */
//  @Override
//  public void ChangeDatabase(String databaseName) {
//    throw new UnsupportedOperationException();
//  }
}