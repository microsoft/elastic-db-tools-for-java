package com.microsoft.azure.elasticdb.query.unittests;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

/**
 * Purpose:
 * Mocks SQLServerStatement
 */
public class MockSqlStatement /*extends SQLServerStatement implements Cloneable*/ {
//
//  private Statement _cmd = new SQLServerStatement();
//
//  public MockSqlStatement() throws SQLServerException {
//    this(5);
//  }
//
//  public MockSqlStatement(int commandTimeout) throws SQLServerException {
//    super();
//    setCommandTimeout(commandTimeout);
//
//  }
//
//  /**
//   * The command text to execute against the shards
//   */
//  private String CommandText;
//
//  @Override
//  public String getCommandText() {
//    return CommandText;
//  }
//
//  @Override
//  public void setCommandText(String value) {
//    CommandText = value;
//  }
//
//  /**
//   * Command timeout
//   */
//  private int CommandTimeout;
//
//  @Override
//  public int getCommandTimeout() {
//    return CommandTimeout;
//  }
//
//  @Override
//  public void setCommandTimeout(int value) {
//    CommandTimeout = value;
//  }
//
//  /**
//   * Command type of the command to be executed
//   */
//  private CommandType CommandType;
//
//  @Override
//  public CommandType getCommandType() {
//    return CommandType;
//  }
//
//  @Override
//  public void setCommandType(CommandType value) {
//    CommandType = value;
//  }
//
//  private Action0Param ExecuteReaderAction;
//
//  public final Action0Param getExecuteReaderAction() {
//    return ExecuteReaderAction;
//  }
//
//  public final void setExecuteReaderAction(Action0Param value) {
//    ExecuteReaderAction = () -> value.invoke();
//  }
//
//  private Func2Param<CancellationToken, MockSqlStatement, DbDataReader> ExecuteReaderFunc;
//
//  public final Func2Param<CancellationToken, MockSqlStatement, DbDataReader> getExecuteReaderFunc() {
//    return ExecuteReaderFunc;
//  }
//
//  public final void setExecuteReaderFunc(
//      Func2Param<CancellationToken, MockSqlStatement, DbDataReader> value) {
//    ExecuteReaderFunc = (CancellationToken arg1, MockSqlStatement arg2) -> value.invoke(arg1, arg2);
//  }
//
//  /**
//   * Gets the SqlParameter Collection
//   */
//  @Override
//  protected DbParameterCollection getDbParameterCollection() {
//    return _cmd.Parameters;
//  }
//
//  @Override
//  protected DbParameter CreateDbParameter() {
//    return new SqlParameter();
//  }
//
//  /**
//   * The ShardedDbConnetion that holds connections to multiple shards
//   */
//  private DbConnection DbConnection;
//
//  @Override
//  protected DbConnection getDbConnection() {
//    return DbConnection;
//  }
//
//  @Override
//  protected void setDbConnection(DbConnection value) {
//    DbConnection = value;
//  }
//
//  private int RetryCount;
//
//  public final int getRetryCount() {
//    return RetryCount;
//  }
//
//  public final void setRetryCount(int value) {
//    RetryCount = value;
//  }
//
//  /**
//   * DEVNOTE (VSTS 2202707): Do we want to support command behavior?
//   */
//  @Override
//  protected DbDataReader ExecuteDbDataReader(CommandBehavior behavior) {
//    return ExecuteReaderFunc(CancellationToken.None, this);
//  }
//
//  @Override
//  protected Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior,
//      CancellationToken cancellationToken) {
//    return Task.<DbDataReader>Run(() -> {
//      cancellationToken.ThrowIfCancellationRequested();
//      var reader = ExecuteReaderFunc(cancellationToken, this);
//      cancellationToken.ThrowIfCancellationRequested();
//      return reader;
//    });
//  }
//
//  /**
//   */
//  @Override
//  public void Prepare() {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Cancel any in progress commands
//   */
//  @Override
//  public void Cancel() {
//  }
//
//  /**
//   * Dispose off any unmanaged/managed resources held
//   */
//  @Override
//  protected void Dispose(boolean disposing) {
//    super.Dispose(disposing);
//  }
//
//  public final MockSqlStatement Clone() {
//    MockSqlStatement clone = new MockSqlStatement(getCommandTimeout());
//    clone.setCommandText(this.getCommandText());
//    clone.setCommandType(this.getCommandType());
//    clone.Connection = this.Connection;
//    clone.setExecuteReaderFunc(
//        (CancellationToken arg1, MockSqlStatement arg2) -> ExecuteReaderFunc(arg1, arg2));
//    return clone;
//  }
//
//  public final Object Clone() {
//    return Clone();
//  }
//
//  @Override
//  public int ExecuteNonQuery() {
//    throw new UnsupportedOperationException();
//  }
//
//  @Override
//  public Task<Integer> ExecuteNonQueryAsync(CancellationToken cancellationToken) {
//    return super.ExecuteNonQueryAsync(cancellationToken);
//  }
//
//  @Override
//  public Object ExecuteScalar() {
//    throw new UnsupportedOperationException();
//  }
//
//  @Override
//  public Task<Object> ExecuteScalarAsync(CancellationToken cancellationToken) {
//    return super.ExecuteScalarAsync(cancellationToken);
//  }
//
//  /**
//   * Gets or sets a value indicating whether the command object
//   * should be visible in a customized interface control
//   *
//   * We do not support this
//   */
//  @Override
//  public boolean getDesignTimeVisible() {
//    throw new UnsupportedOperationException();
//  }
//
//  @Override
//  public void setDesignTimeVisible(boolean value) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Currently, Transactions aren't supported against shards
//   */
//  @Override
//  protected DbTransaction getDbTransaction() {
//    throw new UnsupportedOperationException();
//  }
//
//  @Override
//  protected void setDbTransaction(DbTransaction value) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   */
//  @Override
//  public UpdateRowSource getUpdatedRowSource() {
//    throw new UnsupportedOperationException();
//  }
//
//  @Override
//  public void setUpdatedRowSource(UpdateRowSource value) {
//    throw new UnsupportedOperationException();
//  }
}