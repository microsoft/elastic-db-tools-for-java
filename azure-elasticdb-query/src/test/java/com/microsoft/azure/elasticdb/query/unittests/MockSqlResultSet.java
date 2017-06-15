package com.microsoft.azure.elasticdb.query.unittests;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

/**
 * Purpose:
 * Mocks SqlDataReader
 * Inherits from DbDataReader
 */
public class MockSqlResultSet /*extends ResultSet*/ {
//
//  private boolean _isClosed = false;
//
//  public MockSqlResultSet() {
//    this("MockReader", new DataTable());
//  }
//
//  public MockSqlResultSet(String name) {
//    this(name, new DataTable());
//    setName(name);
//  }
//
//  public MockSqlResultSet(String name, DataTable dataTable) {
//    setName(name);
//    setDataTable(dataTable);
//  }
//
//  /**
//   * Gets a value indicating the depth of nesting for the current row.
//   */
//  @Override
//  public int getDepth() {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Gets the number of columns in the current row.
//   */
//  @Override
//  public int getFieldCount() {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Gets a value that indicates whether this DbDataReader contains one or more rows.
//   */
//  @Override
//  public boolean getHasRows() {
//    return false;
//  }
//
//  /**
//   * Gets a value indicating whether the DbDataReader is closed.
//   */
//  @Override
//  public boolean getIsClosed() {
//    return _isClosed;
//  }
//
//  /**
//   * Gets the value of the specified column as an instance of Object.
//   *
//   * @param ordinal The zero-based column ordinal.
//   * @return The value of the specified column.
//   */
//  @Override
//  public Object getItem(int ordinal) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Gets the value of the specified column as an instance of Object.
//   *
//   * @param name The name of the column.
//   * @return The value of the specified column.
//   */
//  @Override
//  public Object getItem(String name) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Gets the number of rows changed, inserted, or deleted by execution of the SQL statement.
//   *
//   *
//   * However, from the SqlDataReader source, it looks like the property is updated before the reader
//   * is closed and is initialized to -1 by default. So, we'll return -1 always since we only allow
//   * SELECT statements.
//   */
//  @Override
//  public int getRecordsAffected() {
//    return -1;
//  }
//
//  /**
//   */
//  @Override
//  public int getVisibleFieldCount() {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Callback to execute when ReadAsync is invoked
//   */
//  private tangible.Func1Param<MockSqlResultSet, Task<Boolean>> ExecuteOnReadAsync;
//
//  public final tangible.Func1Param<MockSqlResultSet, Task<Boolean>> getExecuteOnReadAsync() {
//    return ExecuteOnReadAsync;
//  }
//
//  public final void setExecuteOnReadAsync(
//      tangible.Func1Param<MockSqlResultSet, Task<Boolean>> value) {
//    ExecuteOnReadAsync = (MockSqlResultSet arg) -> value.invoke(arg);
//  }
//
//  private tangible.Action1Param<MockSqlResultSet> ExecuteOnGetColumn;
//
//  public final tangible.Action1Param<MockSqlResultSet> getExecuteOnGetColumn() {
//    return ExecuteOnGetColumn;
//  }
//
//  public final void setExecuteOnGetColumn(tangible.Action1Param<MockSqlResultSet> value) {
//    ExecuteOnGetColumn = (MockSqlResultSet obj) -> value.invoke(obj);
//  }
//
//  /**
//   * The name of this reader
//   */
//  private String Name;
//
//  public final String getName() {
//    return Name;
//  }
//
//  public final void setName(String value) {
//    Name = value;
//  }
//
//  /**
//   * The data served by this reader
//   */
//  private DataTable DataTable;
//
//  public final DataTable getDataTable() {
//    return DataTable;
//  }
//
//  public final void setDataTable(DataTable value) {
//    DataTable = value;
//  }
//
//  /**
//   * Closes the reader
//   */
//  @Override
//  public void Close() {
//    _isClosed = true;
//  }
//
//  /**
//   * Opens the reader
//   */
//  public final void Open() {
//    _isClosed = false;
//  }
//
//  /**
//   * Not implemented
//   */
//  @Override
//  public ObjRef CreateObjRef(Class requestedType) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Gets the value of the specified column as a Boolean.
//   *
//   * @param ordinal The zero-based column ordinal.
//   * @return The value of the specified column.
//   */
//  @Override
//  public boolean GetBoolean(int ordinal) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Gets the value of the specified column as a byte.
//   *
//   * @param ordinal The zero-based column ordinal.
//   * @return The value of the specified column.
//   */
////WARNING: Unsigned integer types have no direct equivalent in Java:
////ORIGINAL LINE: public override byte GetByte(int ordinal)
//  @Override
//  public byte GetByte(int ordinal) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Reads a stream of bytes from the specified column, starting at location indicated by
//   * dataOffset, into the buffer, starting at the location indicated by bufferOffset.
//   *
//   * @param ordinal The zero-based column ordinal.
//   * @param dataOffset The index within the row from which to begin the read operation.
//   * @param buffer The buffer into which to copy the data.
//   * @param bufferOffset The index with the buffer to which the data will be copied.
//   * @param length The maximum number of characters to read.
//   * @return The actual number of bytes read.
//   */
////WARNING: Unsigned integer types have no direct equivalent in Java:
////ORIGINAL LINE: public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
//  @Override
//  public long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Gets the value of the specified column as a single character.
//   *
//   * @param ordinal The zero-based column ordinal.
//   * @return The value of the specified column.
//   */
//  @Override
//  public char GetChar(int ordinal) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Reads a stream of characters from the specified column, starting at location indicated by
//   * dataOffset, into the buffer, starting at the location indicated by bufferOffset.
//   *
//   * @param ordinal The zero-based column ordinal.
//   * @param dataOffset The index within the row from which to begin the read operation.
//   * @param buffer The buffer into which to copy the data.
//   * @param bufferOffset The index with the buffer to which the data will be copied.
//   * @param length The maximum number of characters to read.
//   * @return The actual number of characters read.
//   */
//  @Override
//  public long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Gets name of the data type of the specified column.
//   *
//   * @param ordinal The zero-based column ordinal.
//   * @return A string representing the name of the data type.
//   */
//  @Override
//  public String GetDataTypeName(int ordinal) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Gets the value of the specified column as a DateTime object.
//   *
//   * @param ordinal The zero-based column ordinal.
//   * @return The value of the specified column.
//   */
//  @Override
//  public java.time.LocalDateTime GetDateTime(int ordinal) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Returns a DbDataReader object for the requested column ordinal that can be overridden with a
//   * provider-specific implementation.
//   *
//   * @param ordinal The zero-based column ordinal.
//   * @return A DbDataReader object.
//   */
//  @Override
//  protected DbDataReader GetDbDataReader(int ordinal) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Gets the value of the specified column as a Decimal object.
//   *
//   * @param ordinal The zero-based column ordinal.
//   * @return The value of the specified column.
//   */
//  @Override
//  public java.math.BigDecimal GetDecimal(int ordinal) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Gets the value of the specified column as a Double object.
//   *
//   * @param ordinal The zero-based column ordinal.
//   * @return The value of the specified column.
//   */
//  @Override
//  public double GetDouble(int ordinal) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Returns an IEnumerator that can be used to iterate through the rows in the data reader.
//   *
//   * @return An IEnumerator that can be used to iterate through the rows in the data reader.
//   */
//  @Override
//  public Iterator GetEnumerator() {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Gets the data type of the specified column.
//   *
//   * @param ordinal The zero-based column ordinal.
//   * @return The data type of the specified column.
//   */
//  @Override
//  public Class GetFieldType(int ordinal) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Synchronously gets the value of the specified column as a type.
//   *
//   * <typeparam name="T">The Type to get the value of the column as.</typeparam>
//   *
//   * @param ordinal The column to be retrieved.
//   * @return The column to be retrieved.
//   */
//  @Override
//  public <T> T GetFieldValue(int ordinal) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Not implemented
//   */
//  @Override
//  public <T> Task<T> GetFieldValueAsync(int ordinal, CancellationToken cancellationToken) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Gets the value of the specified column as a single-precision floating point number.
//   *
//   * @param ordinal The zero-based column ordinal.
//   * @return The value of the specified column.
//   */
//  @Override
//  public float GetFloat(int ordinal) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Gets the value of the specified column as a globally-unique identifier (GUID).
//   *
//   * @param ordinal The zero-based column ordinal.
//   * @return The value of the specified column.
//   */
//  @Override
//  public UUID GetGuid(int ordinal) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Gets the value of the specified column as a 16-bit signed integer.
//   *
//   * @param ordinal The zero-based column ordinal.
//   * @return The value of the specified column.
//   */
//  @Override
//  public short GetInt16(int ordinal) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Gets the value of the specified column as a 32-bit signed integer.
//   *
//   * @param ordinal The zero-based column ordinal.
//   * @return The value of the specified column.
//   */
//  @Override
//  public int GetInt32(int ordinal) {
//    ExecuteOnGetColumn(this);
//    return 0;
//  }
//
//  /**
//   * Gets the value of the specified column as a 64-bit signed integer.
//   *
//   * @param ordinal The zero-based column ordinal.
//   * @return The value of the specified column.
//   */
//  @Override
//  public long GetInt64(int ordinal) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Gets the name of the column, given the zero-based column ordinal.
//   *
//   * @param ordinal The zero-based column ordinal.
//   * @return The name of the specified column.
//   */
//  @Override
//  public String GetName(int ordinal) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Gets the column ordinal given the name of the column.
//   *
//   * @param name The name of the column.
//   * @return The zero-based column ordinal.
//   */
//  @Override
//  public int GetOrdinal(String name) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   */
//  @Override
//  public Class GetProviderSpecificFieldType(int ordinal) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * not implmeented
//   */
//  @Override
//  public Object GetProviderSpecificValue(int ordinal) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Gets all provider-specific attribute columns in the collection for the current row.
//   *
//   * @param values An array of Object into which to copy the attribute columns.
//   * @return The number of instances of Object in the array.
//   */
//  @Override
//  public int GetProviderSpecificValues(Object[] values) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Returns a DataTable that describes the column metadata of the DbDataReader.
//   *
//   * @return A DataTable that describes the column metadata.
//   */
//  @Override
//  public DataTable GetSchemaTable() {
//    return getDataTable();
//  }
//
//  /**
//   * Retrieves data as a Stream.
//   *
//   * @param ordinal The zero-based column ordinal.
//   * @return The returned object.
//   */
////TODO TASK: C# to Java Converter cannot determine whether this System.IO.Stream is input or output:
//  @Override
//  public Stream GetStream(int ordinal) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Gets the value of the specified column as an instance of String.
//   *
//   * @param ordinal The zero-based column ordinal.
//   * @return The value of the specified column.
//   */
//  @Override
//  public String GetString(int ordinal) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Retrieves data as a TextReader.
//   *
//   * @param ordinal Retrieves data as a TextReader.
//   * @return The returned object.
//   *
//   * We let SqlDataReader handle it which returns a TextReader to read the column
//   */
//  @Override
//  public TextReader GetTextReader(int ordinal) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Gets the value of the specified column as an instance of Object.
//   *
//   * @param ordinal The zero-based column ordinal.
//   * @return The value of the specified column.
//   */
//  @Override
//  public Object GetValue(int ordinal) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Populates an array of objects with the column values of the current row.
//   *
//   * @param values An array of Object into which to copy the attribute columns.
//   * @return The number of instances of Object in the array.
//   */
//  @Override
//  public int GetValues(Object[] values) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Not implemented
//   */
//  @Override
//  public Object InitializeLifetimeService() {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Gets a value that indicates whether the column contains nonexistent or missing values.
//   *
//   * @param ordinal The zero-based column ordinal.
//   * @return true if the specified column is equivalent to DBNull; otherwise false.
//   */
//  @Override
//  public boolean IsDBNull(int ordinal) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Async DBNull
//   */
//  @Override
//  public Task<Boolean> IsDBNullAsync(int ordinal, CancellationToken cancellationToken) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Advances the reader to the next result when reading the results of a batch of statements.
//   *
//   * @return true if there are more result sets; otherwise false.
//   */
//  @Override
//  public boolean NextResult() {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Next result in async
//   */
//  @Override
//  public Task<Boolean> NextResultAsync(CancellationToken cancellationToken) {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Advances the reader to the next record in a result set.
//   *
//   * @return true if there are more rows; otherwise false.
//   */
//  @Override
//  public boolean Read() {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Async read
//   */
//  @Override
//  public Task<Boolean> ReadAsync(CancellationToken cancellationToken) {
//    cancellationToken.ThrowIfCancellationRequested();
//
//    return ExecuteOnReadAsync(this);
//  }
}