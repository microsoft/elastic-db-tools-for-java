package com.microsoft.azure.elasticdb.query.unittests;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

/**
 * Very basic unit tests for the MultiShardDataReader class. Just enough to ensure that simple
 * scenarios working as expected. Purpose: Basic unit testing for the MultiShardDataReader class.
 * Will integrate with build at a later date. Notes: Aim is to integrate this within a broader
 * cleint-side wrapper framework. As a result, unit testing will likely be relatively significantly
 * restructured once we have the rest of the wrapper classes in place. NOTE: Unit tests currently
 * assume that a sql server instance is accessible on localhost. NOTE: Unit tests will blow away and
 * recreate databases called Test1, Test2, and Test3.  Should change these database names to guids
 * at some point, but deferring that until our unit testing (and functional testing) framework is
 * more settled. NOTE: Unit tests will blow away and recreate a login called TestUser and grant it
 * "control server" permissions.  Will likely need to revisit this at some point in the future.
 */
public class MultiShardResultSetTests {
//
//  /**
//   * Currently doesn't do anything special.
//   */
//  public MultiShardResultSetTests() {
//  }
//
//  private SqlConnection _conn1;
//  private SqlConnection _conn2;
//  private SqlConnection _conn3;
//  private List<SqlConnection> _conns;
//
//  /**
//   * Handle on conn1, conn2 and conn3
//   */
//  private MultiShardConnection _shardConnection;
//
//  /**
//   * Placeholder object for us to pass into MSDRs that we create without going through a command.
//   */
//  private MultiShardCommand _dummyCommand;
//
//  /**
//   * Gets or sets the test context which provides
//   * information about and functionality for the current test run.
//   */
//  private TestContext TestContext;
//
//  public final TestContext getTestContext() {
//    return TestContext;
//  }
//
//  public final void setTestContext(TestContext value) {
//    TestContext = value;
//  }
//
//  /**
//   * Sets up our three test databases that we drive the unit testing off of.
//   *
//   * @param testContext The TestContext we are running in.
//   */
//  public static void MyClassInitialize(TestContext testContext) {
//    // Drop and recreate the test databases, tables, and data that we will use to verify
//    // the functionality.
//    // For now I have hardcoded the server location and database names.  A better approach would be
//    // to make the server location configurable and the database names be guids.
//    // Not the top priority right now, though.
//    //
//    SqlConnection.ClearAllPools();
//    MultiShardTestUtils.DropAndCreateDatabases();
//    MultiShardTestUtils.CreateAndPopulateTables();
//  }
//
//  /**
//   * Blow away our three test databases that we drove the tests off of.
//   * Doing this so that we don't leave objects littered around.
//   */
//  public static void MyClassCleanup() {
//    // We need to clear the connection pools so that we don't get a database still in use error
//    // resulting from our attenpt to drop the databases below.
//    //
//    SqlConnection.ClearAllPools();
//    MultiShardTestUtils.DropDatabases();
//  }
//
//  /**
//   * Open up a clean connection to each test database prior to each test.
//   */
//  public final void MyTestInitialize() {
//    ShardMap sm = MultiShardTestUtils.CreateAndGetTestShardMap();
//
//    // Use the MultiShardConnection to open up connections
//
//    // Validate the connections to shards
//    _shardConnection = new MultiShardConnection(sm.GetShards(),
//        MultiShardTestUtils.ShardConnectionString);
//    _dummyCommand = MultiShardCommand.Create(_shardConnection, "SELECT 1");
//
//    // DEVNOTE: The MultiShardCommand object handles the connection opening logic.
//    //          BUT, since we are writing tests at a lower level than that, we need to open
//    //          the connections manually here.  Hence the loop below.
//    //
////TODO TASK: There is no equivalent to implicit typing in Java:
//    for (var conn : _shardConnection.ShardConnections) {
//      conn.Item2.Open();
//    }
//
//    _conn1 = (SqlConnection) _shardConnection.ShardConnections[0].Item2;
//    _conn2 = (SqlConnection) _shardConnection.ShardConnections[1].Item2;
//    _conn3 = (SqlConnection) _shardConnection.ShardConnections[2].Item2;
//    _conns = _shardConnection.ShardConnections.Select(x -> (SqlConnection) x.Item2);
//  }
//
//  /**
//   * Close our connections to each test database after each test.
//   */
//  public final void MyTestCleanup() {
////TODO TASK: There is no equivalent to implicit typing in Java:
//    for (var conn : _shardConnection.ShardConnections) {
//      conn.Item2.Close();
//    }
//  }
//
//  /**
//   * Validate MultiShardDataReader can be supplied as argument to DataTable.Load
//   */
//  public final void TestDataTableLoad() {
//    // What we're doing:
//    // Obtain MultiShardDataReader,
//    // Pass it to DataTable.Load and ensure correct number of rows is loaded.
//    //
//    String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field FROM ConsistentShardedTable";
//
//    try (MultiShardDataReader sdr = GetShardedDbReader(_shardConnection, selectSql)) {
//      DataTable dataTable = new DataTable();
//      dataTable.Load(sdr);
//
//      Assert.AreEqual(9, dataTable.Rows.size(), "Expected 9 rows loaded to DataTable");
//
//      int recordsRetrieved = 0;
//      for (DataRow row : dataTable.Rows) {
//        recordsRetrieved++;
//        String dbNameField = row.<String>Field(0);
//        int testIntField = row.<Integer>Field(1);
//        long testBigIntField = row.<Long>Field(2);
//        String shardIdPseudoColumn = row.<String>Field(3);
//        String logRecord = String.format(
//            "RecordRetrieved: dbNameField: %1$s, TestIntField: %2$s, TestBigIntField: %3$s, shardIdPseudoColumnField: %4$s, RecordCount: %5$s",
//            dbNameField, testIntField, testBigIntField, shardIdPseudoColumn, recordsRetrieved);
//        Logger.Log(logRecord);
//        Debug.WriteLine(logRecord);
//      }
//      assert recordsRetrieved == 9;
//    }
//  }
//
//  /**
//   * Check that we can turn the $ShardName pseudo column on and off as expected.
//   */
//  public final void TestShardNamePseudoColumnOption() {
//    // What we're doing:
//    // Grab all rows from each test database.
//    // Load them into a MultiShardDataReader.
//    // Iterate through the rows and make sure that we have 9 rows total with
//    // the Pseudo column present (or not) as per the setting we used.
//    //
//    String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable";
//    boolean[] pseudoColumnPresentOptions = new boolean[2];
//    pseudoColumnPresentOptions[0] = true;
//    pseudoColumnPresentOptions[1] = false;
//
//    for (boolean pseudoColumnPresent : pseudoColumnPresentOptions) {
//      LabeledDbDataReader[] readers = new LabeledDbDataReader[3];
//      readers[0] = GetReader(_conn1, selectSql);
//      readers[1] = GetReader(_conn2, selectSql);
//      readers[2] = GetReader(_conn3, selectSql);
//
//      List<MultiShardSchemaMismatchException> exceptions = null;
//
//      try (ReferenceObjectHelper<List<MultiShardSchemaMismatchException>> tempRef_exceptions = new ReferenceObjectHelper<List<MultiShardSchemaMismatchException>>(
//          exceptions);
//          MultiShardDataReader sdr = GetMultiShardDataReaderFromDbDataReaders(readers,
//              tempRef_exceptions, pseudoColumnPresent);
//          exceptions =tempRef_exceptions.argValue) {
//        assert 0 == exceptions.size();
//
//        int recordsRetrieved = 0;
//
//        int expectedFieldCount = pseudoColumnPresent ? 4 : 3;
//        int expectedVisibleFieldCount = pseudoColumnPresent ? 4 : 3;
//        assert expectedFieldCount == sdr.FieldCount;
//        assert expectedVisibleFieldCount == sdr.VisibleFieldCount;
//
//        while (sdr.Read()) {
//          recordsRetrieved++;
//
//          String dbNameField = sdr.GetString(0);
//          int testIntField = sdr.<Integer>GetFieldValue(1);
//          long testBigIntField = sdr.<Long>GetFieldValue(2);
//          try {
//            String shardIdPseudoColumn = sdr.<String>GetFieldValue(3);
//            if (!pseudoColumnPresent) {
//              Assert.Fail("Should not have been able to pull the pseudo column.");
//            }
//          } catch (IndexOutOfBoundsException e) {
//            if (pseudoColumnPresent) {
//              Assert.Fail("Should not have encountered an exception.");
//            }
//          }
//        }
//
//        sdr.Close();
//        assert recordsRetrieved == 9;
//      }
//    }
//  }
//
//  /**
//   * Check that we can handle empty result sets interspersed with non-empty result sets as expected.
//   */
//  public final void TestMiddleResultEmptyOnSelect() {
//    // What we're doing:
//    // Grab all rows from each test database that satisfy a particular predicate (there should be 3 from db1 and
//    // db3 and 0 from db2).
//    // Load them into a MultiShardDataReader.
//    // Iterate through the rows and make sure that we have 6 rows.
//    //
//    String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable WHERE dbNameField='Test0' OR dbNameField='Test2'";
//    LabeledDbDataReader[] readers = new LabeledDbDataReader[3];
//    readers[0] = GetReader(_conn1, selectSql);
//    readers[1] = GetReader(_conn2, selectSql);
//    readers[2] = GetReader(_conn3, selectSql);
//
//    List<MultiShardSchemaMismatchException> exceptions = null;
//
//    try (ReferenceObjectHelper<List<MultiShardSchemaMismatchException>> tempRef_exceptions = new ReferenceObjectHelper<List<MultiShardSchemaMismatchException>>(
//        exceptions);
//        MultiShardDataReader sdr = GetMultiShardDataReaderFromDbDataReaders(readers,
//            tempRef_exceptions, true);
//        exceptions =tempRef_exceptions.argValue) {
//      assert 0 == exceptions.size();
//
//      int recordsRetrieved = 0;
//      while (sdr.Read()) {
//        recordsRetrieved++;
//      }
//
//      sdr.Close();
//
//      assert recordsRetrieved == 6;
//    }
//  }
//
//  /**
//   * Check that we can handle non-empty result sets interspersed with empty result sets as expected.
//   */
//  public final void TestOuterResultsEmptyOnSelect() {
//    // What we're doing:
//    // Grab all rows from each test database that satisfy a particular predicate (there should be 0 from db1 and
//    // db3 and 3 from db2).
//    // Load them into a MultiShardDataReader.
//    // Iterate through the rows and make sure that we have 3 rows.
//    //
//    String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable WHERE dbNameField='Test1'";
//    LabeledDbDataReader[] readers = new LabeledDbDataReader[3];
//    readers[0] = GetReader(_conn1, selectSql);
//    readers[1] = GetReader(_conn2, selectSql);
//    readers[2] = GetReader(_conn3, selectSql);
//
//    List<MultiShardSchemaMismatchException> exceptions = null;
//
//    try (ReferenceObjectHelper<List<MultiShardSchemaMismatchException>> tempRef_exceptions = new ReferenceObjectHelper<List<MultiShardSchemaMismatchException>>(
//        exceptions);
//        MultiShardDataReader sdr = GetMultiShardDataReaderFromDbDataReaders(readers,
//            tempRef_exceptions, true);
//        exceptions =tempRef_exceptions.argValue) {
//      assert 0 == exceptions.size();
//
//      int recordsRetrieved = 0;
//      while (sdr.Read()) {
//        recordsRetrieved++;
//      }
//
//      sdr.Close();
//
//      assert recordsRetrieved == 3;
//    }
//  }
//
//  /**
//   * Check that we collect an exception and expose it on the ShardedReader
//   * when encountering schema mismatches across result sets due to different
//   * column names.
//   */
//  public final void TestMismatchedSchemasWrongColumnName() {
//    // What we're doing:
//    // Issue different queries to readers 1 & 2 so that we have the same column count and types but we have a
//    // column name mismatch.
//    // Try to load them into a MultiShardDataReader.
//    // Should see an exception on the MultiShardDataReader.
//    // Should also be able to successfully iterate through some records.
//    //
//    String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable;";
//    String alternateSelectSql = "SELECT dbNameField as DifferentName, Test_int_Field, Test_bigint_Field FROM ConsistentShardedTable;";
//    LabeledDbDataReader[] readers = new LabeledDbDataReader[2];
//    readers[0] = GetReader(_conn1, selectSql);
//    readers[1] = GetReader(_conn2, alternateSelectSql);
//
//    List<MultiShardSchemaMismatchException> exceptions = null;
//
//    try (ReferenceObjectHelper<List<MultiShardSchemaMismatchException>> tempRef_exceptions = new ReferenceObjectHelper<List<MultiShardSchemaMismatchException>>(
//        exceptions);
//        MultiShardDataReader sdr = GetMultiShardDataReaderFromDbDataReaders(readers,
//            tempRef_exceptions, true);
//        exceptions =tempRef_exceptions.argValue) {
//      if ((null == exceptions) || (exceptions.size() != 1)) {
//        Assert.Fail("Expected an element in the InvalidReaders collection.");
//      } else {
//        int recordsRetrieved = 0;
//
//        while (sdr.Read()) {
//          recordsRetrieved++;
//        }
//
//        assert recordsRetrieved == 3;
//      }
//      sdr.Close();
//    }
//  }
//
//  /**
//   * Check that we throw as expected when encountering schema mismatches across result sets due to
//   * different column types.
//   */
//  public final void TestMismatchedSchemasWrongType() {
//    // What we're doing:
//    // Issue different queries to readers 1 & 2 so that we have the same column count and names but we have a
//    // column type mismatch.
//    // Try to load them into a MultiShardDataReader.
//    // Should see an exception on the MultiShardDataReader.
//    // Should also be able to successfully iterate through some records.
//    //
//    String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable;";
//    String alternateSelectSql = "SELECT dbNameField, Test_int_Field, Test_int_Field as Test_bigint_Field FROM ConsistentShardedTable;";
//    LabeledDbDataReader[] readers = new LabeledDbDataReader[2];
//    readers[0] = GetReader(_conn1, selectSql);
//    readers[1] = GetReader(_conn2, alternateSelectSql);
//
//    List<MultiShardSchemaMismatchException> exceptions = null;
//
//    try (ReferenceObjectHelper<List<MultiShardSchemaMismatchException>> tempRef_exceptions = new ReferenceObjectHelper<List<MultiShardSchemaMismatchException>>(
//        exceptions);
//        MultiShardDataReader sdr = GetMultiShardDataReaderFromDbDataReaders(readers,
//            tempRef_exceptions, true);
//        exceptions =tempRef_exceptions.argValue) {
//      if ((null == exceptions) || (exceptions.size() != 1)) {
//        Assert.Fail("Expected an element in the InvalidReaders collection.");
//      } else {
//        int recordsRetrieved = 0;
//
//        while (sdr.Read()) {
//          recordsRetrieved++;
//        }
//
//        assert recordsRetrieved == 3;
//      }
//      sdr.Close();
//    }
//  }
//
//  /**
//   * Check that we throw as expected when trying to add a reader after the sharded reader has been
//   * marked complete.
//   */
//  public final void TestAddReaderAfterReaderMarkedComplete() {
//    // What we're doing:
//    // Set up a new sharded reader
//    // Add two readers to it.
//    // Mark it as closed.
//    // Try to add a third reader to it.
//    // Verify that we threw as expected.
//    //
//    String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable WHERE Test_int_Field = 876";
//    LabeledDbDataReader[] readers = new LabeledDbDataReader[3];
//    readers[0] = GetReader(_conn1, selectSql);
//    readers[1] = GetReader(_conn2, selectSql);
//    readers[2] = GetReader(_conn3, selectSql);
//
//    try (MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, readers,
//        MultiShardExecutionPolicy.CompleteResults, true)) {
//      // Just a single call to AddReader should be sufficient to check this logic.
//      //
//      sdr.AddReader(readers[0]);
//    }
//  }
//
//  /**
//   * Check that we throw as expected when trying to add a null reader.
//   */
//  public final void TestAddNullReader() {
//    // What we're doing:
//    // Set up a new sharded reader
//    // Add two readers to it.
//    // Try to add a third reader to it that is null.
//    // We should not throw here since initial constructor AddReader call and subsequent AddReader call are now on par.
//    //
//    String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable WHERE Test_int_Field = 876";
//    LabeledDbDataReader[] readers = new LabeledDbDataReader[3];
//    readers[0] = GetReader(_conn1, selectSql);
//    readers[1] = GetReader(_conn2, selectSql);
//    readers[2] = null;
//
//    try (MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, readers,
//        MultiShardExecutionPolicy.CompleteResults, true, 6)) {
//      sdr.AddReader(readers[0]);
//      sdr.AddReader(readers[1]);
//      sdr.AddReader(readers[2]);
//    }
//  }
//
//  /**
//   * Validate basic ReadAsync behavior.
//   */
//  public final void TestReadAsync() {
//    LabeledDbDataReader[] readers = new LabeledDbDataReader[1];
//    readers[0] = GetReader(_conn1, "select 1");
//    int numRowsRead = 0;
//
//    try (MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, readers,
//        MultiShardExecutionPolicy.CompleteResults, true)) {
//      while (sdr.ReadAsync().Result) {
//        numRowsRead++;
//      }
//
//      Assert.AreEqual(1, numRowsRead, "ReadAsync didn't return the expeceted number of rows.");
//    }
//  }
//
//  /**
//   * Validate ReadAsync() behavior when multiple data readers are involved. This test is same as
//   * existing test TestMiddleResultEmptyOnSelect except that we are using ReadAsync() in this case
//   * instead of Read() to read individual rows.
//   *
//   * NOTE: We needn't replicate every single Read() test for ReadAsync() since Read() ends up
//   * calling ReadAsync().Result under the hood. So, by validating Read(), we are also validating
//   * ReadAsync() indirectly.
//   */
//  public final void TestReadSyncWithMultipleDataReaders() {
//    // What we're doing:
//    // Grab all rows from each test database that satisfy a particular predicate (there should be 3 from db1 and
//    // db3 and 0 from db2).
//    // Load them into a MultiShardDataReader.
//    // Iterate through the rows using ReadAsync() and make sure that we have 6 rows.
//    //
//    String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable WHERE dbNameField='Test0' OR dbNameField='Test2'";
//    LabeledDbDataReader[] readers = new LabeledDbDataReader[3];
//    readers[0] = GetReader(_conn1, selectSql);
//    readers[1] = GetReader(_conn2, selectSql);
//    readers[2] = GetReader(_conn3, selectSql);
//
//    List<MultiShardSchemaMismatchException> exceptions = null;
//
//    try (ReferenceObjectHelper<List<MultiShardSchemaMismatchException>> tempRef_exceptions = new ReferenceObjectHelper<List<MultiShardSchemaMismatchException>>(
//        exceptions);
//        MultiShardDataReader sdr = GetMultiShardDataReaderFromDbDataReaders(readers,
//            tempRef_exceptions, true);
//        exceptions =tempRef_exceptions.argValue) {
//      assert 0 == exceptions.size();
//
//      int recordsRetrieved = 0;
//      while (sdr.ReadAsync().Result) {
//        recordsRetrieved++;
//      }
//
//      sdr.Close();
//
//      assert recordsRetrieved == 6;
//    }
//  }
//
//  public final void TestMultiShardQueryCancellation() {
//    ManualResetEvent rollback = new ManualResetEvent(false);
//    ManualResetEvent readerInitialized = new ManualResetEvent(false);
//    String dbToUpdate = _conn2.Database;
//
//    // Start a task that would begin a transaction, update the rows on the second shard and then
//    // block on an event. While the transaction is still open and the task is blocked, another
//    // task will try to read rows off the shard.
//    Task lockRowTask = Task.Factory.StartNew(() -> {
//      try (SqlConnection conn = new SqlConnection(_conn2.ConnectionString)) {
//        conn.Open();
//        SqlTransaction tran = conn.BeginTransaction();
//
//        try (SqlCommand cmd = new SqlCommand(String.format(
//            "UPDATE ConsistentShardedTable SET dbNameField='TestN' WHERE dbNameField='%1$s'",
//            dbToUpdate), conn, tran)) {
//          // This will X-lock all rows in the second shard.
//          cmd.ExecuteNonQuery();
//
//          if (rollback.WaitOne()) {
//            tran.Rollback();
//          }
//        }
//      }
//    });
//
//    CancellationTokenSource tokenSource = new CancellationTokenSource();
//
//    // Create a new task that would try to read rows off the second shard while they are locked by the previous task
//    // and block therefore.
//    Task readToBlockTask = Task.Factory.StartNew(() -> {
//      String selectSql = String.format(
//          "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable WHERE dbNameField='%1$s'",
//          dbToUpdate);
//
//      try (MultiShardDataReader sdr = GetShardedDbReaderAsync(_shardConnection, selectSql,
//          tokenSource.Token)) {
//        readerInitialized.Set();
//
//        // This call should block.
//        while (sdr.ReadAsync(tokenSource.Token).Result) {
//          ;
//        }
//      }
//    });
//
//    // Cancel the second task.This should trigger the cancellation of the multi-shard query.
//    tokenSource.Cancel();
//
//    try {
//      readToBlockTask.Wait();
//      Assert.IsTrue(false, "The task expected to block ran to completion.");
//    } catch (AggregateException aggex) {
//      RuntimeException tempVar = aggex.Flatten().getCause();
//      TaskCanceledException ex = (TaskCanceledException) ((tempVar instanceof TaskCanceledException)
//          ? tempVar : null);
//
//      Assert.IsTrue(ex != null, "A task canceled exception was not received upon cancellation.");
//    }
//
//    // Set the event signaling the first task to rollback its update transaction.
//    rollback.Set();
//
//    lockRowTask.Wait();
//  }
//
//  /**
//   * Check that we do not hang when trying to read after adding null readers.
//   */
//  public final void TestReadFromNullReader() {
//    // The code below exposes a flaw in our current implementation related to
//    // CompleteResults semantics and the internal c-tor.  The flaw does not
//    // leak out to customers because the MultiShardCommand object manages the
//    // necessary logic, but we need to patch the flaw so it doesn't end up
//    // inadvertently leaking out to customers.
//    // See VSTS 2616238 (i believe).  Philip will be modofying logic and
//    // augmenting tests to deal with this issue.
//    //
//
//    // Pass a null reader and verify that read does not hang.
//    LabeledDbDataReader[] readers = new LabeledDbDataReader[2];
//    readers[0] = GetReader(_conn1, "select 1");
//    readers[1] = null;
//
//    try (MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, readers,
//        MultiShardExecutionPolicy.CompleteResults, true)) {
//      Task t = Task.Factory.StartNew(() -> {
//        while (sdr.Read()) {
//        }
//      });
//
//      Thread.sleep(500);
//      Assert.AreEqual(TaskStatus.RanToCompletion, t.Status, "Read hung on the null reader.");
//      sdr.ExpectNoMoreReaders();
//    }
//  }
//
//  /**
//   * Check that we do not hang when trying to read after adding a reader with an exception.
//   */
//  public final void TestReadFromReaderWithException() {
//    // The code below exposes a flaw in our current implementation related to
//    // CompleteResults semantics and the internal c-tor.  The flaw does not
//    // leak out to customers because the MultiShardCommand object manages the
//    // necessary logic, but we need to patch the flaw so it doesn't end up
//    // inadvertently leaking out to customers.
//    // See VSTS 2616238 (i believe).  Philip will be modofying logic and
//    // augmenting tests to deal with this issue.
//    //
//
//    // Pass a reader with an exception that read does not hang.
//    LabeledDbDataReader[] readers = new LabeledDbDataReader[2];
//    readers[0] = GetReader(_conn1, "select 1");
//    LabeledDbDataReader(new MultiShardException(), new ShardLocation("foo", "bar"), new SqlCommand
//        tempVar = new LabeledDbDataReader(new MultiShardException(),
//            new ShardLocation("foo", "bar"), new SqlCommand();
//    tempVar.Connection = _conn2;
//    readers[1] = tempVar);
//
//    try (MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, readers,
//        MultiShardExecutionPolicy.CompleteResults, true)) {
//      Task t = Task.Factory.StartNew(() -> {
//        while (sdr.Read()) {
//        }
//      });
//
//      Thread.sleep(500);
//      Assert.AreEqual(TaskStatus.RanToCompletion, t.Status, "Read hung on the garbage reader.");
//      sdr.ExpectNoMoreReaders();
//    }
//  }
//
//  /**
//   * Validate that we throw an exception and invalidate the
//   * MultiShardDataReader when we encounter a reader that has
//   * multiple result sets
//   */
//  public final void TestReadFromReaderWithNextResultException() {
//    String selectSql =
//        "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable WHERE Test_int_Field = 876;"
//            + "\r\n" +
//            "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable WHERE Test_int_Field = 876";
//
//    LabeledDbDataReader[] readers = new LabeledDbDataReader[1];
//    readers[0] = GetReader(_conn1, selectSql);
//
//    MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, readers,
//        MultiShardExecutionPolicy.CompleteResults, true);
//
//    AssertExtensions.<UnsupportedOperationException>WaitAndAssertThrows(sdr.NextResultAsync());
//    Assert.IsTrue(sdr.IsClosed, "Expected MultiShardDataReader to be closed!");
//  }
//
//  /**
//   * Check that we throw as expected when trying to add a LabeledDataReader with a null DbDataReader
//   * underneath.
//   */
//  public final void TestAddLabeledDataReaderWithNullDbDataReader() {
//    // What we're doing:
//    // Set up a new sharded reader
//    // Add two readers to it.
//    // Try to add a third reader to it that has a null DbDataReader underneath.
//    // Verify that we threw as expected.
//    //
//    String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable WHERE Test_int_Field = 876";
//    LabeledDbDataReader[] readers = new LabeledDbDataReader[3];
//    readers[0] = GetReader(_conn1, selectSql);
//    readers[1] = GetReader(_conn2, selectSql);
//    DbDataReader nothing = null;
//    LabeledDbDataReader(nothing, new ShardLocation(_conn2.DataSource, _conn2.Database),
//        new SqlCommand tempVar = new LabeledDbDataReader(nothing,
//            new ShardLocation(_conn2.DataSource, _conn2.Database), new SqlCommand();
//    tempVar.Connection = _conn2;
//    readers[2] = tempVar);
//
//    try (MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, readers,
//        MultiShardExecutionPolicy.CompleteResults, true)) {
//    }
//  }
//
//  /**
//   * Check that we throw as expected when trying to add a LabeledDataReader after the sharded data
//   * reader should be closed.
//   */
//  public final void TestAddDataReaderAfterShardedReaderIsClosed() {
//    // What we're doing:
//    // Set up a new sharded reader
//    // Add two readers to it.
//    // Close it.
//    // Try to add a third reader to it.
//    // Verify that we threw as expected.
//    //
//    String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable WHERE Test_int_Field = 876";
//    LabeledDbDataReader[] readers = new LabeledDbDataReader[3];
//    readers[0] = GetReader(_conn1, selectSql);
//    readers[1] = GetReader(_conn2, selectSql);
//    LabeledDbDataReader readerToAddAfterClose = GetReader(_conn3, selectSql);
//
//    try (MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, readers,
//        MultiShardExecutionPolicy.CompleteResults, true, 5)) {
//      sdr.AddReader(readers[0]);
//      sdr.AddReader(readers[1]);
//      sdr.Close();
//      assert sdr.IsClosed;
//      ExpectException<MultiShardDataReaderInternalException, LabeledDbDataReader>
//      (sdr.AddReader, readerToAddAfterClose);
//    }
//  }
//
//  /**
//   * Check that we can successfully read from readers before all readers are added.
//   */
//  public final void TestReadsWhileReadersBeingAddedWithPartialResults() {
//    String selectSql = "SELECT 1";
//
//    try (MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand,
//        new LabeledDbDataReader[0], MultiShardExecutionPolicy.PartialResults, true, 1000)) {
//      for (int i = 0; i < 1000; i++) {
//        sdr.AddReader(GetReader(_conn1, selectSql));
//        Assert.IsTrue(sdr.Read(), "MultiShardReader did not pick up newly added readers.");
//      }
//    }
//  }
//
//  /**
//   * Check that we throw as expected when trying to add a closed LabeledDataReader
//   */
//  public final void TestAddClosedDataReaderAfterShardedReader() {
//    // The code below exposes a flaw in our current implementation related to
//    // CompleteResults semantics and the internal c-tor.  The flaw does not
//    // leak out to customers because the MultiShardCommand object manages the
//    // necessary logic, but we need to patch the flaw so it doesn't end up
//    // inadvertently leaking out to customers.
//    // See VSTS 2616238 (i believe).  Philip will be modofying logic and
//    // augmenting tests to deal with this issue.
//    //
//
//    // What we're doing:
//    // Set up a new sharded reader
//    // Add two readers to it.
//    // Try to add a third closed reader to it.
//    // Verify that we threw as expected.
//    //
//    String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable WHERE Test_int_Field = 876";
//    LabeledDbDataReader[] readers = new LabeledDbDataReader[3];
//    readers[0] = GetReader(_conn1, selectSql);
//    readers[1] = GetReader(_conn2, selectSql);
//    LabeledDbDataReader closedReaderToAdd = GetReader(_conn3, selectSql);
//
//    try (MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, readers,
//        MultiShardExecutionPolicy.CompleteResults, true, 6)) {
//      sdr.AddReader(readers[0]);
//      sdr.AddReader(readers[1]);
//      closedReaderToAdd.DbDataReader.Close();
//      Assert.IsTrue(closedReaderToAdd.DbDataReader.IsClosed,
//          "labeledDataReader was not successfully closed.");
//      sdr.AddReader(closedReaderToAdd);
//      Assert.AreEqual(1, sdr.MultiShardExceptions.size(),
//          "Adding a closed reader did not trigger the logging of an exception.");
//      Assert.IsInstanceOfType(sdr.MultiShardExceptions.First().InnerException,
//          MultiShardDataReaderClosedException.class, "The incorrect exception type was detected.");
//    }
//  }
//
//  /**
//   * Check that we successfuly support the addition of readers after initial creation when the
//   * expected number of readers is greater than those provided.
//   */
//  public final void TestAddDataReaderWhileExpectingAdditionalReaders() {
//    String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable WHERE Test_int_Field = 876";
//    LabeledDbDataReader[] readers = new LabeledDbDataReader[3];
//    readers[0] = GetReader(_conn1, selectSql);
//    readers[1] = GetReader(_conn2, selectSql);
//    readers[2] = GetReader(_conn3, selectSql);
//
//    try (MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, readers,
//        MultiShardExecutionPolicy.CompleteResults, true, 5)) {
//      sdr.AddReader(readers[0]);
//      sdr.AddReader(readers[1]);
//      ExpectException<MultiShardDataReaderInternalException, LabeledDbDataReader>
//      (sdr.AddReader, readers[2]);
//    }
//  }
//
//  /**
//   * Check that we successfuly support the asynchronous addition of readers while we are in the
//   * process of reading.
//   */
//  public final void TestAddDataReaderWhileReadingRows() {
//    ArrayList<Tuple<Integer, java.time.LocalDateTime>> readersAddedTimes = new ArrayList<Tuple<Integer, java.time.LocalDateTime>>();
//    ArrayList<Tuple<Integer, java.time.LocalDateTime>> readersReadTimes = new ArrayList<Tuple<Integer, java.time.LocalDateTime>>();
//    ArrayList<SqlConnection> connections = new ArrayList<SqlConnection>(
//        Arrays.asList(new SqlConnection[]{_conn1, _conn2, _conn3}));
//
//    LabeledDbDataReader[] readers = new LabeledDbDataReader[0];
//
//    try (MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, readers,
//        MultiShardExecutionPolicy.CompleteResults,
//        addShardNamePseudoColumn:true,expectedReaderCount:connections.size())){
//      Task.Factory.StartNew(() -> {
//        for (int readerIndex = 0; readerIndex < connections.size(); readerIndex++) {
//          Thread.sleep(TimeSpan.FromMilliseconds(500));
//          readersAddedTimes.add(new Tuple<Integer, java.time.LocalDateTime>(readerIndex,
//              java.time.LocalDateTime.UtcNow));
//          sdr.AddReader(
//              GetReader(connections.get(readerIndex), "select " + String.valueOf(readerIndex)));
//        }
//      });
//
//      int i = 0;
//      while (sdr.Read()) {
//        readersReadTimes.add(
//            new Tuple<Integer, java.time.LocalDateTime>(Integer.parseInt(sdr[0].toString()),
//                java.time.LocalDateTime.UtcNow));
//        i++;
//      }
//
//      for (Tuple<Integer, java.time.LocalDateTime> tuple : readersAddedTimes) {
//        Trace.TraceInformation("Reader {0} was added at {1:O}", tuple.Item1, tuple.Item2);
//      }
//      for (Tuple<Integer, java.time.LocalDateTime> tuple : readersReadTimes) {
//        Trace.TraceInformation("Reader {0} was read at {1:O}", tuple.Item1, tuple.Item2);
//      }
//
//      Assert.AreEqual(3, i, "Not all rows successfully returned.");
//      for (boolean happenedInOrder : readersAddedTimes
//          .Zip(readersReadTimes, (x, y) -> y.Item2 >= x.Item2)) {
//        Assert.IsTrue(happenedInOrder,
//            "The next row was somehow able to be retrieved before its corresponding reader was added.");
//      }
//    }
//  }
//
//  /**
//   * Check that we successfuly support the asynchronous addition of readers while we are in the
//   * process of reading, when we start with some readers already added.
//   */
//  public final void TestAddDataReaderWhileReadingRowsWhenReadersAlreadyPresent() {
//    ArrayList<Tuple<Integer, java.time.LocalDateTime>> readersAddedTimes = new ArrayList<Tuple<Integer, java.time.LocalDateTime>>();
//    ArrayList<Tuple<Integer, java.time.LocalDateTime>> readersReadTimes = new ArrayList<Tuple<Integer, java.time.LocalDateTime>>();
//    ArrayList<SqlConnection> connections = new ArrayList<SqlConnection>(
//        Arrays.asList(new SqlConnection[]{_conn1, _conn2, _conn3}));
//
//    LabeledDbDataReader[] readers = new LabeledDbDataReader[]{
//        GetReader(connections.get(0), "SELECT 0")};
//
//    try (MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, readers,
//        MultiShardExecutionPolicy.CompleteResults,
//        addShardNamePseudoColumn:true,expectedReaderCount:connections.size())){
//      readersAddedTimes
//          .add(new Tuple<Integer, java.time.LocalDateTime>(0, java.time.LocalDateTime.UtcNow));
//
//      Task.Factory.StartNew(() -> {
//        // First reader is already added, add two remaining asynchronously
//        for (int readerIndex = 1; readerIndex < connections.size(); readerIndex++) {
//          Thread.sleep(TimeSpan.FromMilliseconds(500));
//          readersAddedTimes.add(new Tuple<Integer, java.time.LocalDateTime>(readerIndex,
//              java.time.LocalDateTime.UtcNow));
//          sdr.AddReader(
//              GetReader(connections.get(readerIndex), "select " + String.valueOf(readerIndex)));
//        }
//      });
//
//      int i = 0;
//      while (sdr.Read()) {
//        readersReadTimes.add(
//            new Tuple<Integer, java.time.LocalDateTime>(Integer.parseInt(sdr[0].toString()),
//                java.time.LocalDateTime.UtcNow));
//        i++;
//      }
//
//      for (Tuple<Integer, java.time.LocalDateTime> tuple : readersAddedTimes) {
//        Trace.TraceInformation("Reader {0} was added at {1:O}", tuple.Item1, tuple.Item2);
//      }
//      for (Tuple<Integer, java.time.LocalDateTime> tuple : readersReadTimes) {
//        Trace.TraceInformation("Reader {0} was read at {1:O}", tuple.Item1, tuple.Item2);
//      }
//
//      Assert.AreEqual(3, i, "Not all rows successfully returned.");
//      for (boolean happenedInOrder : readersAddedTimes
//          .Zip(readersReadTimes, (x, y) -> y.Item2 >= x.Item2)) {
//        Assert.IsTrue(happenedInOrder,
//            "The next row was somehow able to be retrieved before its corresponding reader was added.");
//      }
//    }
//  }
//
//  /**
//   * Check that we wait a long time until we explicitly call ExpectNoMoreReaders
//   */
//  public final void TestAddDataReaderWaitsALongTimeForExpectedReadersUntilExpectNoMoreReadersIsCalled() {
//    String selectSql = "SELECT 1";
//    LabeledDbDataReader[] readers = new LabeledDbDataReader[0];
//
//    try (MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, readers,
//        MultiShardExecutionPolicy.CompleteResults, true, 4)) {
//      // Launch a task that adds readers (but 1 too few). We expect 4, but we only add 3.
//      sdr.AddReader(GetReader(_conn1, selectSql));
//      sdr.AddReader(GetReader(_conn2, selectSql));
//      sdr.AddReader(GetReader(_conn3, selectSql));
//      boolean readingCompleted = false;
//
//      CancellationTokenSource ctSource = new CancellationTokenSource();
//
//      // Launch a task that reads rows.
//      Task readerTask = Task.Factory.StartNew(() -> {
//        while (sdr.Read()) {
//        }
//        readingCompleted = true;
//      }, ctSource.Token);
//
//      try {
//        while (readerTask.Status != TaskStatus.Canceled
//            && readerTask.Status != TaskStatus.RanToCompletion
//            && readerTask.Status != TaskStatus.Faulted) {
//          Thread.sleep(50);
//          sdr.ExpectNoMoreReaders();
//        }
//        Assert.IsTrue(readingCompleted,
//            "The reader's read call did not return false after ExpectNoMoreReaders was called.");
//      } finally {
//        ctSource.Cancel();
//      }
//    }
//  }
//
//  /**
//   * A little stress test that tries adding a few readers concurrently. Make sure no exceptions were
//   * thrown.
//   */
//  public final void TestAddDataReadersConcurrently() {
//    String selectSql = "SELECT 1";
//    LabeledDbDataReader[] empty = new LabeledDbDataReader[0];
//    List<LabeledDbDataReader> readers = _conns.Select((x) -> GetReader(x, selectSql));
//
//    try (MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, empty,
//        MultiShardExecutionPolicy.CompleteResults, true, 3)) {
//      for (LabeledDbDataReader reader : readers) {
//        Task.Factory.StartNew(() -> {
//          sdr.AddReader(reader);
//        });
//      }
//
//      int rowsRead = 0;
//      while (sdr.Read()) {
//        rowsRead++;
//      }
//      Assert.AreEqual(3, rowsRead, "Not all expected rows were read.");
//    }
//  }
//
//  /**
//   * Check that we throw as expected when trying to call CreateObjRef.
//   */
//  public final void TestCreateObjRefFails() {
//    // What we're doing:
//    // Set up a new sharded reader
//    // Try to call CreateObjRef on it.
//    // Verify that we threw as expected.
//    //
//    String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable";
//
//    try (MultiShardDataReader sdr = GetShardedDbReader(_shardConnection, selectSql)) {
//      sdr.CreateObjRef(MultiShardDataReader.class);
//    }
//  }
//
//  /**
//   * Check that we can iterate through the result sets as expected comparing all the values
//   * returned from the getters plus some of the properties.
//   * Check everythign both with and without the $ShardName pseudo column.
//   */
//  public final void TestGettersPositiveCases() {
//    TestGettersPositiveCasesHelper(true);
//    TestGettersPositiveCasesHelper(false);
//  }
//
//  /**
//   * Check that we can iterate through the result sets as expected comparing all the values
//   * returned from the getters plus some of the properties.
//   */
//  private void TestGettersPositiveCasesHelper(boolean includeShardNamePseudoColumn) {
//    // What we're doing:
//    // Grab all rows from each test database.
//    // Load them into a MultiShardDataReader.
//    // Iterate through the rows and make sure that we have 9 total.
//    // Also iterate through all columns and make sure that the getters that should work do work.
//    //
//    List<MutliShardTestCaseColumn> toCheck = MutliShardTestCaseColumn.DefinedColumns;
//    MutliShardTestCaseColumn pseudoColumn = MutliShardTestCaseColumn.ShardNamePseudoColumn;
//
//    for (MutliShardTestCaseColumn curCol : toCheck) {
//      String selectSql = String
//          .format("SELECT %1$s FROM ConsistentShardedTable", curCol.TestColumnName);
//
//      try (MultiShardDataReader sdr = GetShardedDbReader(_shardConnection, selectSql,
//          includeShardNamePseudoColumn)) {
//        int recordsRetrieved = 0;
//        Logger.Log("Starting to get records");
//        while (sdr.Read()) {
//          int expectedFieldCount = includeShardNamePseudoColumn ? 2
//              : 1; // 2 columns if we have the shard name, 1 column if not.
//          assert expectedFieldCount == sdr.FieldCount;
//          assert expectedFieldCount == sdr.VisibleFieldCount;
//
//          recordsRetrieved++;
//
//          // Do verification for the test column.
//          //
//          CheckDataTypeName(sdr, curCol, 0);
//          CheckColumnName(sdr, curCol, 0);
//          VerifyAllGettersPositiveCases(sdr, curCol, 0);
//
//          // Then also do it for the $ShardName PseudoColumn if necessary.
//          //
//          if (includeShardNamePseudoColumn) {
//            CheckDataTypeName(sdr, pseudoColumn, 1);
//            CheckColumnName(sdr, pseudoColumn, 1);
//            VerifyAllGettersPositiveCases(sdr, pseudoColumn, 1);
//          }
//        }
//
//        sdr.Close();
//
//        assert recordsRetrieved == 9;
//      }
//    }
//  }
//
//  /**
//   * Test what happens when we try to get a value without calling read first.
//   */
//  public final void TestBadlyPlacedGetValueCalls() {
//    // What we're doing:
//    // Set up a new sharded reader
//    // Try to get a value without calling read first and see what happens.
//    // Should throw.
//    //
//    String selectSql = "SELECT 1";
//    try (MultiShardDataReader sdr = GetShardedDbReader(_shardConnection, selectSql)) {
//      ExpectException<IllegalStateException> (sdr.GetValue, 0);
//
//      while (sdr.Read()) {
//        sdr.GetValue(0);
//      }
//
//      ExpectException<IllegalStateException> (sdr.GetValue, 0);
//
//      sdr.Close();
//
//      ExpectException<MultiShardDataReaderClosedException> (sdr.GetValue, 0);
//
//      // And try to close it again.
//      //
//      sdr.Close();
//    }
//  }
//
//  private <T extends RuntimeException> void ExpectException(
//      tangible.Func1Param<Integer, Object> func, int ordinal) {
//    try {
//      func.invoke(ordinal);
//      Assert.Fail(String.format("Should have hit %1$s.", T.class));
//    } catch (T e) {
//    }
//  }
//
//  private <T extends RuntimeException, U> void ExpectException(tangible.Action1Param<U> func,
//      U input) {
//    try {
//      func.invoke(input);
//      Assert.Fail(String.format("Should have hit %1$s.", T.class));
//    } catch (T e) {
//    }
//  }
//
//  private void CheckColumnName(MultiShardDataReader reader, MutliShardTestCaseColumn column,
//      int ordinal) {
//    assert column.TestColumnName == reader.GetName(ordinal);
//    assert ordinal == reader.GetOrdinal(column.TestColumnName);
//  }
//
//  private void CheckDataTypeName(MultiShardDataReader reader, MutliShardTestCaseColumn column,
//      int ordinal) {
//    // Not happy about this hack for numeric, but not sure how else to deal with it.
//    //
//    if (column.SqlServerDatabaseEngineType.equals("numeric")) {
//      assert reader.GetDataTypeName(ordinal).equals("decimal");
//    } else {
//      assert reader.GetDataTypeName(ordinal) == column.SqlServerDatabaseEngineType;
//    }
//  }
//
//  private void VerifyAllGettersPositiveCases(MultiShardDataReader reader,
//      MutliShardTestCaseColumn column, int ordinal) {
//    // General pattern here:
//    // Grab the value through the regular getter, through the getValue,
//    // through the sync GetFieldValue, and through the async GetFieldValue to ensure we are
//    // getting back the same thing from all calls.
//    //
//    // Then grab through the Sql getter to make sure it works. (should we compare again?)
//    //
//    // Then verify that the field types are as we expect.
//    //
//    // Note: For the array-based getters we can't do the sync/async comparison.
//    //
//
//    // These are indexes into our .NET type array.
//    //
//    int ValueResult = 0;
//    int ItemOrdinalResult = 1;
//    int ItemNameResult = 2;
//    int GetResult = 3;
//    int SyncResult = 4;
//    int AsyncResult = 5;
//    Object[] results = new Object[AsyncResult + 1];
//
//    // These first three we can pull consistently for all fields.
//    // The rest have type specific getters.
//    //
//    results[ValueResult] = reader.GetValue(ordinal);
//    results[ItemOrdinalResult] = reader[ordinal];
//    results[ItemNameResult] = reader[column.TestColumnName];
//
//    // And these are indexes into our SQL type array.
//    //
//    int SqlValueResult = 0;
//    int SqlGetResult = 1;
//    Object[] sqlResults = new Object[SqlGetResult + 1];
//
//    sqlResults[SqlValueResult] = reader.GetSqlValue(ordinal);
//
//    switch (column.DbType) {
//      case SqlDbType.BigInt:
//        if (!reader.IsDBNull(ordinal)) {
//          results[GetResult] = reader.GetInt64(ordinal);
//          results[SyncResult] = reader.<Long>GetFieldValue(ordinal);
//          results[AsyncResult] = reader.<Long>GetFieldValueAsync(ordinal).Result;
//          AssertAllAreEqual(results);
//
//          sqlResults[SqlGetResult] = reader.GetSqlInt64(ordinal);
//          AssertAllAreEqual(sqlResults);
//
//          assert reader.GetFieldType(ordinal) == Long.class;
//        }
//        break;
//      case SqlDbType.Binary:
//      case SqlDbType.Image:
//      case SqlDbType.VarBinary:
//        if (!reader.IsDBNull(ordinal)) {
//          // Do the bytes and the stream.  Can also compare them.
//          //
////WARNING: Unsigned integer types have no direct equivalent in Java:
////ORIGINAL LINE: Byte[] byteBuffer = new Byte[column.FieldLength];
//          byte[] byteBuffer = new byte[column.FieldLength];
//          reader.GetBytes(ordinal, 0, byteBuffer, 0, column.FieldLength);
//
//          java.io.InputStream theStream = reader.GetStream(ordinal);
//          assert theStream.getLength() == column.FieldLength;
////WARNING: Unsigned integer types have no direct equivalent in Java:
////ORIGINAL LINE: Byte[] byteBufferFromStream = new Byte[column.FieldLength];
//          byte[] byteBufferFromStream = new byte[column.FieldLength];
//
//          int bytesRead = theStream.read(byteBufferFromStream, 0, column.FieldLength);
////WARNING: Unsigned integer types have no direct equivalent in Java:
////ORIGINAL LINE: PerformArrayComparison<Byte>(byteBuffer, byteBufferFromStream);
//          this.<Byte>PerformArrayComparison(byteBuffer, byteBufferFromStream);
//
//          // The value getter comes through as a SqlBinary, so we don't pull
//          // the Sql getter here.
//          //
//          reader.GetSqlBytes(ordinal);
//
//          sqlResults[SqlGetResult] = reader.GetSqlBinary(ordinal);
//          AssertAllAreEqual(sqlResults);
//
////WARNING: Unsigned integer types have no direct equivalent in Java:
////ORIGINAL LINE: Assert.AreEqual(reader.GetFieldType(ordinal), typeof(Byte[]));
//          assert reader.GetFieldType(ordinal) == byte[].class;
//        }
//        break;
//      case SqlDbType.Bit:
//        if (!reader.IsDBNull(ordinal)) {
//          results[GetResult] = reader.GetBoolean(ordinal);
//          results[SyncResult] = reader.<Boolean>GetFieldValue(ordinal);
//          results[AsyncResult] = reader.<Boolean>GetFieldValueAsync(ordinal).Result;
//          AssertAllAreEqual(results);
//
//          sqlResults[SqlGetResult] = reader.GetSqlBoolean(ordinal);
//          AssertAllAreEqual(sqlResults);
//
//          assert reader.GetFieldType(ordinal) == Boolean.class;
//        }
//        break;
//      case SqlDbType.Char:
//      case SqlDbType.NChar:
//      case SqlDbType.NText:
//      case SqlDbType.NVarChar:
//      case SqlDbType.Text:
//      case SqlDbType.VarChar:
//        if (!reader.IsDBNull(ordinal)) {
//          char[] charBuffer = new char[column.FieldLength];
//          long bufferLength = reader.GetChars(ordinal, 0, charBuffer, 0, column.FieldLength);
//          charBuffer = new char[bufferLength]; //size it right for the string compare below.
//          reader.GetChars(ordinal, 0, charBuffer, 0, column.FieldLength);
//
//          // The value getter comes through as a SqlString, so we
//          // don't pull the Sql getter here.
//          //
//          reader.GetSqlChars(ordinal);
//
//          results[GetResult] = reader.GetString(ordinal);
//          results[SyncResult] = reader.<String>GetFieldValue(ordinal);
//          results[AsyncResult] = reader.<String>GetFieldValueAsync(ordinal).Result;
//          AssertAllAreEqual(results);
//
//          // Also compare the string result to our char result.
//          //
//          this.<Character>PerformArrayComparison(charBuffer,
//              reader.GetString(ordinal).trim().toCharArray());
//
//          // and get a text reader.
//          //
//          String fromTr = reader.GetTextReader(ordinal).ReadToEnd();
//          assert fromTr == results[GetResult];
//
//          sqlResults[SqlGetResult] = reader.GetSqlString(ordinal);
//          AssertAllAreEqual(sqlResults);
//
//          assert reader.GetFieldType(ordinal) == String.class;
//        }
//        break;
//      case SqlDbType.DateTime:
//      case SqlDbType.SmallDateTime:
//        if (!reader.IsDBNull(ordinal)) {
//          results[GetResult] = reader.GetDateTime(ordinal);
//          results[SyncResult] = reader.<java.time.LocalDateTime>GetFieldValue(ordinal);
//          results[AsyncResult] = reader.<java.time.LocalDateTime>GetFieldValueAsync(ordinal).Result;
//          AssertAllAreEqual(results);
//
//          sqlResults[SqlGetResult] = reader.GetSqlDateTime(ordinal);
//          AssertAllAreEqual(sqlResults);
//
//          assert reader.GetFieldType(ordinal) == java.time.LocalDateTime.class;
//        }
//        break;
//      case SqlDbType.Date: // NOTE: docs say this can come back via SqlDateTime, but apparently it can't.
//      case SqlDbType.DateTime2:
//        // differs from above in the sql specific Getter.
//        if (!reader.IsDBNull(ordinal)) {
//          results[GetResult] = reader.GetDateTime(ordinal);
//          results[SyncResult] = reader.<java.time.LocalDateTime>GetFieldValue(ordinal);
//          results[AsyncResult] = reader.<java.time.LocalDateTime>GetFieldValueAsync(ordinal).Result;
//          AssertAllAreEqual(results);
//
//          assert reader.GetFieldType(ordinal) == java.time.LocalDateTime.class;
//        }
//        break;
//      case SqlDbType.DateTimeOffset:
//        if (!reader.IsDBNull(ordinal)) {
//          results[GetResult] = reader.GetDateTimeOffset(ordinal);
//          results[SyncResult] = reader.<DateTimeOffset>GetFieldValue(ordinal);
//          results[AsyncResult] = reader.<DateTimeOffset>GetFieldValueAsync(ordinal).Result;
//          AssertAllAreEqual(results);
//
//          assert reader.GetFieldType(ordinal) == DateTimeOffset.class;
//        }
//        break;
//      case SqlDbType.Decimal:
//        if (!reader.IsDBNull(ordinal)) {
//          results[GetResult] = reader.GetDecimal(ordinal);
//          results[SyncResult] = reader.<java.math.BigDecimal>GetFieldValue(ordinal);
//          results[AsyncResult] = reader.<java.math.BigDecimal>GetFieldValueAsync(ordinal).Result;
//          AssertAllAreEqual(results);
//
//          sqlResults[SqlGetResult] = reader.GetSqlDecimal(ordinal);
//          AssertAllAreEqual(sqlResults);
//
//          assert reader.GetFieldType(ordinal) == java.math.BigDecimal.class;
//        }
//        break;
//      case SqlDbType.Float:
//        if (!reader.IsDBNull(ordinal)) {
//          results[GetResult] = reader.GetDouble(ordinal);
//          results[SyncResult] = reader.<Double>GetFieldValue(ordinal);
//          results[AsyncResult] = reader.<Double>GetFieldValueAsync(ordinal).Result;
//          AssertAllAreEqual(results);
//
//          sqlResults[SqlGetResult] = reader.GetSqlDouble(ordinal);
//          AssertAllAreEqual(sqlResults);
//
//          assert reader.GetFieldType(ordinal) == Double.class;
//        }
//        break;
//      case SqlDbType.Int:
//        if (!reader.IsDBNull(ordinal)) {
//          results[GetResult] = reader.GetInt32(ordinal);
//          results[SyncResult] = reader.<Integer>GetFieldValue(ordinal);
//          results[AsyncResult] = reader.<Integer>GetFieldValueAsync(ordinal).Result;
//          AssertAllAreEqual(results);
//
//          sqlResults[SqlGetResult] = reader.GetSqlInt32(ordinal);
//          AssertAllAreEqual(sqlResults);
//
//          assert reader.GetFieldType(ordinal) == Integer.class;
//        }
//        break;
//      case SqlDbType.Money:
//      case SqlDbType.SmallMoney:
//        if (!reader.IsDBNull(ordinal)) {
//          results[GetResult] = reader.GetDecimal(ordinal);
//          results[SyncResult] = reader.<java.math.BigDecimal>GetFieldValue(ordinal);
//          results[AsyncResult] = reader.<java.math.BigDecimal>GetFieldValueAsync(ordinal).Result;
//          AssertAllAreEqual(results);
//
//          sqlResults[SqlGetResult] = reader.GetSqlMoney(ordinal);
//          AssertAllAreEqual(sqlResults);
//
//          assert reader.GetFieldType(ordinal) == java.math.BigDecimal.class;
//        }
//        break;
//      case SqlDbType.Real:
//        if (!reader.IsDBNull(ordinal)) {
//          results[GetResult] = reader.GetFloat(ordinal);
//          results[SyncResult] = reader.<Float>GetFieldValue(ordinal);
//          results[AsyncResult] = reader.<Float>GetFieldValueAsync(ordinal).Result;
//          AssertAllAreEqual(results);
//
//          sqlResults[SqlGetResult] = reader.GetSqlSingle(ordinal);
//          AssertAllAreEqual(sqlResults);
//
//          assert reader.GetFieldType(ordinal) == Float.class;
//        }
//        break;
//      case SqlDbType.SmallInt:
//        if (!reader.IsDBNull(ordinal)) {
//          results[GetResult] = reader.GetInt16(ordinal);
//          results[SyncResult] = reader.<Short>GetFieldValue(ordinal);
//          results[AsyncResult] = reader.<Short>GetFieldValueAsync(ordinal).Result;
//          AssertAllAreEqual(results);
//
//          sqlResults[SqlGetResult] = reader.GetSqlInt16(ordinal);
//          AssertAllAreEqual(sqlResults);
//
//          assert reader.GetFieldType(ordinal) == Short.class;
//        }
//        break;
//      case SqlDbType.Time: // NOTE: docs say this can come back via GetDateTime, but apparently it can't.
//        if (!reader.IsDBNull(ordinal)) {
//          results[GetResult] = reader.GetTimeSpan(ordinal);
//          results[SyncResult] = reader.<TimeSpan>GetFieldValue(ordinal);
//          results[AsyncResult] = reader.<TimeSpan>GetFieldValueAsync(ordinal).Result;
//          AssertAllAreEqual(results);
//
//          assert reader.GetFieldType(ordinal) == TimeSpan.class;
//        }
//        break;
//      case SqlDbType.Timestamp:
//        if (!reader.IsDBNull(ordinal)) {
//          // Differs from the other binaries in that it doesn't
//          // support GetStream.
//          //
////WARNING: Unsigned integer types have no direct equivalent in Java:
////ORIGINAL LINE: Byte[] byteBuffer = new Byte[column.FieldLength];
//          byte[] byteBuffer = new byte[column.FieldLength];
//          reader.GetBytes(ordinal, 0, byteBuffer, 0, column.FieldLength);
//
//          // The value getter comes through as a SqlBinary, so we don't pull
//          // the Sql getter here.
//          //
//          reader.GetSqlBytes(ordinal);
//
//          sqlResults[SqlGetResult] = reader.GetSqlBinary(ordinal);
//          AssertAllAreEqual(sqlResults);
//
////WARNING: Unsigned integer types have no direct equivalent in Java:
////ORIGINAL LINE: Assert.AreEqual(reader.GetFieldType(ordinal), typeof(Byte[]));
//          assert reader.GetFieldType(ordinal) == byte[].class;
//        }
//        break;
//      case SqlDbType.TinyInt:
//        if (!reader.IsDBNull(ordinal)) {
//          results[GetResult] = reader.GetByte(ordinal);
////WARNING: Unsigned integer types have no direct equivalent in Java:
////ORIGINAL LINE: results[SyncResult] = reader.GetFieldValue<Byte>(ordinal);
//          results[SyncResult] = reader.<Byte>GetFieldValue(ordinal);
////WARNING: Unsigned integer types have no direct equivalent in Java:
////ORIGINAL LINE: results[AsyncResult] = reader.GetFieldValueAsync<Byte>(ordinal).Result;
//          results[AsyncResult] = reader.<Byte>GetFieldValueAsync(ordinal).Result;
//          AssertAllAreEqual(results);
//
//          sqlResults[SqlGetResult] = reader.GetSqlByte(ordinal);
//          AssertAllAreEqual(sqlResults);
//
////WARNING: Unsigned integer types have no direct equivalent in Java:
////ORIGINAL LINE: Assert.AreEqual(reader.GetFieldType(ordinal), typeof(Byte));
//          assert reader.GetFieldType(ordinal) == Byte.class;
//        }
//        break;
//      case SqlDbType.UniqueIdentifier:
//        if (!reader.IsDBNull(ordinal)) {
//          results[GetResult] = reader.GetGuid(ordinal);
//          results[SyncResult] = reader.<UUID>GetFieldValue(ordinal);
//          results[AsyncResult] = reader.<UUID>GetFieldValueAsync(ordinal).Result;
//          AssertAllAreEqual(results);
//
//          sqlResults[SqlGetResult] = reader.GetSqlGuid(ordinal);
//          AssertAllAreEqual(sqlResults);
//
//          assert reader.GetFieldType(ordinal) == UUID.class;
//        }
//        break;
//      default:
//        throw new IllegalArgumentException(column.DbType.toString());
//    }
//  }
//
//  private void AssertAllAreEqual(Object[] toCheck) {
//    Object baseline = toCheck[0];
//    for (Object curObject : toCheck) {
//      assert baseline == curObject;
//    }
//  }
//
//  private <T> void PerformArrayComparison(T[] first, T[] second) {
//    assert first.length == second.length;
//    for (int i = 0; i < first.length; i++) {
//      assert first[i] == second[i];
//    }
//  }
//
//  /**
//   * Gets a SqlDataReader by executing the passed in t-sql over the passed in connection.
//   *
//   * @param conn Connection to the database we wish to execute the t-sql against.
//   * @param tsql The t-sql to execute.
//   * @return The SqlDataReader obtained by executin the passed in t-sql over the passed in
//   * connection.
//   */
//  private LabeledDbDataReader GetReader(DbConnection conn, String tsql) {
//    String label = GetLabel(conn);
//    if (conn.State == ConnectionState.Open) {
//      conn.Close();
//    }
//    if (conn.State != ConnectionState.Open) {
//      conn.Open();
//    }
//    DbCommand cmd = conn.CreateCommand();
//    cmd.CommandText = tsql;
//    DbDataReader sdr = cmd.ExecuteReader();
//    LabeledDbDataReader(sdr, new ShardLocation(cmd.Connection.DataSource, cmd.Connection.Database),
//        new MockSqlStatement tempVar = new LabeledDbDataReader(sdr,
//            new ShardLocation(cmd.Connection.DataSource, cmd.Connection.Database),
//            new MockSqlStatement();
//    tempVar.Connection = conn;
//    return tempVar);
//  }
//
//  /**
//   * Gets a shard label based on the datasource and database from a connection;
//   *
//   * @param conn The connection to pull the datasource/database information from.
//   * @return The label of the form 'datasource' ; 'database'.
//   */
//  private String GetLabel(DbConnection conn) {
//    return String.format("%1$s ; %2$s", conn.DataSource, conn.Database);
//  }
//
//  /**
//   * Helper that grabs a MultiShardDataReader based on a MultiShardConnection and a tsql string to
//   * execute.
//   *
//   * @param conn The MultiShardConnection to use to get the command/reader.
//   * @param tsql The tsql to execute on the shards.
//   * @return The MultiShardDataReader resulting from executing the given tsql on the given
//   * connection.
//   */
//  private MultiShardDataReader GetShardedDbReader(MultiShardConnection conn, String tsql) {
////TODO TASK: There is no equivalent to implicit typing in Java:
//    var cmd = conn.CreateCommand();
//    cmd.CommandText = tsql;
//    cmd.ExecutionOptions = MultiShardExecutionOptions.IncludeShardNameColumn;
//    return cmd.ExecuteReader();
//  }
//
//  /**
//   * Helper that grabs a MultiShardDataReader based on a MultiShardConnection and a tsql string to
//   * execute. This is different from the GetShardedDbReader method in that it uses
//   * ExecuteReaderAsync() API under the hood and is cancellable.
//   *
//   * @param conn The MultiShardConnection to use to get the command/reader.
//   * @param tsql The tsql to execute on the shards.
//   * @param cancellationToken The cancellation instruction.
//   * @return The MultiShardDataReader resulting from executing the given tsql on the given
//   * connection.
//   */
//  private MultiShardDataReader GetShardedDbReaderAsync(MultiShardConnection conn, String tsql,
//      CancellationToken cancellationToken) {
////TODO TASK: There is no equivalent to implicit typing in Java:
//    var cmd = conn.CreateCommand();
//    cmd.CommandText = tsql;
//    cmd.ExecutionOptions = MultiShardExecutionOptions.IncludeShardNameColumn;
//    return cmd.ExecuteReaderAsync(cancellationToken).Result;
//  }
//
//  private MultiShardDataReader GetShardedDbReader(MultiShardConnection conn, String tsql,
//      boolean includeShardName) {
////TODO TASK: There is no equivalent to implicit typing in Java:
//    var cmd = conn.CreateCommand();
//    cmd.CommandText = tsql;
//    cmd.ExecutionOptions = includeShardName ? MultiShardExecutionOptions.IncludeShardNameColumn
//        : MultiShardExecutionOptions.None;
//    cmd.ExecutionPolicy = MultiShardExecutionPolicy.PartialResults;
//    return cmd.ExecuteReader(CommandBehavior.Default);
//  }
//
//  /**
//   * Helper method that sets up a MultiShardDataReader based on the given DbDataReaders so that
//   * the MultiShardDataReader is ready to use.
//   *
//   * @param readers The DbDataReaders that will underlie this MultiShardDataReader.
//   * @param exceptions Populated with any SchemaMismatchExceptions encountered while setting up the
//   * MultiShardDataReader.
//   * @param addShardNamePseudoColumn True if we should add the $ShardName pseudo column, false if
//   * not.
//   * @return A new MultiShardDataReader object that is ready to use.
//   *
//   * Note that normally this setup and marking as complete would be hidden from the client (inside
//   * the MultiShardCommand), but since we are doing unit testing at a lower level than the command
//   * we need to perform it ourselves here.
//   */
//  private MultiShardDataReader GetMultiShardDataReaderFromDbDataReaders(
//      LabeledDbDataReader[] readers,
//      ReferenceObjectHelper<List<MultiShardSchemaMismatchException>> exceptions,
//      boolean addShardNamePseudoColumn) {
//    exceptions.argValue = new ArrayList<MultiShardSchemaMismatchException>();
//
//    MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, readers,
//        MultiShardExecutionPolicy.PartialResults, addShardNamePseudoColumn);
//
////TODO TASK: There is no equivalent to implicit typing in Java:
//    for (var exception : sdr.MultiShardExceptions) {
//      exceptions.argValue.add((MultiShardSchemaMismatchException) exception);
//    }
//
//    return sdr;
//  }
}
