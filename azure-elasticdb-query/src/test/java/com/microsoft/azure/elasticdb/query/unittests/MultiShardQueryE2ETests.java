package com.microsoft.azure.elasticdb.query.unittests;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

/**
 * Tests for end to end scenarios where a user
 * connects to his shards, executes commands against them
 * and receives results
 * Purpose:
 * Basic End-To-End test scenarios for the cross shard query client library
 * Notes:
 * Tests currently assume there's a running sqlservr instance.
 * Everything will be automated once we integrate with the larger framework.
 * Currently the tests use the same methods to create shards as MultiShardResultSetTests
 */
public class MultiShardQueryE2ETests {
//
//  private TestContext _testContextInstance;
//
//  /**
//   * Handle on connections to all shards
//   */
//  private MultiShardConnection _shardConnection;
//
//  /**
//   * Handle to the ShardMap with our Test databases.
//   */
//  private ShardMap _shardMap;
//
//  /**
//   * Gets or sets the test context which provides
//   * information about and functionality for the current test run.
//   */
//  public final TestContext getTestContext() {
//    return _testContextInstance;
//  }
//
//  public final void setTestContext(TestContext value) {
//    _testContextInstance = value;
//  }
//
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
//    _shardMap = MultiShardTestUtils.CreateAndGetTestShardMap();
//
//    _shardConnection = new MultiShardConnection(_shardMap.GetShards(),
//        MultiShardTestUtils.ShardConnectionString);
//  }
//
//  /**
//   * Close our connections to each test database after each test.
//   */
//  public final void MyTestCleanup() {
//    // Close connections after each test.
//    //
//    _shardConnection.Dispose();
//  }
//
//  /**
//   * Check that we can iterate through 3 result sets as expected.
//   */
//  public final void TestSimpleSelect_PartialResults() {
//    TestSimpleSelect(MultiShardExecutionPolicy.PartialResults);
//  }
//
//  /**
//   * Check that we can iterate through 3 result sets as expected.
//   */
//  public final void TestSimpleSelect_CompleteResults() {
//    TestSimpleSelect(MultiShardExecutionPolicy.CompleteResults);
//  }
//
//  public final void TestSimpleSelect(MultiShardExecutionPolicy policy) {
//    // What we're doing:
//    // Grab all rows from each test database.
//    // Load them into a MultiShardDataReader.
//    // Iterate through the rows and make sure that we have 9 total.
//    //
//    try (MultiShardConnection conn = new MultiShardConnection(_shardMap.GetShards(),
//        MultiShardTestUtils.ShardConnectionString)) {
//      try (MultiShardCommand cmd = conn.CreateCommand()) {
//        cmd.CommandText = "SELECT dbNameField, Test_int_Field, Test_bigint_Field FROM ConsistentShardedTable";
//        cmd.ExecutionOptions = MultiShardExecutionOptions.IncludeShardNameColumn;
//        cmd.ExecutionPolicy = policy;
//
//        try (MultiShardDataReader sdr = cmd.ExecuteReader()) {
//          int recordsRetrieved = 0;
//          Logger.Log("Starting to get records");
//          while (sdr.Read()) {
//            recordsRetrieved++;
//            String dbNameField = sdr.GetString(0);
//            int testIntField = sdr.<Integer>GetFieldValue(1);
//            long testBigIntField = sdr.<Long>GetFieldValue(2);
//            String shardIdPseudoColumn = sdr.<String>GetFieldValue(3);
//            String logRecord = String.format(
//                "RecordRetrieved: dbNameField: %1$s, TestIntField: %2$s, TestBigIntField: %3$s, shardIdPseudoColumnField: %4$s, RecordCount: %5$s",
//                dbNameField, testIntField, testBigIntField, shardIdPseudoColumn, recordsRetrieved);
//            Logger.Log(logRecord);
//            Debug.WriteLine(logRecord);
//          }
//
//          sdr.Close();
//
//          assert recordsRetrieved == 9;
//        }
//      }
//    }
//  }
//
//
//  /**
//   * Check that we can return an empty result set that has a schema table
//   */
//  public final void TestSelect_NoRows_CompleteResults() {
//    TestSelectNoRows("select 1 where 0 = 1", MultiShardExecutionPolicy.CompleteResults);
//  }
//
//  /**
//   * Check that we can return an empty result set that has a schema table
//   */
//  public final void TestSelect_NoRows_PartialResults() {
//    TestSelectNoRows("select 1 where 0 = 1", MultiShardExecutionPolicy.PartialResults);
//  }
//
//  public final void TestSelectNoRows(String commandText, MultiShardExecutionPolicy policy) {
//    try (MultiShardConnection conn = new MultiShardConnection(_shardMap.GetShards(),
//        MultiShardTestUtils.ShardConnectionString)) {
//      try (MultiShardCommand cmd = conn.CreateCommand()) {
//        cmd.CommandText = commandText;
//        cmd.ExecutionPolicy = policy;
//
//        // Read first
//        try (MultiShardDataReader sdr = cmd.ExecuteReader()) {
//          assert 0 == sdr.MultiShardExceptions.size();
//          while (sdr.Read()) {
//            Assert.Fail("Should not have gotten any records.");
//          }
//          assert !sdr.HasRows;
//        }
//
//        // HasRows first
//        try (MultiShardDataReader sdr = cmd.ExecuteReader()) {
//          assert 0 == sdr.MultiShardExceptions.size();
//          assert !sdr.HasRows;
//          while (sdr.Read()) {
//            Assert.Fail("Should not have gotten any records.");
//          }
//        }
//      }
//    }
//  }
//
//  /**
//   * Check that we can return an empty result set that does not have a schema table
//   */
//  public final void TestSelect_NonQuery_CompleteResults() {
//    TestSelectNonQuery("if (0 = 1) select 1 ", MultiShardExecutionPolicy.CompleteResults);
//  }
//
//
//  /**
//   * Check that we can return a completely empty result set as expected.
//   */
//  public final void TestSelect_NonQuery_PartialResults() {
//    TestSelectNonQuery("if (0 = 1) select 1", MultiShardExecutionPolicy.PartialResults);
//  }
//
//  public final void TestSelectNonQuery(String commandText, MultiShardExecutionPolicy policy) {
//    try (MultiShardConnection conn = new MultiShardConnection(_shardMap.GetShards(),
//        MultiShardTestUtils.ShardConnectionString)) {
//      try (MultiShardCommand cmd = conn.CreateCommand()) {
//        cmd.CommandText = commandText;
//        cmd.ExecutionPolicy = policy;
//
//        try (MultiShardDataReader sdr = cmd.ExecuteReader()) {
//          assert 0 == sdr.MultiShardExceptions.size();
//
//          // TODO: This is a weird error message, but it's good enough for now
//          // Fixing this will require significant refactoring of MultiShardDataReader,
//          // we should fix it when we finish implementing async adding of child readers
//          AssertExtensions.<MultiShardDataReaderClosedException>AssertThrows(() -> sdr.Read());
//        }
//      }
//    }
//  }
//
//  /**
//   * Check that ExecuteReader throws when all shards have an exception
//   */
//  public final void TestSelect_Failure_PartialResults() {
//    MultiShardAggregateException e = TestSelectFailure("raiserror('blah', 16, 0)",
//        MultiShardExecutionPolicy.PartialResults);
//
//    // All children should have failed
//    assert _shardMap.GetShards().Count() == e.InnerExceptions.size();
//  }
//
//  /**
//   * Check that ExecuteReader throws when all shards have an exception
//   */
//  public final void TestSelect_Failure_CompleteResults() {
//    MultiShardAggregateException e = TestSelectFailure("raiserror('blah', 16, 0)",
//        MultiShardExecutionPolicy.CompleteResults);
//
//    // We don't know exactly how many child exceptions will happen, because the
//    // first exception that is seen will cause the children to be canceled.
//    AssertExtensions.AssertGreaterThanOrEqualTo(1, e.InnerExceptions.size());
//    AssertExtensions
//        .AssertLessThanOrEqualTo(_shardMap.GetShards().Count(), e.InnerExceptions.size());
//  }
//
//  public final MultiShardAggregateException TestSelectFailure(String commandText,
//      MultiShardExecutionPolicy policy) {
//    try (MultiShardConnection conn = new MultiShardConnection(_shardMap.GetShards(),
//        MultiShardTestUtils.ShardConnectionString)) {
//      try (MultiShardCommand cmd = conn.CreateCommand()) {
//        cmd.CommandText = commandText;
//        cmd.ExecutionPolicy = policy;
//
//        // ExecuteReader should fail
//        MultiShardAggregateException aggregateException = AssertExtensions.<MultiShardAggregateException>AssertThrows(
//            () -> cmd.ExecuteReader());
//
//        // Sanity check the exceptions are the correct type
//        for (RuntimeException e : aggregateException.InnerExceptions) {
//          Assert.IsInstanceOfType(e, MultiShardException.class);
//          Assert.IsInstanceOfType(e.getCause(), SqlException.class);
//        }
//
//        // Return the exception so that the caller can do additional validation
//        return aggregateException;
//      }
//    }
//  }
//
//  /**
//   * Check that we can return a partially succeeded reader when PartialResults policy is on
//   */
//  public final void TestSelect_PartialFailure_PartialResults() {
//    try (MultiShardConnection conn = new MultiShardConnection(_shardMap.GetShards(),
//        MultiShardTestUtils.ShardConnectionString)) {
//      try (MultiShardCommand cmd = conn.CreateCommand()) {
//        cmd.CommandText = GetPartialFailureQuery();
//        cmd.ExecutionPolicy = MultiShardExecutionPolicy.PartialResults;
//
//        try (MultiShardDataReader sdr = cmd.ExecuteReader()) {
//          // Exactly one should have failed
//          assert 1 == sdr.MultiShardExceptions.size();
//
//          // We should be able to read
//          while (sdr.Read()) {
//          }
//        }
//      }
//    }
//  }
//
//  /**
//   * Check that we fail a partially successful command when CompleteResults policy is on
//   */
//  public final void TestSelect_PartialFailure_CompleteResults() {
//    String query = GetPartialFailureQuery();
//    MultiShardAggregateException e = TestSelectFailure(query,
//        MultiShardExecutionPolicy.CompleteResults);
//
//    // Exactly one should have failed
//    assert 1 == e.InnerExceptions.size();
//  }
//
//  /**
//   * Gets a command that fails on one shard, but succeeds on others
//   */
//  private String GetPartialFailureQuery() {
//    List<ShardLocation> shardLocations = _shardMap.GetShards().Select(s -> s.Location);
//
//    // Pick an arbitrary one of those shards
//    ShardLocation chosenShardLocation = shardLocations.get(0);
//
//    // This query assumes that the chosen shard location's db name is distinct from all others
//    // In other words, only one shard location should have a database equal to the chosen location
//    assert 1 == shardLocations.size() (l -> l.Database.equals(chosenShardLocation.Database));
//
//    // We also assume that there is more than one shard
//    AssertExtensions.AssertGreaterThan(1, shardLocations.size());
//
//    // The command will fail only on the chosen shard
//    return String.format("if db_name() = '%1$s' raiserror('blah', 16, 0) else select 1",
//        shardLocations.get(0).Database);
//  }
//
//  /**
//   * Basic test for async api(s)
//   * Also demonstrates the async pattern of this library
//   * The Sync api is implicitly tested in MultiShardResultSetTests::TestSimpleSelect
//   */
//  public final void TestQueryShardsAsync() {
//    // Create new sharded connection so we can test the OpenAsync call as well.
//    //
//    try (MultiShardConnection conn = new MultiShardConnection(_shardMap.GetShards(),
//        MultiShardTestUtils.ShardConnectionString)) {
//      try (MultiShardCommand cmd = conn.CreateCommand()) {
//        cmd.CommandText = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable";
//        cmd.CommandType = CommandType.Text;
//
//        try (MultiShardDataReader sdr = ExecAsync(conn, cmd).Result) {
//          int recordsRetrieved = 0;
//          while (sdr.Read()) {
//            recordsRetrieved++;
////TODO TASK: There is no equivalent to implicit typing in Java:
//            var dbNameField = sdr.GetString(0);
////TODO TASK: There is no equivalent to implicit typing in Java:
//            var testIntField = sdr.<Integer>GetFieldValue(1);
////TODO TASK: There is no equivalent to implicit typing in Java:
//            var testBigIntField = sdr.<Long>GetFieldValue(2);
//            Logger.Log(
//                "RecordRetrieved: dbNameField: {0}, TestIntField: {1}, TestBigIntField: {2}, RecordCount: {3}",
//                dbNameField, testIntField, testBigIntField, recordsRetrieved);
//          }
//
//          assert recordsRetrieved == 9;
//        }
//      }
//    }
//  }
//
//  /**
//   * Basic test for ensuring that we include/don't include the $ShardName pseudo column as desired.
//   */
//  public final void TestShardNamePseudoColumnOption() {
//    boolean[] pseudoColumnOptions = new boolean[2];
//    pseudoColumnOptions[0] = true;
//    pseudoColumnOptions[1] = false;
//
//    // do the loop over the options.
//    // add the excpetion handling when referencing the pseudo column
//    //
//    for (boolean pseudoColumnPresent : pseudoColumnOptions) {
//      try (MultiShardConnection conn = new MultiShardConnection(_shardMap.GetShards(),
//          MultiShardTestUtils.ShardConnectionString)) {
//        try (MultiShardCommand cmd = conn.CreateCommand()) {
//          cmd.CommandText = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable";
//          cmd.CommandType = CommandType.Text;
//
//          cmd.ExecutionPolicy = MultiShardExecutionPolicy.CompleteResults;
//          cmd.ExecutionOptions =
//              pseudoColumnPresent ? MultiShardExecutionOptions.IncludeShardNameColumn
//                  : MultiShardExecutionOptions.None;
//          try (MultiShardDataReader sdr = cmd.ExecuteReader(CommandBehavior.Default)) {
//            assert 0 == sdr.MultiShardExceptions.size();
//
//            int recordsRetrieved = 0;
//
//            int expectedFieldCount = pseudoColumnPresent ? 4 : 3;
//            int expectedVisibleFieldCount = pseudoColumnPresent ? 4 : 3;
//            assert expectedFieldCount == sdr.FieldCount;
//            assert expectedVisibleFieldCount == sdr.VisibleFieldCount;
//
//            while (sdr.Read()) {
//              recordsRetrieved++;
////TODO TASK: There is no equivalent to implicit typing in Java:
//              var dbNameField = sdr.GetString(0);
////TODO TASK: There is no equivalent to implicit typing in Java:
//              var testIntField = sdr.<Integer>GetFieldValue(1);
////TODO TASK: There is no equivalent to implicit typing in Java:
//              var testBigIntField = sdr.<Long>GetFieldValue(2);
//
//              try {
//                String shardIdPseudoColumn = sdr.<String>GetFieldValue(3);
//                if (!pseudoColumnPresent) {
//                  Assert.Fail("Should not have been able to pull the pseudo column.");
//                }
//              } catch (IndexOutOfBoundsException e) {
//                if (pseudoColumnPresent) {
//                  Assert.Fail("Should not have encountered an exception.");
//                }
//              }
//            }
//
//            assert recordsRetrieved == 9;
//          }
//        }
//      }
//    }
//  }
//
//  /**
//   * Basic test for ensuring that we don't fail due to a schema mismatch on the shards.
//   */
//  public final void TestSchemaMismatchErrorPropagation() {
//    // First we need to alter the schema on one of the shards - we'll choose the last one.
//    //
//    String origColName = "Test_bigint_Field";
//    String newColName = "ModifiedName";
//
//    MultiShardTestUtils.ChangeColumnNameOnShardedTable(2, origColName, newColName);
//
//    // Then create new sharded connection so we can test the error handling logic.
//    // We'll wrap this all in a try-catch-finally block so that we can change the schema back
//    // to what the other tests will expect it to be in the finally.
//    //
//    try {
//      try (MultiShardConnection conn = new MultiShardConnection(_shardMap.GetShards(),
//          MultiShardTestUtils.ShardConnectionString)) {
//        try (MultiShardCommand cmd = conn.CreateCommand()) {
//          // Need to do a SELECT * in order to get the column name error as a schema mismatcherror.  If we name it explicitly
//          // we will get a command execution error instead.
//          //
//          cmd.CommandText = "SELECT * FROM ConsistentShardedTable";
//          cmd.CommandType = CommandType.Text;
//
//          try (MultiShardDataReader sdr = ExecAsync(conn, cmd).Result) {
//            // The number of errors we have depends on which shard executed first.
//            // So, we know it should be 1 OR 2.
//            //
//            Assert.IsTrue(
//                ((sdr.MultiShardExceptions.size() == 1) || (sdr.MultiShardExceptions.size() == 2)),
//                String.format("Expected 1 or 2 execution erros, but saw %1$s",
//                    sdr.MultiShardExceptions.size()));
//
//            int recordsRetrieved = 0;
//            while (sdr.Read()) {
//              recordsRetrieved++;
////TODO TASK: There is no equivalent to implicit typing in Java:
//              var dbNameField = sdr.GetString(0);
//            }
//
//            // We should see 9 records less 3 for each one that had a schema error.
//            int expectedRecords = ((9 - (3 * sdr.MultiShardExceptions.size())));
//
//            assert recordsRetrieved == expectedRecords;
//          }
//        }
//      }
//    } finally {
//      MultiShardTestUtils.ChangeColumnNameOnShardedTable(2, newColName, origColName);
//    }
//  }
//
//  //TODO TASK: There is no equivalent in Java to the 'async' keyword:
////ORIGINAL LINE: private async Task<MultiShardDataReader> ExecAsync(MultiShardConnection conn, MultiShardCommand cmd)
//  private Task<MultiShardDataReader> ExecAsync(MultiShardConnection conn, MultiShardCommand cmd) {
//    cmd.ExecutionPolicy = MultiShardExecutionPolicy.PartialResults;
//    cmd.ExecutionOptions = MultiShardExecutionOptions.IncludeShardNameColumn;
//
////TODO TASK: There is no equivalent to 'await' in Java:
//    return await cmd.ExecuteReaderAsync(CommandBehavior.Default, CancellationToken.None);
//  }
//
//  /**
//   * Try connecting to a non-existant shard
//   * Verify exception is propagated to the user
//   */
//  public final void TestQueryShardsInvalidConnectionSync() {
//    ShardLocation badShard = new ShardLocation("badLocation", "badDatabase");
//    SqlConnectionStringBuilder bldr = new SqlConnectionStringBuilder();
//    bldr.DataSource = badShard.DataSource;
//    bldr.InitialCatalog = badShard.Database;
//    SqlConnection badConn = new SqlConnection(bldr.ConnectionString);
//    try {
//      try (MultiShardConnection conn = new MultiShardConnection(_shardMap.GetShards(),
//          MultiShardTestUtils.ShardConnectionString)) {
//        conn.ShardConnections
//            .Add(new Tuple<ShardLocation, System.Data.Common.DbConnection>(badShard, badConn));
//        try (var cmd = conn.CreateCommand()) {
//          cmd.CommandText = "select 1";
//          cmd.ExecuteReader();
//        }
//      }
//    } catch (RuntimeException ex) {
//      if (ex instanceof MultiShardAggregateException) {
//        MultiShardAggregateException maex = (MultiShardAggregateException) ex;
//        Logger.Log("Exception encountered: " + maex.toString());
//        throw ((MultiShardException) (maex.getCause())).getCause();
//      }
//      throw ex;
//    }
//  }
//
//  /**
//   * Tests passing a tvp as a param
//   * using a datatable
//   */
//  public final void TestQueryShardsTvpParam() {
//    try {
//      // Install schema
//      String createTbl = "" + "\r\n" +
//          "                CREATE TABLE dbo.PageView" + "\r\n" +
//          "(" + "\r\n" +
//          "    PageViewID BIGINT NOT NULL," + "\r\n" +
//          "    PageViewCount BIGINT NOT NULL" + "\r\n" +
//          ");" + "\r\n" +
//          "CREATE TYPE dbo.PageViewTableType AS TABLE" + "\r\n" +
//          "(" + "\r\n" +
//          "    PageViewID BIGINT NOT NULL" + "\r\n" +
//          ");";
//      String createProc = "CREATE PROCEDURE dbo.procMergePageView" + "\r\n" +
//          "    @Display dbo.PageViewTableType READONLY" + "\r\n" +
//          "AS" + "\r\n" +
//          "BEGIN" + "\r\n" +
//          "    MERGE INTO dbo.PageView AS T" + "\r\n" +
//          "    USING @Display AS S" + "\r\n" +
//          "    ON T.PageViewID = S.PageViewID" + "\r\n" +
//          "    WHEN MATCHED THEN UPDATE SET T.PageViewCount = T.PageViewCount + 1" + "\r\n" +
//          "    WHEN NOT MATCHED THEN INSERT VALUES(S.PageViewID, 1);" + "\r\n" +
//          "END";
//      try (var cmd = _shardConnection.CreateCommand()) {
//        cmd.CommandText = createTbl;
//        cmd.ExecutionPolicy = MultiShardExecutionPolicy.PartialResults;
//        cmd.ExecuteReader();
//
//        cmd.CommandText = createProc;
//        cmd.ExecuteNonQueryAsync(CancellationToken.None, MultiShardExecutionPolicy.PartialResults)
//            .Wait();
//      }
//
//      Logger.Log("Schema installed..");
//
//      // Create the data table
//      DataTable table = new DataTable();
//      table.Columns.Add("PageViewID", Long.class);
//      int idCount = 3;
//      for (int i = 0; i < idCount; i++) {
//        table.Rows.Add(i);
//      }
//
//      // Execute the command
//      try (var cmd = _shardConnection.CreateCommand()) {
//        cmd.CommandType = CommandType.StoredProcedure;
//        cmd.CommandText = "dbo.procMergePageView";
//
//        SqlParameter param = new SqlParameter("@Display", table);
//        param.TypeName = "dbo.PageViewTableType";
//        param.SqlDbType = SqlDbType.Structured;
//        cmd.Parameters.Add(param);
//
//        cmd.ExecuteNonQueryAsync(CancellationToken.None, MultiShardExecutionPolicy.PartialResults)
//            .Wait();
//        cmd.ExecuteNonQueryAsync(CancellationToken.None, MultiShardExecutionPolicy.PartialResults)
//            .Wait();
//      }
//
//      Logger.Log("Command executed..");
//
//      try (var cmd = _shardConnection.CreateCommand()) {
//        // Validate that the pageviewcount was updated
//        cmd.CommandText = "select PageViewCount from PageView";
//        cmd.CommandType = CommandType.Text;
//        cmd.ExecutionPolicy = MultiShardExecutionPolicy.PartialResults;
//        cmd.ExecutionOptions = MultiShardExecutionOptions.IncludeShardNameColumn;
//        try (var sdr = cmd.ExecuteReader(CommandBehavior.Default)) {
//          while (sdr.Read()) {
//            long pageCount = (long) sdr["PageViewCount"];
//            Logger.Log("Page view count: {0} obtained from shard: {1}", pageCount,
//                sdr.<String>GetFieldValue(1));
//            assert 2 == pageCount;
//          }
//        }
//      }
//    } catch (RuntimeException ex) {
//      if (ex instanceof AggregateException) {
//        AggregateException aex = (AggregateException) ex;
//        Logger.Log("Exception encountered: {0}", aex.getCause().toString());
//      } else {
//        Logger.Log(ex.getMessage());
//      }
//      throw ex;
//    } finally {
//      String dropSchema =
//          "if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[procMergePageView]') and objectproperty(id, N'IsProcedure') = 1)"
//              + "\r\n" +
//              "begin" + "\r\n" +
//              "drop procedure dbo.procMergePageView" + "\r\n" +
//              "end" + "\r\n" +
//              "if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[Pageview]'))"
//              + "\r\n" +
//              "begin" + "\r\n" +
//              "drop table dbo.Pageview" + "\r\n" +
//              "end" + "\r\n" +
//              "if exists (select * from sys.types where name = 'PageViewTableType')" + "\r\n" +
//              "begin" + "\r\n" +
//              "drop type dbo.PageViewTableType" + "\r\n" +
//              "end";
//      try (var cmd = _shardConnection.CreateCommand()) {
//        cmd.CommandText = dropSchema;
//        cmd.ExecuteNonQueryAsync(CancellationToken.None, MultiShardExecutionPolicy.PartialResults)
//            .Wait();
//      }
//    }
//  }
//
//  /**
//   * Verifies that the command cancellation events are fired
//   * upon cancellation of a command that is in progress
//   */
//  public final void TestQueryShardsCommandCancellationHandler() {
//    ArrayList<ShardLocation> cancelledShards = new ArrayList<ShardLocation>();
//    CancellationTokenSource cts = new CancellationTokenSource();
//
//    try (MultiShardCommand cmd = _shardConnection.CreateCommand()) {
//      Barrier barrier = new Barrier(cmd.Connection.Shards.Count() + 1);
//
//      // If the threads don't meet the barrier by this time, then give up and fail the test
//      TimeSpan barrierTimeout = TimeSpan.FromSeconds(10);
//
//      cmd.CommandText = "WAITFOR DELAY '00:01:00'";
//      cmd.CommandTimeoutPerShard = 12;
//
//      cmd.ShardExecutionCanceled += (obj, args) -> {
//        cancelledShards.add(args.ShardLocation);
//      };
//
//      cmd.ShardExecutionBegan += (obj, args) -> {
//        // If ShardExecutionBegan were only signaled by one thread,
//        // then this would hang forever.
//        barrier.SignalAndWait(barrierTimeout);
//      };
//
//      Task cmdTask = cmd.ExecuteReaderAsync(cts.Token);
//
//      boolean syncronized = barrier.SignalAndWait(barrierTimeout);
//      assert syncronized;
//
//      // Cancel the command once execution begins
//      // Sleeps are bad but this is just to really make sure
//      // sqlclient has had a chance to begin command execution
//      // Will not effect the test outcome
//      Thread.sleep(TimeSpan.FromSeconds(1));
//      cts.Cancel();
//
//      // Validate that the task was cancelled
//      AssertExtensions.<TaskCanceledException>WaitAndAssertThrows(cmdTask);
//
//      // Validate that the cancellation event was fired for all shards
//      ArrayList<ShardLocation> allShards = _shardConnection.ShardConnections.Select(l -> l.Item1)
//          .ToList();
//      CollectionAssert.AreEquivalent(allShards, cancelledShards,
//          "Expected command canceled event to be fired for all shards!");
//    }
//  }
//
//  /**
//   * Close the connection to one of the shards behind MultiShardConnection's back. Verify that we
//   * reopen the connection with the built-in retry policy
//   */
//  public final void TestQueryShardsInvalidShardStateSync() {
//    // Get a shard and close it's connection
////TODO TASK: There is no equivalent to implicit typing in Java:
//    var shardSqlConnections = _shardConnection.ShardConnections;
//    shardSqlConnections[1].Item2.Close();
//
//    try {
//      // Execute
//      try (MultiShardCommand cmd = _shardConnection.CreateCommand()) {
//        cmd.CommandText = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable";
//        cmd.CommandType = CommandType.Text;
//
//        try (MultiShardDataReader sdr = cmd.ExecuteReader()) {
//        }
//      }
//    } catch (RuntimeException ex) {
//      if (ex instanceof AggregateException) {
//        AggregateException aex = (AggregateException) ex;
//        Logger.Log("Exception encountered: " + ex.getMessage());
//        throw aex.InnerExceptions.FirstOrDefault((e) -> e instanceof IllegalStateException);
//      }
//      throw ex;
//    }
//  }
//
//  /**
//   * Validate the MultiShardConnectionString's connectionString param.
//   * - Shouldn't be null
//   * - No DataSource/InitialCatalog should be set
//   * - ApplicationName should be enhanced with a MSQ library
//   * specific suffix and should be capped at 128 chars
//   */
//  public final void TestInvalidMultiShardConnectionString() {
//    MultiShardConnection conn;
//
//    try {
//      conn = new MultiShardConnection(_shardMap.GetShards(), connectionString:null);
//    } catch (RuntimeException ex) {
//      Assert.IsTrue(ex instanceof IllegalArgumentException, "Expected ArgumentNullException!");
//    }
//
//    try {
//      conn = new MultiShardConnection(_shardMap.GetShards(),
//          MultiShardTestUtils.ShardMapManagerConnectionString);
//    } catch (RuntimeException ex) {
//      Assert.IsTrue(ex instanceof IllegalArgumentException, "Expected ArgumentException!");
//    }
//
//    // Validate that the ApplicationName is updated properly
//    StringBuilder applicationStringBldr = new StringBuilder();
//    for (int i = 0; i < ApplicationNameHelper.MaxApplicationNameLength; i++) {
//      applicationStringBldr.append('x');
//    }
//    String applicationName = applicationStringBldr.toString();
//    SqlConnectionStringBuilder connStringBldr = new SqlConnectionStringBuilder(
//        MultiShardTestUtils.ShardConnectionString);
//    connStringBldr.ApplicationName = applicationName;
//    conn = new MultiShardConnection(_shardMap.GetShards(), connStringBldr.ConnectionString);
//
//    String updatedApplicationName = (new SqlConnectionStringBuilder(
//        conn.ShardConnections[0].Item2.ConnectionString)).ApplicationName;
//    Assert.IsTrue(updatedApplicationName.length() == ApplicationNameHelper.MaxApplicationNameLength
//            && updatedApplicationName.endsWith(MultiShardConnection.ApplicationNameSuffix),
//        "ApplicationName not appended with {0}!", MultiShardConnection.ApplicationNameSuffix);
//  }
//
//  public final void TestCreateConnectionWithNoShards() {
//    try (MultiShardConnection conn = new MultiShardConnection(new Shard[]{}, "")) {
//      Assert.Fail("Should have failed in the MultiShardConnection c-tor.");
//    }
//  }
//
//  /**
//   * Regression test for VSTS Bug# 3936154
//   * - Execute a command that will result in a failure in a loop
//   * - Without the fix (disabling the command behavior)s, the test will hang and timeout.
//   */
//  public final void TestFailedCommandWithConnectionCloseCmdBehavior() {
//    Parallel.For(0, 100, i -> {
//      try {
//        try (MultiShardConnection conn = new MultiShardConnection(_shardMap.GetShards(),
//            MultiShardTestUtils.ShardConnectionString)) {
//          try (MultiShardCommand cmd = conn.CreateCommand()) {
//            cmd.CommandText = "select * from table_does_not_exist";
//            cmd.CommandType = CommandType.Text;
//
//            try (MultiShardDataReader sdr = cmd.ExecuteReader()) {
//              while (sdr.Read()) {
//              }
//            }
//          }
//        }
//      } catch (RuntimeException ex) {
//        System.out
//            .printf("Encountered exception: %1$s in iteration: %2$s" + "\r\n", ex.toString(), i);
//      } finally {
//        System.out.printf("Completed execution of iteration: %1$s" + "\r\n", i);
//      }
//    });
//  }
//
//  /**
//   * This test induces failures via a ProxyServer in order to validate that:
//   * a) we are handling reader failures as expected, and
//   * b) we get all-or-nothing semantics on our reads from a single row
//   */
//  public final void TestShardResultFailures() {
//    ProxyServer proxyServer = GetProxyServer();
//
//    try {
//      // Start up the proxy server.  Do it in a try so we can shut it down in the finally.
//      // Also, we have to generate the proxyShardconnections *AFTER* we start up the server
//      // so that we know what port the proxy is listening on.  More on the placement
//      // of the connection generation below.
//      //
//      proxyServer.Start();
//
//      // PreKillReads is the number of successful reads to perform before killing
//      // all the connections.  We start at 0 to test the no failure case as well.
//      //
//      for (int preKillReads = 0; preKillReads <= 10; preKillReads++) {
//        // Additionally, since we are running inside a loop, we need to regenerate the proxy shard connections each time
//        // so that we don't re-use dead connections.  If we do that we will end up hung in the read call.
//        //
//        ArrayList<Tuple<ShardLocation, DbConnection>> proxyShardConnections = GetProxyShardConnections(
//            proxyServer);
//        try (MultiShardConnection conn = new MultiShardConnection(proxyShardConnections)) {
//          try (MultiShardCommand cmd = conn.CreateCommand()) {
//            cmd.CommandText = "SELECT db_name() as dbName1, REPLICATE(db_name(), 1000) as longExpr, db_name() as dbName2 FROM ConsistentShardedTable";
//            cmd.CommandType = CommandType.Text;
//
//            cmd.ExecutionPolicy = MultiShardExecutionPolicy.PartialResults;
//            cmd.ExecutionOptions = MultiShardExecutionOptions.IncludeShardNameColumn;
//
//            try (MultiShardDataReader sdr = cmd.ExecuteReader(CommandBehavior.Default)) {
//              int tuplesRead = 0;
//
//              while (sdr.Read()) {
//                // Read part of the tuple first before killing the connections and
//                // then attempting to read the rest of the tuple.
//                //
//                tuplesRead++;
//
//                try {
//                  // The longExpr should contain the first dbName field multiple times.
//                  //
//                  String dbName1 = sdr.GetString(0);
//                  String longExpr = sdr.GetString(1);
//                  assert longExpr.contains(dbName1);
//
//                  if (tuplesRead == preKillReads) {
//                    proxyServer.KillAllConnections();
//                  }
//
//                  // The second dbName field should be the same as the first dbName field.
//                  //
//                  String dbName2 = sdr.GetString(2);
//                  assert dbName1 == dbName2;
//
//                  // The shardId should contain both the first and the second dbName fields.
//                  //
//                  String shardId = sdr.GetString(3);
//                  assert shardId.contains(dbName1);
//                  assert shardId.contains(dbName2);
//                } catch (RuntimeException ex) {
//                  // We've seen some failures here due to an attempt to access a socket after it has
//                  // been disposed.  The only place where we are attempting to access the socket
//                  // is in the call to proxyServer.KillAllConnections.  Unfortunately, it's not clear
//                  // what is causing that problem since it only appears to repro in the lab.
//                  // I (errobins) would rather not blindly start changing things in the code (either
//                  // our code above, our exception handling code here, or the proxyServer code) until
//                  // we know which socket we are trying to access when we hit this problem.
//                  // So, the first step I will take is to pull additional exception information
//                  // so that we can see some more information about what went wrong the next time it repros.
//                  //
//                  Assert.Fail(
//                      "Unexpected exception, rethrowing.  Here is some info: \n Message: {0} \n Source: {1} \n StackTrace: {2}",
//                      ex.getMessage(), ex.Source, ex.StackTrace);
//                  throw ex;
//                }
//              }
//
//              Assert.IsTrue((tuplesRead <= preKillReads) || (0 == preKillReads), String
//                  .format("Tuples read was %1$s, Pre-kill reads was %2$s", tuplesRead,
//                      preKillReads));
//            }
//          }
//        }
//      }
//    } finally {
//      // Be sure to shut down the proxy server.
//      //
//      String proxyLog = proxyServer.EventLog.toString();
//      Logger.Log(proxyLog);
//      proxyServer.Stop();
//    }
//  }
//
//  /**
//   * Helper that sets up a proxy server for us and points it at our local host, 1433 SQL Server.
//   *
//   * @return The newly created proxy server for our local sql server host.
//   *
//   *
//   * Note that we are not inducing any network delay (the first arg).  We coul dchange this if
//   * desired.
//   */
//  private ProxyServer GetProxyServer() {
//    ProxyServer proxy = new ProxyServer(simulatedPacketDelay:0, simulatedInDelay:
//    true, simulatedOutDelay:true, bufferSize:8192);
//    proxy.RemoteEndpoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 1433);
//
//    return proxy;
//  }
//
//  /**
//   * Helper that provides us with ShardConnections based on the shard map (for the database), but
//   * routed through the proxy.
//   *
//   * @param proxy The proxy to route the connections through.
//   * @return The List of {ShardLocation, DbConnection} tuples that we can use to instantiate our
//   * multi-shard connection.
//   *
//   *
//   * Since our shards all reside in the local instance we can just point them at a single proxy
//   * server.  If we were using actual physically distributed shards, then I think we would need a
//   * separate proxy for each shard.  We could augment these tests to use a separate proxy per shard,
//   * if we wanted, in order to be able to simulate a richer variety of failures.  For now, we just
//   * simulate total failures of all shards.
//   */
//  private ArrayList<Tuple<ShardLocation, DbConnection>> GetProxyShardConnections(
//      ProxyServer proxy) {
//    // We'll do this by looking at our pre-existing connections and working from that.
//    //
//    String baseConnString = MultiShardTestUtils.ShardConnectionString.toString();
//    ArrayList<Tuple<ShardLocation, DbConnection>> rVal = new ArrayList<Tuple<ShardLocation, DbConnection>>();
//    for (Shard shard : _shardMap.GetShards()) {
//      // Location doesn't really matter, so just use the same one.
//      //
//      ShardLocation curLoc = shard.Location;
//
//      // The connection, however, does matter, so set up a connection
//      SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(baseConnString);
//      builder.DataSource = MultiShardTestUtils.GetServerName() + "," + proxy.LocalPort;
//      builder.InitialCatalog = curLoc.Database;
//
//      SqlConnection curConn = new SqlConnection(builder.toString());
//
//      Tuple<ShardLocation, DbConnection> curTuple = new Tuple<ShardLocation, DbConnection>(curLoc,
//          curConn);
//      rVal.add(curTuple);
//    }
//    return rVal;
//  }
}
