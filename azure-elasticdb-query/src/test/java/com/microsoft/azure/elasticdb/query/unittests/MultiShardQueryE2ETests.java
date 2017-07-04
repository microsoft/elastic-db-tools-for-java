package com.microsoft.azure.elasticdb.query.unittests;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.helpers.ApplicationNameHelper;
import com.microsoft.azure.elasticdb.query.category.ExcludeFromGatedCheckin;
import com.microsoft.azure.elasticdb.query.exception.MultiShardAggregateException;
import com.microsoft.azure.elasticdb.query.exception.MultiShardDataReaderClosedException;
import com.microsoft.azure.elasticdb.query.exception.MultiShardException;
import com.microsoft.azure.elasticdb.query.logging.CommandBehavior;
import com.microsoft.azure.elasticdb.query.logging.MultiShardExecutionOptions;
import com.microsoft.azure.elasticdb.query.logging.MultiShardExecutionPolicy;
import com.microsoft.azure.elasticdb.query.multishard.MultiShardConnection;
import com.microsoft.azure.elasticdb.query.multishard.MultiShardResultSet;
import com.microsoft.azure.elasticdb.query.multishard.MultiShardStatement;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlConnectionStringBuilder;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for end to end scenarios where a user connects to his shards, executes commands against
 * them and receives results Purpose: Basic End-To-End test scenarios for the cross shard query
 * client library
 * Notes: Tests currently assume there's a running sqlservr instance. Everything will
 * be automated once we integrate with the larger framework. Currently the tests use the same
 * methods to create shards as MultiShardResultSetTests
 */
public class MultiShardQueryE2ETests {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Handle on connections to all shards
   */
  private MultiShardConnection shardConnection;

  /**
   * Handle to the ShardMap with our Test databases.
   */
  private ShardMap shardMap;

  @BeforeClass
  public static void myClassInitialize() throws SQLException {
    // Drop and recreate the test databases, tables, and data that we will use to verify the
    // functionality. For now I have hardcoded the database names. A better approach would be to
    // make the database names be guids. Not the top priority right now, though.
    MultiShardTestUtils.dropAndCreateDatabases();
    MultiShardTestUtils.createAndPopulateTables();
  }

  /**
   * Blow away our three test databases that we drove the tests off of.
   * Doing this so that we don't leave objects littered around.
   */
  @AfterClass
  public static void myClassCleanup() throws SQLException {
    // We need to clear the connection pools so that we don't get a database still in use error
    // resulting from our attenpt to drop the databases below.
    MultiShardTestUtils.dropDatabases();
  }

  /**
   * Open up a clean connection to each test database prior to each test.
   */
  @Before
  public final void myTestInitialize() {
    shardMap = MultiShardTestUtils.createAndGetTestShardMap();
    List<Shard> shards = shardMap.getShards();

    // Create the multi-shard connection
    shardConnection = new MultiShardConnection(MultiShardTestUtils.MULTI_SHARD_CONN_STRING,
        shards.toArray(new Shard[shards.size()]));
  }

  /**
   * Close our connections to each test database after each test.
   */
  @After
  public final void myTestCleanup() throws IOException {
    // Close connections after each test.
    shardConnection.close();
  }

  /**
   * Check that we can iterate through 3 result sets as expected.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testSimpleSelect_PartialResults() {
    testSimpleSelect(MultiShardExecutionPolicy.PartialResults);
  }

  /**
   * Check that we can iterate through 3 result sets as expected.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testSimpleSelect_CompleteResults() {
    testSimpleSelect(MultiShardExecutionPolicy.CompleteResults);
  }

  private void testSimpleSelect(MultiShardExecutionPolicy policy) {
    // What we're doing:
    // Grab all rows from each test database.
    // Load them into a MultiShardResultSet.
    // Iterate through the rows and make sure that we have 9 total.
    try (MultiShardStatement stmt = shardConnection.createCommand()) {
      stmt.setCommandText("SELECT dbNameField, Test_int_Field, Test_bigint_Field"
          + " FROM ConsistentShardedTable");
      stmt.setExecutionOptions(MultiShardExecutionOptions.IncludeShardNameColumn);
      stmt.setExecutionPolicy(policy);

      try (MultiShardResultSet sdr = stmt.executeQuery()) {
        assert 0 == sdr.getMultiShardExceptions().size();

        int recordsRetrieved = 0;
        log.info("Starting to get records");
        while (sdr.next()) {
          recordsRetrieved++;
          String dbNameField = sdr.getString(1);
          int testIntField = sdr.getInt(2);
          long testBigIntField = sdr.getLong(3);
          String shardIdPseudoColumn = sdr.getString(4);
          String logRecord = String.format("RecordRetrieved: dbNameField: %1$s, TestIntField:"
                  + " %2$s, TestBigIntField: %3$s, shardIdPseudoColumnField: %4$s, RecordCount:"
                  + " %5$s", dbNameField, testIntField, testBigIntField, shardIdPseudoColumn,
              recordsRetrieved);
          log.info(logRecord);
          System.out.println(logRecord);
        }

        sdr.close();

        assert recordsRetrieved == 9;
      }
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  /**
   * Check that we can return an empty result set that has a schema table
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testSelect_NoRows_CompleteResults() {
    testSelectNoRows("select 1 where 0 = 1", MultiShardExecutionPolicy.CompleteResults);
  }

  /**
   * Check that we can return an empty result set that has a schema table
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testSelect_NoRows_PartialResults() {
    testSelectNoRows("select 1 where 0 = 1", MultiShardExecutionPolicy.PartialResults);
  }

  private void testSelectNoRows(String commandText, MultiShardExecutionPolicy policy) {
    try (MultiShardStatement stmt = shardConnection.createCommand()) {
      stmt.setCommandText(commandText);
      stmt.setExecutionPolicy(policy);

      // Read first
      try (MultiShardResultSet sdr = stmt.executeQuery()) {
        assert 0 == sdr.getMultiShardExceptions().size();
        while (sdr.next()) {
          Assert.fail("Should not have gotten any records.");
        }
        assert sdr.getRowCount() == 0;
      }

      // HasRows first
      try (MultiShardResultSet sdr = stmt.executeQuery()) {
        assert 0 == sdr.getMultiShardExceptions().size();
        assert sdr.getRowCount() == 0;
        while (sdr.next()) {
          Assert.fail("Should not have gotten any records.");
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  /**
   * Check that we can return an empty result set that does not have a schema table
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testSelect_NonQuery_CompleteResults() {
    testSelectNonQuery("if (0 = 1) select 1 ", MultiShardExecutionPolicy.CompleteResults);
  }

  /**
   * Check that we can return a completely empty result set as expected.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testSelect_NonQuery_PartialResults() {
    testSelectNonQuery("if (0 = 1) select 1", MultiShardExecutionPolicy.PartialResults);
  }

  private void testSelectNonQuery(String commandText, MultiShardExecutionPolicy policy) {
    try (MultiShardStatement stmt = shardConnection.createCommand()) {
      stmt.setCommandText(commandText);
      stmt.setExecutionPolicy(policy);

      try (MultiShardResultSet sdr = stmt.executeQuery()) {
        assert 0 == sdr.getMultiShardExceptions().size();

        // TODO: This is a weird error message, but it's good enough for now
        // Fixing this will require significant refactoring of MultiShardResultSet,
        // we should fix it when we finish implementing async adding of child readers
        MultiShardDataReaderClosedException ex = AssertExtensions.assertThrows(() -> {
          try {
            sdr.next();
          } catch (SQLException e) {
            e.printStackTrace();
          }
        });
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  /**
   * Check that ExecuteReader throws when all shards have an exception
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testSelect_Failure_PartialResults() {
    MultiShardAggregateException e = testSelectFailure("raiserror('blah', 16, 0)",
        MultiShardExecutionPolicy.PartialResults);

    // All children should have failed
    assert shardMap.getShards().size() == e.getInnerExceptions().size();
  }

  /**
   * Check that ExecuteReader throws when all shards have an exception
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testSelect_Failure_CompleteResults() {
    MultiShardAggregateException e = testSelectFailure("raiserror('blah', 16, 0)",
        MultiShardExecutionPolicy.CompleteResults);

    // We don't know exactly how many child exceptions will happen, because the
    // first exception that is seen will cause the children to be canceled.
    assert e.getInnerExceptions().size() >= 1;
    assert e.getInnerExceptions().size() <= shardMap.getShards().size();
  }

  private MultiShardAggregateException testSelectFailure(String commandText,
      MultiShardExecutionPolicy policy) {
    try (MultiShardStatement stmt = shardConnection.createCommand()) {
      stmt.setCommandText(commandText);
      stmt.setExecutionPolicy(policy);

      // ExecuteReader should fail
      MultiShardAggregateException aggregateException = AssertExtensions.assertThrows(
          stmt::executeQuery);

      // Sanity check the exceptions are the correct type
      assert aggregateException != null;
      for (Exception e : aggregateException.getInnerExceptions()) {
        assert e instanceof MultiShardException;
        assert e.getCause() instanceof SQLException;
      }

      // Return the exception so that the caller can do additional validation
      return aggregateException;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Check that we can return a partially succeeded reader when PartialResults policy is on
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testSelect_PartialFailure_PartialResults() {
    try (MultiShardStatement stmt = shardConnection.createCommand()) {
      stmt.setCommandText(getPartialFailureQuery());
      stmt.setExecutionPolicy(MultiShardExecutionPolicy.PartialResults);

      try (MultiShardResultSet sdr = stmt.executeQuery()) {
        // Exactly one should have failed
        assert 1 == sdr.getMultiShardExceptions().size();

        // We should be able to read
        assert !sdr.next();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Check that we fail a partially successful command when CompleteResults policy is on
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testSelect_PartialFailure_CompleteResults() {
    String query = getPartialFailureQuery();
    MultiShardAggregateException e = testSelectFailure(query,
        MultiShardExecutionPolicy.CompleteResults);

    // Exactly one should have failed
    assert 1 == e.getInnerExceptions().size();
  }

  /**
   * Gets a command that fails on one shard, but succeeds on others
   */
  private String getPartialFailureQuery() {
    List<ShardLocation> shardLocations = shardMap.getShards().stream().map(Shard::getLocation)
        .collect(Collectors.toList());

    // Pick an arbitrary one of those shards
    int index = new Random().nextInt(shardLocations.size());
    ShardLocation chosenShardLocation = shardLocations.get(index);

    // This query assumes that the chosen shard location's db name is distinct from all others
    // In other words, only one shard location should have a database equal to the chosen location
    assert 1 == shardLocations.size();
    assert 1 == shardLocations.stream()
        .filter(l -> l.getDatabase().equals(chosenShardLocation.getDatabase())).count();

    // We also assume that there is more than one shard
    assert shardLocations.size() > 1;

    // The command will fail only on the chosen shard
    return String.format("if db_name() = '%1$s' raiserror('blah', 16, 0) else select 1",
        shardLocations.get(0).getDatabase());
  }

  /**
   * Basic test for async api(s)
   * Also demonstrates the async pattern of this library
   * The Sync api is implicitly tested in MultiShardResultSetTests::TestSimpleSelect
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testQueryShardsAsync() {
    // Create new sharded connection so we can test the OpenAsync call as well.
    try (MultiShardStatement stmt = shardConnection.createCommand()) {
      stmt.setCommandText("SELECT dbNameField, Test_int_Field, Test_bigint_Field "
          + " FROM ConsistentShardedTable");

      try (MultiShardResultSet sdr = execAsync(stmt).call()) {
        int recordsRetrieved = 0;
        while (sdr.next()) {
          recordsRetrieved++;
          String dbNameField = sdr.getString(0);
          int testIntField = sdr.getInt(1);
          long testBigIntField = sdr.getLong(2);
          log.info("RecordRetrieved: dbNameField: {}, TestIntField: {}, TestBigIntField: {}, Count:"
              + " {}", dbNameField, testIntField, testBigIntField, recordsRetrieved);
        }

        assert recordsRetrieved == 9;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Basic test for ensuring that we include/don't include the $ShardName pseudo column as desired.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testShardNamePseudoColumnOption() {
    boolean[] pseudoColumnOptions = new boolean[2];
    pseudoColumnOptions[0] = true;
    pseudoColumnOptions[1] = false;

    // do the loop over the options.
    // add the excpetion handling when referencing the pseudo column
    List<Shard> shards = shardMap.getShards();
    for (boolean pseudoColumnPresent : pseudoColumnOptions) {
      try (MultiShardConnection conn = new MultiShardConnection(
          MultiShardTestUtils.MULTI_SHARD_TEST_CONN_STRING,
          shards.toArray(new Shard[shards.size()]))) {
        try (MultiShardStatement stmt = conn.createCommand()) {
          stmt.setCommandText(
              "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable");

          stmt.setExecutionPolicy(MultiShardExecutionPolicy.CompleteResults);
          stmt.setExecutionOptions(pseudoColumnPresent
              ? MultiShardExecutionOptions.IncludeShardNameColumn
              : MultiShardExecutionOptions.None);
          try (MultiShardResultSet sdr = stmt.executeQuery(CommandBehavior.Default)) {
            assert 0 == sdr.getMultiShardExceptions().size();

            int recordsRetrieved = 0;

            int expectedFieldCount = pseudoColumnPresent ? 4 : 3;
            assert expectedFieldCount == sdr.getMetaData().getColumnCount();

            while (sdr.next()) {
              recordsRetrieved++;
              String dbNameField = sdr.getString(0);
              int testIntField = sdr.getInt(1);
              long testBigIntField = sdr.getLong(2);

              try {
                String shardIdPseudoColumn = sdr.getString(3);
                if (!pseudoColumnPresent) {
                  Assert.fail("Should not have been able to pull the pseudo column.");
                }
              } catch (IndexOutOfBoundsException e) {
                if (pseudoColumnPresent) {
                  Assert.fail("Should not have encountered an exception.");
                }
              }
            }

            assert recordsRetrieved == 9;
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Basic test for ensuring that we don't fail due to a schema mismatch on the shards.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testSchemaMismatchErrorPropagation() {
    // First we need to alter the schema on one of the shards - we'll choose the last one.
    String origColName = "Test_bigint_Field";
    String newColName = "ModifiedName";

    // Then create new sharded connection so we can test the error handling logic.
    // We'll wrap this all in a try-catch-finally block so that we can change the schema back
    // to what the other tests will expect it to be in the finally.
    try {
      MultiShardTestUtils.changeColumnNameOnShardedTable(2, origColName, newColName);
      try (MultiShardStatement stmt = shardConnection.createCommand()) {
        // Need to do a SELECT * in order to get the column name error as a schema mismatcherror.
        // If we name it explicitly we will get a command execution error instead.
        stmt.setCommandText("SELECT * FROM ConsistentShardedTable");

        try (MultiShardResultSet sdr = execAsync(stmt).call()) {
          // The number of errors we have depends on which shard executed first.
          // So, we know it should be 1 OR 2.
          Assert.assertTrue(String.format("Expected 1 or 2 execution errors, but saw %1$s",
              sdr.getMultiShardExceptions().size()), sdr.getMultiShardExceptions().size() == 1
              || sdr.getMultiShardExceptions().size() == 2);

          int recordsRetrieved = 0;
          while (sdr.next()) {
            recordsRetrieved++;
            String dbNameField = sdr.getString(0);
          }

          // We should see 9 records less 3 for each one that had a schema error.
          int expectedRecords = ((9 - (3 * sdr.getMultiShardExceptions().size())));

          assert recordsRetrieved == expectedRecords;
        }
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
      try {
        MultiShardTestUtils.changeColumnNameOnShardedTable(2, newColName, origColName);
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  private Callable<MultiShardResultSet> execAsync(MultiShardStatement stmt) {
    stmt.setExecutionPolicy(MultiShardExecutionPolicy.PartialResults);
    stmt.setExecutionOptions(MultiShardExecutionOptions.IncludeShardNameColumn);

    return stmt.executeQueryAsync(CommandBehavior.Default);
  }

  /**
   * Try connecting to a non-existant shard
   * Verify exception is propagated to the user
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testQueryShardsInvalidConnectionSync() throws Throwable {
    ShardLocation badShard = new ShardLocation("badLocation", "badDatabase");
    SqlConnectionStringBuilder bldr = new SqlConnectionStringBuilder();
    bldr.setDataSource(badShard.getDataSource());
    bldr.setDatabaseName(badShard.getDatabase());
    try {
      Connection badConn = DriverManager.getConnection(bldr.getConnectionString());
      shardConnection.getShardConnections().add(new ImmutablePair<>(badShard, badConn));
      try (MultiShardStatement stmt = shardConnection.createCommand()) {
        stmt.setCommandText("select 1");
        stmt.executeQuery();
      }
    } catch (Exception ex) {
      if (ex instanceof MultiShardAggregateException) {
        MultiShardAggregateException maex = (MultiShardAggregateException) ex;
        log.info("Exception encountered: " + maex.toString());
        throw maex.getCause().getCause();
      }
      throw ex;
    }
  }

  /**
   * Tests passing a tvp as a param
   * using a datatable
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testQueryShardsTvpParam() throws Exception {
    try {
      // Install schema
      String createTbl = "" + "\r\n" +
          "                CREATE TABLE dbo.PageView" + "\r\n" +
          "(" + "\r\n" +
          "    PageViewID BIGINT NOT NULL," + "\r\n" +
          "    PageViewCount BIGINT NOT NULL" + "\r\n" +
          ");" + "\r\n" +
          "CREATE TYPE dbo.PageViewTableType AS TABLE" + "\r\n" +
          "(" + "\r\n" +
          "    PageViewID BIGINT NOT NULL" + "\r\n" +
          ");";
      String createProc = "CREATE PROCEDURE dbo.procMergePageView" + "\r\n" +
          "    @Display dbo.PageViewTableType READONLY" + "\r\n" +
          "AS" + "\r\n" +
          "BEGIN" + "\r\n" +
          "    MERGE INTO dbo.PageView AS T" + "\r\n" +
          "    USING @Display AS S" + "\r\n" +
          "    ON T.PageViewID = S.PageViewID" + "\r\n" +
          "    WHEN MATCHED THEN UPDATE SET T.PageViewCount = T.PageViewCount + 1" + "\r\n" +
          "    WHEN NOT MATCHED THEN INSERT VALUES(S.PageViewID, 1);" + "\r\n" +
          "END";
      try (MultiShardStatement stmt = shardConnection.createCommand()) {
        stmt.setCommandText(createTbl);
        stmt.setExecutionPolicy(MultiShardExecutionPolicy.PartialResults);
        stmt.executeQuery();

        stmt.setCommandText(createProc);
        stmt.executeQueryAsync().call();
      } catch (Exception e) {
        e.printStackTrace();
      }

      log.info("Schema installed..");

      /* TODO:
      // Create the data table
      DataTable table = new DataTable();
      table.Columns.Add("PageViewID", Long.class);
      int idCount = 3;
      for (int i = 0; i < idCount; i++) {
        table.Rows.Add(i);
      }

      // Execute the command
      try (Statement stmt = shardConnection.createCommand()) {
        stmt.CommandType = CommandType.StoredProcedure;
        stmt.setCommandText("dbo.procMergePageView");

        SqlParameter param = new SqlParameter("@Display", table);
        param.TypeName = "dbo.PageViewTableType";
        param.SqlDbType = SqlDbType.Structured;
        stmt.Parameters.Add(param);

        stmt.ExecuteNonQueryAsync(CancellationToken.None, MultiShardExecutionPolicy.PartialResults)
            .Wait();
        stmt.ExecuteNonQueryAsync(CancellationToken.None, MultiShardExecutionPolicy.PartialResults)
            .Wait();
      }*/

      log.info("Command executed..");

      try (MultiShardStatement stmt = shardConnection.createCommand()) {
        // Validate that the pageviewcount was updated
        stmt.setCommandText("select PageViewCount from PageView");
        stmt.setExecutionPolicy(MultiShardExecutionPolicy.PartialResults);
        stmt.setExecutionOptions(MultiShardExecutionOptions.IncludeShardNameColumn);
        try (MultiShardResultSet sdr = stmt.executeQuery(CommandBehavior.Default)) {
          while (sdr.next()) {
            long pageCount = sdr.getLong("PageViewCount");
            log.info("Page view count: {0} obtained from shard: {1}", pageCount, sdr.getString(1));
            assert 2 == pageCount;
          }
        }
      }
    } catch (Exception ex) {
      /* TODO:
      if (ex instanceof AggregateException) {
        AggregateException aex = (AggregateException) ex;
        log.info("Exception encountered: {0}", aex.getCause().toString());
      } else {
        log.info(ex.getMessage());
      }*/
      throw ex;
    } finally {
      String dropSchema =
          "if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[procMergePageView]') and objectproperty(id, N'IsProcedure') = 1)"
              + "\r\n" +
              "begin" + "\r\n" +
              "drop procedure dbo.procMergePageView" + "\r\n" +
              "end" + "\r\n" +
              "if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[Pageview]'))"
              + "\r\n" +
              "begin" + "\r\n" +
              "drop table dbo.Pageview" + "\r\n" +
              "end" + "\r\n" +
              "if exists (select * from sys.types where name = 'PageViewTableType')" + "\r\n" +
              "begin" + "\r\n" +
              "drop type dbo.PageViewTableType" + "\r\n" +
              "end";
      try (MultiShardStatement stmt = shardConnection.createCommand()) {
        stmt.setCommandText(dropSchema);
        stmt.setExecutionPolicy(MultiShardExecutionPolicy.PartialResults);
        stmt.executeQueryAsync().call();
      }
    }
  }

  /**
   * Verifies that the command cancellation events are fired
   * upon cancellation of a command that is in progress
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testQueryShardsCommandCancellationHandler() {
    List<ShardLocation> cancelledShards = new ArrayList<>();

    try (MultiShardStatement stmt = shardConnection.createCommand()) {
      //Barrier barrier = new Barrier(stmt.Connection.Shards.Count() + 1);

      // If the threads don't meet the barrier by this time, then give up and fail the test
      Duration barrierTimeout = Duration.ofSeconds(10);

      stmt.setCommandText("WAITFOR DELAY '00:01:00'");
      stmt.setCommandTimeoutPerShard(12);

      //TODO
      /*
      stmt.shardExecutionCanceled += (obj, args) -> {
        cancelledShards.add(args.ShardLocation);
      };

      stmt.shardExecutionBegan += (obj, args) -> {
        // If shardExecutionBegan were only signaled by one thread, then this would hang forever.
        barrier.SignalAndWait(barrierTimeout);
      };
      */

      Callable cmdTask = stmt.executeQueryAsync();

      /*boolean syncronized = barrier.SignalAndWait(barrierTimeout);
      assert syncronized;*/

      // Cancel the command once execution begins Sleeps are bad,
      // but this is just to really make sure sql client has had a chance to begin command execution
      // and will not effect the test outcome
      Thread.sleep(1);
      stmt.cancel();

      // Validate that the task was cancelled
      //TODO: AssertExtensions.<TaskCanceledException>WaitAndAssertThrows(cmdTask);

      // Validate that the cancellation event was fired for all shards
      List<ShardLocation> allShards = shardConnection.getShardLocations();
      Assert.assertTrue("Expected command canceled event to be fired for all shards!",
          CollectionUtils.isEqualCollection(allShards, cancelledShards));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Close the connection to one of the shards behind MultiShardConnection's back. Verify that we
   * reopen the connection with the built-in retry policy
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testQueryShardsInvalidShardStateSync() {
    // Get a shard and close it's connection
    List<Pair<ShardLocation, Connection>> shardConnections = shardConnection.getShardConnections();
    try {
      shardConnections.get(1).getRight().close();
      // Execute
      try (MultiShardStatement stmt = shardConnection.createCommand()) {
        stmt.setCommandText(
            "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable");

        try (MultiShardResultSet sdr = stmt.executeQuery()) {
          sdr.close();
        }
      }
    } catch (RuntimeException ex) {
      if (ex instanceof MultiShardAggregateException) {
        MultiShardAggregateException aex = (MultiShardAggregateException) ex;
        log.info("Exception encountered: " + ex.getMessage());
        throw aex.getInnerExceptions().stream()
            .filter(e -> e instanceof IllegalStateException).findFirst().orElse(null);
      }
      throw ex;
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  /**
   * Validate the MultiShardConnectionString's connectionString param.
   * - Shouldn't be null
   * - No DataSource/InitialCatalog should be set
   * - ApplicationName should be enhanced with a MSQ library
   * specific suffix and should be capped at 128 chars
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testInvalidMultiShardConnectionString() throws SQLException {
    MultiShardConnection conn;
    List<Shard> shards = shardMap.getShards();
    Shard[] shardArray = shards.toArray(new Shard[shards.size()]);

    try {
      new MultiShardConnection(null, shardArray);
      Assert.fail("Expected ArgumentNullException!");
    } catch (RuntimeException ex) {
      Assert.assertTrue("Expected ArgumentNullException!", ex instanceof IllegalArgumentException);
    }

    try {
      new MultiShardConnection(MultiShardTestUtils.MULTI_SHARD_TEST_CONN_STRING, shardArray);
      Assert.fail("Expected ArgumentException!");
    } catch (RuntimeException ex) {
      Assert.assertTrue("Expected ArgumentException!", ex instanceof IllegalArgumentException);
    }

    // Validate that the ApplicationName is updated properly
    StringBuilder applicationStringBldr = new StringBuilder();
    for (int i = 0; i < ApplicationNameHelper.MaxApplicationNameLength; i++) {
      applicationStringBldr.append('x');
    }
    String applicationName = applicationStringBldr.toString();
    SqlConnectionStringBuilder connStringBldr = new SqlConnectionStringBuilder(
        MultiShardTestUtils.MULTI_SHARD_TEST_CONN_STRING);
    connStringBldr.setApplicationName(applicationName);
    conn = new MultiShardConnection(connStringBldr.getConnectionString(), shardArray);

    String updatedApplicationName = (new SqlConnectionStringBuilder(
        conn.getShardConnections().get(0).getRight().getMetaData().getURL())).getApplicationName();
    Assert.assertTrue(String.format("ApplicationName not appended with %1$s!",
        MultiShardConnection.ApplicationNameSuffix),
        updatedApplicationName.length() == ApplicationNameHelper.MaxApplicationNameLength
            && updatedApplicationName.endsWith(MultiShardConnection.ApplicationNameSuffix));
  }

  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testCreateConnectionWithNoShards() {
    try (MultiShardConnection conn = new MultiShardConnection("", new Shard[0])) {
      Assert.fail("Should have failed in the MultiShardConnection c-tor.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Regression test for VSTS Bug# 3936154
   * - Execute a command that will result in a failure in a loop
   * - Without the fix (disabling the command behavior)s, the test will hang and timeout.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testFailedCommandWithConnectionCloseCmdBehavior() {
    ExecutorService exec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    try {
      for (int i = 0; i < 100; i++) {
        int index = i;
        exec.submit(() -> {
          try (MultiShardStatement stmt = shardConnection.createCommand()) {
            stmt.setCommandText("select * from table_does_not_exist");

            try (MultiShardResultSet sdr = stmt.executeQuery()) {
              sdr.next();
              sdr.next();
            }
          } catch (RuntimeException ex) {
            System.out.printf("Encountered exception: %1$s in iteration: %2$s \r\n",
                ex.toString(), index);
          } catch (Exception ex) {
            ex.printStackTrace();
          } finally {
            System.out.printf("Completed execution of iteration: %1$s" + "\r\n", index);
          }
        });
      }
    } finally {
      exec.shutdown();
    }
  }

//  /**
//   * This test induces failures via a ProxyServer in order to validate that:
//   * a) we are handling reader failures as expected, and
//   * b) we get all-or-nothing semantics on our reads from a single row
//   */
//  @Test
//  @Category(value = ExcludeFromGatedCheckin.class)
//  public final void testShardResultFailures() {
//    ProxyServer proxyServer = getProxyServer();
//
//    try {
//      // Start up the proxy server.  Do it in a try so we can shut it down in the finally.
//      // Also, we have to generate the proxyShardconnections *AFTER* we start up the server
//      // so that we know what port the proxy is listening on.  More on the placement
//      // of the connection generation below.
//      proxyServer.Start();
//
//      // PreKillReads is the number of successful reads to perform before killing
//      // all the connections.  We start at 0 to test the no failure case as well.
//      for (int preKillReads = 0; preKillReads <= 10; preKillReads++) {
//        // Additionally, since we are running inside a loop, we need to regenerate the proxy shard connections each time
//        // so that we don't re-use dead connections.  If we do that we will end up hung in the read call.
//        ArrayList<Tuple<ShardLocation, DbConnection>> proxyShardConnections = getProxyShardConnections(
//            proxyServer);
//        try (MultiShardConnection conn = new MultiShardConnection(proxyShardConnections)) {
//          try (MultiShardStatement stmt = conn.createCommand()) {
//            stmt.setCommandText(
//                "SELECT db_name() as dbName1, REPLICATE(db_name(), 1000) as longExpr, db_name() as dbName2 FROM ConsistentShardedTable");
//
//            stmt.setExecutionPolicy(MultiShardExecutionPolicy.PartialResults);
//            stmt.ExecutionOptions = MultiShardExecutionOptions.IncludeShardNameColumn;
//
//            try (MultiShardResultSet sdr = stmt.ExecuteReader(CommandBehavior.Default)) {
//              int tuplesRead = 0;
//
//              while (sdr.next()) {
//                // Read part of the tuple first before killing the connections and
//                // then attempting to read the rest of the tuple.
//                tuplesRead++;
//
//                try {
//                  // The longExpr should contain the first dbName field multiple times.
//                  String dbName1 = sdr.getString(0);
//                  String longExpr = sdr.getString(1);
//                  assert longExpr.contains(dbName1);
//
//                  if (tuplesRead == preKillReads) {
//                    proxyServer.KillAllConnections();
//                  }
//
//                  // The second dbName field should be the same as the first dbName field.
//                  String dbName2 = sdr.getString(2);
//                  assert dbName1 == dbName2;
//
//                  // The shardId should contain both the first and the second dbName fields.
//                  String shardId = sdr.getString(3);
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
//                  Assert.fail("Unexpected exception, rethrowing."
//                          + "Here is some info: \n Message: {0} \n Source: {1} \n StackTrace: {2}",
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
//      String proxyLog = proxyServer.EventLog.toString();
//      log.info(proxyLog);
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
//  private ProxyServer getProxyServer() {
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
//  private List<Pair<ShardLocation, Connection>> getProxyShardConnections(ProxyServer proxy) {
//    // We'll do this by looking at our pre-existing connections and working from that.
//    String baseConnString = MultiShardTestUtils.ShardConnectionString.toString();
//    ArrayList<Tuple<ShardLocation, DbConnection>> rVal
//        = new ArrayList<Tuple<ShardLocation, DbConnection>>();
//    for (Shard shard : shardMap.GetShards()) {
//      // Location doesn't really matter, so just use the same one.
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
