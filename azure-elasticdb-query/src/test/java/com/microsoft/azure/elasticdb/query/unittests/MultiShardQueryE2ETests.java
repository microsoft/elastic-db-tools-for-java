package com.microsoft.azure.elasticdb.query.unittests;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.helpers.ApplicationNameHelper;
import com.microsoft.azure.elasticdb.query.category.ExcludeFromGatedCheckin;
import com.microsoft.azure.elasticdb.query.exception.MultiShardAggregateException;
import com.microsoft.azure.elasticdb.query.exception.MultiShardException;
import com.microsoft.azure.elasticdb.query.exception.MultiShardResultSetClosedException;
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
import com.microsoft.sqlserver.jdbc.SQLServerDataTable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for end to end scenarios where a user connects to his shards, executes commands against
 * them and receives results Purpose: Basic End-To-End test scenarios for the cross shard query
 * client library.
 * Notes: Tests currently assume there's a running sqlservr instance. Everything will
 * be automated once we integrate with the larger framework. Currently the tests use the same
 * methods to create shards as MultiShardResultSetTests.
 */
public class MultiShardQueryE2ETests {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Handle on connections to all shards.
   */
  private MultiShardConnection shardConnection;

  /**
   * Handle to the ShardMap with our Test databases.
   */
  private ShardMap shardMap;

  /**
   * Create three test databases and populate them with random test data to drive the tests.
   */
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
          String shardIdPseudoColumn = sdr.getLocation();
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
   * Check that we can return an empty result set that has a schema table.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testSelect_NoRows_CompleteResults() {
    testSelectNoRows("select 1 where 0 = 1", MultiShardExecutionPolicy.CompleteResults);
  }

  /**
   * Check that we can return an empty result set that has a schema table.
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
      Assert.fail(e.getMessage());
    }

  }

  /**
   * Check that we can return an empty result set that does not have a schema table.
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
        // Fixing this will require significant refactoring of MultiShardResultSet
        MultiShardResultSetClosedException ex = AssertExtensions.assertThrows(() -> {
          try {
            sdr.next();
          } catch (SQLException e) {
            Assert.fail(e.getMessage());
          }
        });

        assert ex != null;
      }
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  /**
   * Check that ExecuteReader throws when all shards have an exception.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testSelect_Failure_PartialResults() {
    MultiShardAggregateException e = testSelectFailure("raiserror('blah', 16, 0)",
        MultiShardExecutionPolicy.PartialResults);

    // All children should have failed
    assert e != null;
    assert shardMap.getShards().size() == e.getInnerExceptions().size();
  }

  /**
   * Check that ExecuteReader throws when all shards have an exception.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testSelect_Failure_CompleteResults() {
    MultiShardAggregateException e = testSelectFailure("raiserror('blah', 16, 0)",
        MultiShardExecutionPolicy.CompleteResults);

    // We don't know exactly how many child exceptions will happen, because the first exception that
    // is seen will cause the children to be canceled.
    assert e != null;
    assert e.getInnerExceptions().size() >= 1;
    assert e.getInnerExceptions().size() <= shardMap.getShards().size();
  }

  private MultiShardAggregateException testSelectFailure(String commandText,
      MultiShardExecutionPolicy policy) {
    try (MultiShardStatement stmt = shardConnection.createCommand()) {
      stmt.setCommandText(commandText);
      stmt.setExecutionPolicy(policy);

      // ExecuteReader should fail
      try {
        stmt.executeQuery();
        Assert.fail("Statement was expected to fail but did not fail.");
      } catch (MultiShardAggregateException ex) {
        for (Exception e : ex.getInnerExceptions()) {
          assert e instanceof MultiShardException;
          assert e.getCause() instanceof SQLException;
        }
        // Return the exception so that the caller can do additional validation
        return ex;
      }
    } catch (Exception e) {
      Assert.fail("Statement was expected to fail with MultiShardAggregateException.");
    }
    return null;
  }

  /**
   * Check that we can return a partially succeeded reader when PartialResults policy is on.
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
        if (!sdr.next()) {
          Assert.fail("We should be able to read. .next() failed.");
        }
      }
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  /**
   * Check that we fail a partially successful command when CompleteResults policy is on.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testSelect_PartialFailure_CompleteResults() {
    String query = getPartialFailureQuery();
    MultiShardAggregateException e = testSelectFailure(query,
        MultiShardExecutionPolicy.CompleteResults);

    // Exactly one should have failed
    assert e != null;
    assert 1 == e.getInnerExceptions().size();
  }

  /**
   * Gets a command that fails on one shard, but succeeds on others.
   */
  private String getPartialFailureQuery() {
    List<ShardLocation> shardLocations = shardMap.getShards().stream().map(Shard::getLocation)
        .collect(Collectors.toList());

    // Pick an arbitrary one of those shards
    int index = new Random().nextInt(shardLocations.size());
    ShardLocation chosenShardLocation = shardLocations.get(index);

    // This query assumes that the chosen shard location's db name is distinct from all others
    // In other words, only one shard location should have a database equal to the chosen location
    assert 1 == shardLocations.stream()
        .filter(l -> l.getDatabase().equals(chosenShardLocation.getDatabase())).count();

    // We also assume that there is more than one shard
    assert shardLocations.size() > 1;

    // The command will fail only on the chosen shard
    return String.format("if db_name() = '%1$s' raiserror('blah', 16, 0) else select 1",
        shardLocations.get(0).getDatabase());
  }

  /**
   * Basic test for async api(s), Also demonstrates the async pattern of this library.
   * The Sync api is implicitly tested in MultiShardResultSetTests::TestSimpleSelect.
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
          String dbNameField = sdr.getString(1);
          int testIntField = sdr.getInt(2);
          long testBigIntField = sdr.getLong(3);
          log.info("RecordRetrieved: dbNameField: {}, TestIntField: {}, TestBigIntField: {}, Count:"
              + " {}", dbNameField, testIntField, testBigIntField, recordsRetrieved);
        }

        assert recordsRetrieved == 9;
      }
    } catch (Exception e) {
      Assert.fail(e.getMessage());
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
          MultiShardTestUtils.MULTI_SHARD_CONN_STRING,
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

            assert 3 == sdr.getMetaData().getColumnCount();

            while (sdr.next()) {
              recordsRetrieved++;
              sdr.getString(1);
              sdr.getInt(2);
              sdr.getLong(3);

              String shardIdPseudoColumn = sdr.getLocation();
              if (!pseudoColumnPresent && !Objects.equals(shardIdPseudoColumn, "")) {
                Assert.fail("Should not have been able to pull the pseudo column.");
              }
              if (pseudoColumnPresent && Objects.equals(shardIdPseudoColumn, "")) {
                Assert.fail("Should have been able to pull the pseudo column.");
              }
            }

            assert recordsRetrieved == 9;
          }
        }
      } catch (Exception e) {
        Assert.fail(e.getMessage());
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
            sdr.getString(1);
          }

          // We should see 9 records less 3 for each one that had a schema error.
          int expectedRecords = ((9 - (3 * sdr.getMultiShardExceptions().size())));

          assert recordsRetrieved == expectedRecords;
        }
      }
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    } finally {
      try {
        MultiShardTestUtils.changeColumnNameOnShardedTable(2, newColName, origColName);
      } catch (SQLException e) {
        Assert.fail(e.getMessage());
      }
    }
  }

  private Callable<MultiShardResultSet> execAsync(MultiShardStatement stmt) {
    stmt.setExecutionPolicy(MultiShardExecutionPolicy.PartialResults);
    stmt.setExecutionOptions(MultiShardExecutionOptions.IncludeShardNameColumn);

    return stmt.executeQueryAsync(CommandBehavior.Default);
  }

  /**
   * Tests passing a tvp as a param using a SQLServerDataTable.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testQueryShardsTvpParam() throws Exception {
    try {
      // Install schema
      String createTbl
          = "CREATE TABLE dbo.PageView (PageViewID BIGINT NOT NULL, PageViewCount BIGINT NOT NULL);"
          + "\r\n" + "CREATE TYPE dbo.PageViewTableType AS TABLE (PageViewID BIGINT NOT NULL);";
      String createProc = "CREATE PROCEDURE dbo.procMergePageView" + "\r\n"
          + "  @Display dbo.PageViewTableType READONLY" + "\r\n" + "AS" + "\r\n" + "BEGIN" + "\r\n"
          + "    MERGE INTO dbo.PageView AS T USING @Display AS S ON T.PageViewID = S.PageViewID"
          + "\r\n" + "    WHEN MATCHED THEN UPDATE SET T.PageViewCount = T.PageViewCount + 1"
          + "\r\n" + "    WHEN NOT MATCHED THEN INSERT VALUES(S.PageViewID, 1);" + "\r\n" + "END";
      try (MultiShardStatement stmt = shardConnection.createCommand()) {
        stmt.setCommandText(createTbl);
        stmt.setExecutionPolicy(MultiShardExecutionPolicy.PartialResults);
        stmt.executeQuery();

        stmt.setCommandText(createProc);
        stmt.executeQueryAsync().call();
      } catch (Exception e) {
        Assert.fail(e.getMessage());
      }

      log.info("Schema installed..");

      // Create the data table
      SQLServerDataTable table = new SQLServerDataTable();
      table.addColumnMetadata("PageViewID", Types.BIGINT);
      Random random = new Random();
      for (int i = 0; i < 3; i++) {
        table.addRow(Long.toString(random.nextLong()));
      }

      // Execute the command
      try (MultiShardStatement stmt = shardConnection.createCommand()) {
        stmt.setCommandText("EXEC dbo.procMergePageView ?");
        stmt.setExecutionPolicy(MultiShardExecutionPolicy.PartialResults);

        stmt.setParameters(1, Types.STRUCT, "dbo.PageViewTableType", table);

        stmt.executeQuery();
        stmt.executeQuery();
      } catch (Exception e) {
        Assert.fail(e.getMessage());
      }

      log.info("Command executed..");

      try (MultiShardStatement stmt = shardConnection.createCommand()) {
        // Validate that the pageviewcount was updated
        stmt.setCommandText("SELECT PageViewCount FROM PageView");
        stmt.setExecutionPolicy(MultiShardExecutionPolicy.PartialResults);
        stmt.setExecutionOptions(MultiShardExecutionOptions.IncludeShardNameColumn);
        try (MultiShardResultSet sdr = stmt.executeQuery(CommandBehavior.Default)) {
          while (sdr.next()) {
            long pageCount = sdr.getLong("PageViewCount");
            log.info("Page view count: {0} obtained from shard: {1}", pageCount, sdr.getString(1));
            assert 2 == pageCount;
          }
        }
      } catch (Exception e) {
        Assert.fail(e.getMessage());
      }
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    } finally {
      String dropSchema = "IF EXISTS"
          + " (SELECT * FROM dbo.sysobjects WHERE id = object_id(N'[dbo].[procMergePageView]') AND"
          + " objectproperty(id, N'IsProcedure') = 1)" + "\r\n" + "BEGIN" + "\r\n"
          + "  DROP PROCEDURE dbo.procMergePageView" + "\r\n" + "END" + "\r\n"
          + "IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id(N'[dbo].[Pageview]'))"
          + "\r\n" + "BEGIN" + "\r\n" + "  DROP TABLE dbo.Pageview" + "\r\n" + "END" + "\r\n"
          + "IF EXISTS (SELECT * FROM sys.types WHERE name = 'PageViewTableType')" + "\r\n"
          + "BEGIN" + "\r\n" + "DROP TYPE dbo.PageViewTableType" + "\r\n" + "END";
      try (MultiShardStatement stmt = shardConnection.createCommand()) {
        stmt.setCommandText(dropSchema);
        stmt.setExecutionPolicy(MultiShardExecutionPolicy.PartialResults);
        stmt.executeQueryAsync().call();
      }
    }
  }

  /**
   * Verifies that the cancellation events are fired upon cancellation of an in progress command.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  @Ignore
  public final void testQueryShardsCommandCancellationHandler() {
    List<ShardLocation> cancelledShards = new ArrayList<>();

    try (MultiShardStatement stmt = shardConnection.createCommand()) {
      CyclicBarrier barrier = new CyclicBarrier(stmt.getConnection().getShards().size() + 1);

      // If the threads don't meet the barrier by this time, then give up and fail the test
      Duration barrierTimeout = Duration.ofSeconds(10);

      stmt.setCommandText("WAITFOR DELAY '00:01:00'");
      stmt.setCommandTimeoutPerShard(12);

      stmt.shardExecutionCanceled.addListener((obj, args)
          -> cancelledShards.add(args.getShardLocation()));

      // If shardExecutionBegan were only signaled by one thread, then this would hang forever.
      stmt.shardExecutionBegan.addListener((obj, args) -> {
        try {
          barrier.await(barrierTimeout.getSeconds(), TimeUnit.SECONDS);
        } catch (Exception e) {
          throw new RuntimeException(e.getMessage(), e);
        }
      });

      Callable cmdTask = stmt.executeQueryAsync();
      cmdTask.call();

      // Validate that the task was cancelled
      try {
        int syncronized = barrier.await(barrierTimeout.getSeconds(), TimeUnit.SECONDS);
        //TODO: assert syncronized;

        // Cancel the command once execution begins, Sleeps are bad, but this is to really make sure
        // sql client had a chance to begin command execution and will not effect the test outcome
        Thread.sleep(1);
        stmt.cancel();

        Assert.fail("Task was supposed to be cancelled.");
      } catch (Exception e) {
        assert e instanceof MultiShardAggregateException;
      }

      // Validate that the cancellation event was fired for all shards
      List<ShardLocation> allShards = shardConnection.getShardLocations();
      Assert.assertTrue("Expected command canceled event to be fired for all shards!",
          CollectionUtils.isEqualCollection(allShards, cancelledShards));
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  /**
   * Close the connection to one of the shards behind MultiShardConnection's back. Verify that we
   * reopen the connection with the built-in retry policy.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testQueryShardsInvalidShardStateSync() throws Exception {
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
    } catch (Exception ex) {
      log.info("Exception encountered: " + ex.getMessage());
      Assert.fail(ex.toString());
    }
  }

  /**
   * Validate the MultiShardConnectionString's connectionString param.
   * - Shouldn't be null.
   * - No DataSource/InitialCatalog should be set.
   * - ApplicationName should be enhanced with a library specific suffix and capped at 128 chars.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testInvalidMultiShardConnectionString() throws SQLException {
    List<Shard> shards = shardMap.getShards();
    Shard[] shardArray = shards.toArray(new Shard[shards.size()]);

    try {
      new MultiShardConnection(null, shardArray);
      Assert.fail("Expected ArgumentNullException!");
    } catch (RuntimeException ex) {
      Assert.assertTrue("Expected ArgumentNullException!", ex instanceof IllegalArgumentException);
    }

    try {
      new MultiShardConnection("", shardArray);
      Assert.fail("Expected ArgumentException!");
    } catch (RuntimeException ex) {
      Assert.assertTrue("Expected ArgumentException!", ex instanceof IllegalArgumentException);
    }

    // Validate that the ApplicationName is updated properly
    StringBuilder applicationStringBldr = new StringBuilder();
    int length = ApplicationNameHelper.MaxApplicationNameLength
        - MultiShardConnection.ApplicationNameSuffix.length();
    for (int i = 0; i < length; i++) {
      applicationStringBldr.append('x');
    }
    String applicationName = applicationStringBldr.toString();
    SqlConnectionStringBuilder connStringBldr = new SqlConnectionStringBuilder(
        MultiShardTestUtils.MULTI_SHARD_CONN_STRING);
    connStringBldr.setApplicationName(applicationName);
    MultiShardConnection conn = new MultiShardConnection(connStringBldr.getConnectionString(),
        shardArray);

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
      conn.close();
      Assert.fail("Should have failed in the MultiShardConnection c-tor.");
    } catch (IllegalArgumentException e) {
      assert e.getMessage().equals("connectionString");
    } catch (IOException e) {
      Assert.fail(e.getMessage());
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
              while (sdr.next()) {
              }
            }
          } catch (Exception ex) {
            System.out.printf("Encountered exception: %1$s in iteration: %2$s \r\n",
                ex.toString(), index);
          } finally {
            System.out.printf("Completed execution of iteration: %1$s" + "\r\n", index);
          }
        });
      }
    } finally {
      exec.shutdown();
    }
  }
}
