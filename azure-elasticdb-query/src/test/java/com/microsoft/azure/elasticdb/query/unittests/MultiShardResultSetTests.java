package com.microsoft.azure.elasticdb.query.unittests;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.query.category.ExcludeFromGatedCheckin;
import com.microsoft.azure.elasticdb.query.exception.MultiShardAggregateException;
import com.microsoft.azure.elasticdb.query.exception.MultiShardException;
import com.microsoft.azure.elasticdb.query.exception.MultiShardResultSetClosedException;
import com.microsoft.azure.elasticdb.query.exception.MultiShardSchemaMismatchException;
import com.microsoft.azure.elasticdb.query.logging.CommandBehavior;
import com.microsoft.azure.elasticdb.query.logging.MultiShardExecutionOptions;
import com.microsoft.azure.elasticdb.query.logging.MultiShardExecutionPolicy;
import com.microsoft.azure.elasticdb.query.multishard.LabeledResultSet;
import com.microsoft.azure.elasticdb.query.multishard.MultiShardConnection;
import com.microsoft.azure.elasticdb.query.multishard.MultiShardResultSet;
import com.microsoft.azure.elasticdb.query.multishard.MultiShardStatement;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlConnectionStringBuilder;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;
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
 * Very basic unit tests for the MultiShardResultSet class. Just enough to ensure that simple
 * scenarios working as expected. Purpose: Basic unit testing for the MultiShardResultSet class.
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

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private Connection conn1;
  private Connection conn2;
  private Connection conn3;
  private List<Connection> conns;
  /**
   * Handle on conn1, conn2 and conn3
   */
  private MultiShardConnection shardConnection;
  /**
   * Placeholder object for us to pass into MSDRs that we create without going through a command.
   */
  private MultiShardStatement dummyStatement;

  /**
   * Currently doesn't do anything special.
   */
  public MultiShardResultSetTests() {
  }

  /**
   * Sets up our three test databases that we drive the unit testing off of.
   */
  @BeforeClass
  public static void myClassInitialize() throws SQLException {
    // Drop and recreate the test databases, tables, and data that we will use to verify
    // the functionality.
    // For now I have hardcoded the server location and database names.  A better approach would be
    // to make the server location configurable and the database names be guids.
    // Not the top priority right now, though.
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
    ShardMap sm = MultiShardTestUtils.createAndGetTestShardMap();

    // Use the MultiShardConnection to open up connections

    // Validate the connections to shards
    List<Shard> shards = sm.getShards();
    shardConnection = new MultiShardConnection(
        MultiShardTestUtils.MULTI_SHARD_CONN_STRING,
        shards.toArray(new Shard[shards.size()]));
    dummyStatement = MultiShardStatement.create(shardConnection, "SELECT 1");

    List<Pair<ShardLocation, Connection>> shardConnections = shardConnection.getShardConnections();

    conn1 = shardConnections.get(0).getRight();
    conn2 = shardConnections.get(1).getRight();
    conn3 = shardConnections.get(2).getRight();
    conns = shardConnections.stream().map(Pair::getRight).collect(Collectors.toList());
  }

  /**
   * Close our connections to each test database after each test.
   */
  @After
  public final void myTestCleanup() {
    for (Pair<ShardLocation, Connection> conn : shardConnection.getShardConnections()) {
      try {
        conn.getRight().close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Validate MultiShardResultSet can be supplied as argument to DataTable.Load
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testDataTableLoad() {
    // What we're doing:
    // Obtain MultiShardResultSet,
    // Pass it to DataTable.Load and ensure correct number of rows is loaded.
    String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field"
        + " FROM ConsistentShardedTable";

    try (MultiShardResultSet sdr = GetShardedDbReader(shardConnection, selectSql)) {
      if (sdr.next()) {
        Assert.assertEquals("Expected 9 rows in the result set", 9, sdr.getRowCount());

        int recordsRetrieved = 0;
        while (sdr.next()) {
          recordsRetrieved++;
          String dbNameField = sdr.getString(0);
          int testIntField = sdr.getInt(1);
          long testBigIntField = sdr.getLong(2);
          String shardIdPseudoColumn = sdr.getString(3);
          String logRecord = String.format("RecordRetrieved: dbNameField: %1$s, TestIntField: %2$s,"
                  + " TestBigIntField: %3$s, shardIdPseudoColumnField: %4$s, RecordCount: %5$s",
              dbNameField, testIntField, testBigIntField, shardIdPseudoColumn, recordsRetrieved);
          log.info(logRecord);
        }
        assert recordsRetrieved == 9;
      }
    } catch (SQLException | MultiShardAggregateException e) {
      Assert.fail(e.getMessage());
    }
  }

  /**
   * Check that we can turn the $ShardName pseudo column on and off as expected.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testShardNamePseudoColumnOption() throws SQLException {
    // What we're doing:
    // Grab all rows from each test database.
    // Load them into a MultiShardResultSet.
    // Iterate through the rows and make sure that we have 9 rows total with
    // the Pseudo column present (or not) as per the setting we used.
    String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field"
        + " FROM ConsistentShardedTable";
    boolean[] pseudoColumnPresentOptions = new boolean[2];
    pseudoColumnPresentOptions[0] = true;
    pseudoColumnPresentOptions[1] = false;

    for (boolean pseudoColumnPresent : pseudoColumnPresentOptions) {
      LabeledResultSet[] readers = new LabeledResultSet[3];
      readers[0] = GetReader(conn1, selectSql);
      readers[1] = GetReader(conn2, selectSql);
      readers[2] = GetReader(conn3, selectSql);

      List<MultiShardSchemaMismatchException> exceptions;

      ReferenceObjectHelper<List<MultiShardSchemaMismatchException>> tempRef_exceptions
          = new ReferenceObjectHelper<>(null);
      try (MultiShardResultSet sdr = GetMultiShardDataReaderFromResultSets(readers,
          tempRef_exceptions)) {
        exceptions = tempRef_exceptions.argValue;
        assert 0 == exceptions.size();

        int recordsRetrieved = 0;

        int expectedFieldCount = pseudoColumnPresent ? 4 : 3;
        assert expectedFieldCount == sdr.getRowCount();

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
        sdr.close();
        assert recordsRetrieved == 9;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Check that we can handle empty result sets interspersed with non-empty result sets as expected.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testMiddleResultEmptyOnSelect() throws SQLException {
    // What we're doing:
    // Grab all rows from each test database that satisfy a particular predicate
    // (there should be 3 from db1 and db3 and 0 from db2).
    // Load them into a MultiShardResultSet.
    // Iterate through the rows and make sure that we have 6 rows.
    String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field"
        + " FROM ConsistentShardedTable WHERE dbNameField='Test0' OR dbNameField='Test2'";
    LabeledResultSet[] readers = new LabeledResultSet[3];
    readers[0] = GetReader(conn1, selectSql);
    readers[1] = GetReader(conn2, selectSql);
    readers[2] = GetReader(conn3, selectSql);

    List<MultiShardSchemaMismatchException> exceptions;
    ReferenceObjectHelper<List<MultiShardSchemaMismatchException>> tempRef_exceptions
        = new ReferenceObjectHelper<>(null);
    try (MultiShardResultSet sdr = GetMultiShardDataReaderFromResultSets(readers,
        tempRef_exceptions)) {
      exceptions = tempRef_exceptions.argValue;
      assert 0 == exceptions.size();

      int recordsRetrieved = 0;
      while (sdr.next()) {
        recordsRetrieved++;
      }

      sdr.close();

      assert recordsRetrieved == 6;
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * Check that we can handle non-empty result sets interspersed with empty result sets as expected.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testOuterResultsEmptyOnSelect() throws SQLException {
    // What we're doing:
    // Grab all rows from each test database that satisfy a particular predicate
    // (there should be 0 from db1 and db3 and 3 from db2).
    // Load them into a MultiShardResultSet.
    // Iterate through the rows and make sure that we have 3 rows.
    String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field"
        + " FROM ConsistentShardedTable WHERE dbNameField='Test1'";
    LabeledResultSet[] readers = new LabeledResultSet[3];
    readers[0] = GetReader(conn1, selectSql);
    readers[1] = GetReader(conn2, selectSql);
    readers[2] = GetReader(conn3, selectSql);

    List<MultiShardSchemaMismatchException> exceptions;

    ReferenceObjectHelper<List<MultiShardSchemaMismatchException>> tempRef_exceptions
        = new ReferenceObjectHelper<>(null);
    try (MultiShardResultSet sdr = GetMultiShardDataReaderFromResultSets(readers,
        tempRef_exceptions)) {
      exceptions = tempRef_exceptions.argValue;
      assert 0 == exceptions.size();

      int recordsRetrieved = 0;
      while (sdr.next()) {
        recordsRetrieved++;
      }

      sdr.close();

      assert recordsRetrieved == 3;
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * Check that we collect an exception and expose it on the ShardedReader when encountering schema
   * mismatches across result sets due to different column names.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testMismatchedSchemasWrongColumnName() throws SQLException {
    // What we're doing:
    // Issue different queries to readers 1 & 2 so that we have the same column count and types
    // but we have a column name mismatch.
    // Try to load them into a MultiShardResultSet.
    // Should see an  exception on the MultiShardResultSet.
    // Should also be able to successfully iterate through some records.
    String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field"
        + " FROM ConsistentShardedTable;";
    String alternateSelectSql = "SELECT dbNameField as DifferentName, Test_int_Field,"
        + " Test_bigint_Field FROM ConsistentShardedTable;";
    LabeledResultSet[] readers = new LabeledResultSet[2];
    readers[0] = GetReader(conn1, selectSql);
    readers[1] = GetReader(conn2, alternateSelectSql);

    List<MultiShardSchemaMismatchException> exceptions;
    ReferenceObjectHelper<List<MultiShardSchemaMismatchException>> tempRef_exceptions
        = new ReferenceObjectHelper<>(null);
    try (MultiShardResultSet sdr = GetMultiShardDataReaderFromResultSets(readers,
        tempRef_exceptions)) {
      exceptions = tempRef_exceptions.argValue;
      if ((null == exceptions) || (exceptions.size() != 1)) {
        Assert.fail("Expected an element in the InvalidReaders collection.");
      } else {
        int recordsRetrieved = 0;

        while (sdr.next()) {
          recordsRetrieved++;
        }

        assert recordsRetrieved == 3;
      }
      sdr.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * Check that we throw as expected when encountering schema mismatches across result sets due to
   * different column types.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testMismatchedSchemasWrongType() throws SQLException {
    // What we're doing:
    // Issue different queries to readers 1 & 2 so that we have the same column count and names
    // but we have a column type mismatch.
    // Try to load them into a MultiShardResultSet.
    // Should see an exception on the MultiShardResultSet.
    // Should also be able to successfully iterate through some records.
    String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field"
        + " FROM ConsistentShardedTable;";
    String alternateSelectSql = "SELECT dbNameField, Test_int_Field,"
        + " Test_int_Field as Test_bigint_Field FROM ConsistentShardedTable;";
    LabeledResultSet[] readers = new LabeledResultSet[2];
    readers[0] = GetReader(conn1, selectSql);
    readers[1] = GetReader(conn2, alternateSelectSql);

    List<MultiShardSchemaMismatchException> exceptions;
    ReferenceObjectHelper<List<MultiShardSchemaMismatchException>> tempRef_exceptions
        = new ReferenceObjectHelper<>(null);
    try (MultiShardResultSet sdr = GetMultiShardDataReaderFromResultSets(readers,
        tempRef_exceptions)) {
      exceptions = tempRef_exceptions.argValue;
      if ((null == exceptions) || (exceptions.size() != 1)) {
        Assert.fail("Expected an element in the InvalidReaders collection.");
      } else {
        int recordsRetrieved = 0;

        while (sdr.next()) {
          recordsRetrieved++;
        }

        assert recordsRetrieved == 3;
      }
      sdr.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * Validate basic ReadAsync behavior.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testReadAsync() throws SQLException {
    LabeledResultSet[] readers = new LabeledResultSet[1];
    readers[0] = GetReader(conn1, "select 1");
    int numRowsRead = 0;

    try (MultiShardResultSet sdr = new MultiShardResultSet(Arrays.asList(readers))) {
      while (sdr.next()) {
        numRowsRead++;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    Assert.assertEquals("ReadAsync didn't return the expeceted number of rows.", 1, numRowsRead);
  }

  /**
   * Validate ReadAsync() behavior when multiple data readers are involved. This test is same as
   * existing test TestMiddleResultEmptyOnSelect except that we are using ReadAsync() in this case
   * instead of Read() to read individual rows.
   *
   * NOTE: We needn't replicate every single Read() test for ReadAsync() since Read() ends up
   * calling ReadAsync().Result under the hood. So, by validating Read(), we are also validating
   * ReadAsync() indirectly.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testReadSyncWithMultipleDataReaders() throws SQLException {
    // What we're doing:
    // Grab all rows from each test database that satisfy a particular predicate
    // (there should be 3 from db1 and db3 and 0 from db2).
    // Load them into a MultiShardResultSet.
    // Iterate through the rows using ReadAsync() and make sure that we have 6 rows.
    String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field"
        + " FROM ConsistentShardedTable WHERE dbNameField='Test0' OR dbNameField='Test2'";
    LabeledResultSet[] readers = new LabeledResultSet[3];
    readers[0] = GetReader(conn1, selectSql);
    readers[1] = GetReader(conn2, selectSql);
    readers[2] = GetReader(conn3, selectSql);

    List<MultiShardSchemaMismatchException> exceptions;
    ReferenceObjectHelper<List<MultiShardSchemaMismatchException>> tempRef_exceptions
        = new ReferenceObjectHelper<>(null);
    try (MultiShardResultSet sdr = GetMultiShardDataReaderFromResultSets(readers,
        tempRef_exceptions)) {
      exceptions = tempRef_exceptions.argValue;
      assert 0 == exceptions.size();

      int recordsRetrieved = 0;
      while (sdr.next()) {
        recordsRetrieved++;
      }

      sdr.close();

      assert recordsRetrieved == 6;
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testMultiShardQueryCancellation() throws SQLException {
    /*ManualResetEvent rollback = new ManualResetEvent(false);
    ManualResetEvent readerInitialized = new ManualResetEvent(false);*/
    boolean rollback = false;
    boolean readerInitialized = false;
    String dbToUpdate = conn2.getCatalog();

    // Start a task that would begin a transaction, update the rows on the second shard and then
    // block on an event. While the transaction is still open and the task is blocked, another
    // task will try to read rows off the shard.
    FutureTask lockRowTask = getLowRowTask(false, dbToUpdate);

    // Create a new task that would try to read rows off the second shard while they are locked by
    // the previous task and block therefore.
    FutureTask readToBlockTask = new FutureTask<>(() -> {
      String selectSql = String.format("SELECT dbNameField, Test_int_Field, Test_bigint_Field "
          + " FROM ConsistentShardedTable WHERE dbNameField='%1$s'", dbToUpdate);

      try (MultiShardResultSet sdr = GetShardedDbReaderAsync(shardConnection, selectSql)) {
        //TODO: set readerInitialized to true

        // This call should block.
        while (sdr.next()) {

        }
      }
      return 0;
    });

    try {
      lockRowTask.run();
      readToBlockTask.run();
      Assert.fail("The task expected to block ran to completion.");
    } catch (Exception aggex) {
      // Cancel the second task. This should trigger the cancellation of the multi-shard query.
      readToBlockTask.cancel(true);

      RuntimeException tempVar = (RuntimeException) aggex.getCause();
      //TODO:
      // TaskCanceledException ex = (TaskCanceledException)
      // ((tempVar instanceof TaskCanceledException) ? tempVar : null);

      Assert.assertTrue("A task canceled exception was not received upon cancellation.",
          tempVar != null);
    }

    // Set the event signaling the first task to rollback its update transaction.
    lockRowTask = getLowRowTask(true, dbToUpdate);

    lockRowTask.run();
  }

  /**
   * Check that we do not hang when trying to read after adding null readers.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testReadFromNullReader() throws SQLException {
    // The code below exposes a flaw in our current implementation related to
    // CompleteResults semantics and the internal c-tor.  The flaw does not
    // leak out to customers because the MultiShardStatement object manages the
    // necessary logic, but we need to patch the flaw so it doesn't end up
    // inadvertently leaking out to customers.
    // See VSTS 2616238 (i believe).  Philip will be modofying logic and
    // augmenting tests to deal with this issue.

    // Pass a null reader and verify that read does not hang.
    LabeledResultSet[] readers = new LabeledResultSet[2];
    readers[0] = GetReader(conn1, "select 1");
    readers[1] = null;

    try (MultiShardResultSet sdr = new MultiShardResultSet(Arrays.asList(readers))) {
      new FutureTask<>(() -> {
        int count = 0;
        while (sdr.next()) {
          count++;
        }
        return count;
      }).run();

      Thread.sleep(500);
      //TODO: create proper assert here and below after debugging the code
    } catch (SQLException | InterruptedException e) {
      e.printStackTrace();
      Assert.assertEquals("Read hung on the null reader.", e.getCause(), null);
    }
  }

  /**
   * Check that we do not hang when trying to read after adding a reader with an exception.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testReadFromReaderWithException() throws SQLException {
    // The code below exposes a flaw in our current implementation related to
    // CompleteResults semantics and the internal c-tor.  The flaw does not
    // leak out to customers because the MultiShardStatement object manages the
    // necessary logic, but we need to patch the flaw so it doesn't end up
    // inadvertently leaking out to customers.
    // See VSTS 2616238 (i believe).  Philip will be modofying logic and
    // augmenting tests to deal with this issue.

    // Pass a reader with an exception that read does not hang.
    LabeledResultSet[] readers = new LabeledResultSet[2];
    readers[0] = GetReader(conn1, "select 1");
    readers[1] = new LabeledResultSet(new MultiShardException(), new ShardLocation("foo", "bar"),
        conn2.createStatement());

    try (MultiShardResultSet sdr = new MultiShardResultSet(Arrays.asList(readers))) {
      new FutureTask<>(() -> {
        while (sdr.next()) {
        }
        return 0;
      }).run();

      Thread.sleep(500);
      //TODO: create proper assert here and below after debugging the code
      // Assert.assertEquals(TaskStatus.RanToCompletion, t.Status, "Read hung on the garbage reader.");
    } catch (SQLException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   * Validate that we throw an exception and invalidate the
   * MultiShardResultSet when we encounter a reader that has
   * multiple result sets
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testReadFromReaderWithNextResultException() throws SQLException {
    String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field FROM "
        + "ConsistentShardedTable WHERE Test_int_Field = 876; SELECT dbNameField, Test_int_Field, "
        + "Test_bigint_Field FROM ConsistentShardedTable WHERE Test_int_Field = 876";

    LabeledResultSet[] readers = new LabeledResultSet[1];
    readers[0] = GetReader(conn1, selectSql);

    MultiShardResultSet sdr = new MultiShardResultSet(Arrays.asList(readers));

    //TODO:
    /*AssertExtensions.<UnsupportedOperationException>WaitAndAssertThrows(sdr.NextResultAsync());
    Assert.IsTrue(sdr.IsClosed, "Expected MultiShardResultSet to be closed!");*/
  }

  /**
   * Check that we throw as expected when trying to add a LabeledResultSet with a null ResultSet
   * underneath.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testAddLabeledResultSetWithNullResultSet() throws SQLException {
    // What we're doing:
    // Set up a new sharded reader
    // Add two readers to it.
    // Try to add a third reader to it that has a null ResultSet underneath.
    // Verify that we threw as expected.
    String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field"
        + " FROM ConsistentShardedTable WHERE Test_int_Field = 876";
    LabeledResultSet[] readers = new LabeledResultSet[3];
    readers[0] = GetReader(conn1, selectSql);
    readers[1] = GetReader(conn2, selectSql);

    SqlConnectionStringBuilder str = new SqlConnectionStringBuilder(conn3.getMetaData().getURL());

    readers[2] = new LabeledResultSet(new ShardLocation(str.getDataSource(),
        str.getDatabaseName()), conn3.createStatement());

    try (MultiShardResultSet sdr = new MultiShardResultSet(Arrays.asList(readers))) {
      sdr.close();
    }
    //TODO: Verify Exception
  }

  /**
   * Check that we can iterate through the result sets as expected comparing all the values
   * returned from the getters plus some of the properties.
   * Check everythign both with and without the $ShardName pseudo column.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testGettersPositiveCases() {
    TestGettersPositiveCasesHelper(true);
    TestGettersPositiveCasesHelper(false);
  }

  /**
   * Check that we can iterate through the result sets as expected comparing all the values
   * returned from the getters plus some of the properties.
   */
  private void TestGettersPositiveCasesHelper(boolean includeShardNamePseudoColumn) {
    // What we're doing:
    // Grab all rows from each test database.
    // Load them into a MultiShardResultSet.
    // Iterate through the rows and make sure that we have 9 total.
    // Also iterate through all columns and make sure that the getters that should work do work.
    List<MultiShardTestCaseColumn> toCheck = MultiShardTestCaseColumn.getDefinedColumns();
    MultiShardTestCaseColumn pseudoColumn = MultiShardTestCaseColumn.getShardNamePseudoColumn();

    for (MultiShardTestCaseColumn curCol : toCheck) {
      String selectSql = String.format("SELECT %1$s FROM ConsistentShardedTable",
          curCol.getTestColumnName());

      try (MultiShardResultSet sdr = GetShardedDbReader(shardConnection, selectSql,
          includeShardNamePseudoColumn)) {
        int recordsRetrieved = 0;
        log.info("Starting to get records");
        while (sdr.next()) {
          // 2 columns if we have the shard name, 1 column if not.
          int expectedFieldCount = includeShardNamePseudoColumn ? 2 : 1;
          assert expectedFieldCount == sdr.getRowCount();

          recordsRetrieved++;

          // Do verification for the test column.
          CheckColumnName(sdr, curCol, 0);
          VerifyAllGettersPositiveCases(sdr, curCol, 0);

          // Then also do it for the $ShardName PseudoColumn if necessary.
          if (includeShardNamePseudoColumn) {
            CheckColumnName(sdr, pseudoColumn, 1);
            VerifyAllGettersPositiveCases(sdr, pseudoColumn, 1);
          }
        }

        sdr.close();

        assert recordsRetrieved == 9;
      } catch (SQLException | MultiShardAggregateException e) {
        Assert.fail(e.getMessage());
      }
    }
  }

  /**
   * Test what happens when we try to get a value without calling read first.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testBadlyPlacedGetValueCalls() {
    // What we're doing:
    // Set up a new sharded reader
    // Try to get a value without calling read first and see what happens.
    // Should throw.
    String selectSql = "SELECT 1";
    try (MultiShardResultSet sdr = GetShardedDbReader(shardConnection, selectSql)) {
      try {
        sdr.getInt(0);
        Assert.fail(String.format("Should have hit %1$s.", IllegalStateException.class));
      } catch (IllegalStateException ex) {
        assert ex.getClass().equals(IllegalStateException.class);
      }

      while (sdr.next()) {
        sdr.getInt(0);
      }

      try {
        sdr.getInt(0);
        Assert.fail(String.format("Should have hit %1$s.", IllegalStateException.class));
      } catch (IllegalStateException ex) {
        assert ex.getClass().equals(IllegalStateException.class);
      }

      sdr.close();

      try {
        sdr.getInt(0);
        Assert.fail(String.format("Should have hit %1$s.",
            MultiShardResultSetClosedException.class));
      } catch (MultiShardResultSetClosedException ex) {
        assert ex.getClass().equals(MultiShardResultSetClosedException.class);
      }

      // And try to close it again.

      sdr.close();
    } catch (SQLException | MultiShardAggregateException e) {
      Assert.fail(e.getMessage());
    }
  }

  private FutureTask getLowRowTask(boolean rollback, String dbToUpdate) {
    return new FutureTask<>(() -> {
      conn2.setAutoCommit(false);
      try (Statement cmd = conn2.createStatement()) {
        String query = String.format("UPDATE ConsistentShardedTable SET dbNameField='TestN'"
            + " WHERE dbNameField='%1$s'", dbToUpdate);

        // This will X-lock all rows in the second shard.
        cmd.execute(query);

        if (rollback) {
          conn2.rollback();
        }
      }
      return 0;
    });
  }

  private void CheckColumnName(MultiShardResultSet reader, MultiShardTestCaseColumn column,
      int ordinal) throws SQLException {
    assert Objects.equals(column.getTestColumnName(),
        reader.getMetaData().getColumnName(ordinal + 1));
    assert ordinal == reader.findColumn(column.getTestColumnName());
  }

  private void VerifyAllGettersPositiveCases(MultiShardResultSet reader,
      MultiShardTestCaseColumn column, int ordinal) throws SQLException {
    // General pattern here:
    // Grab the value through the regular getter, through the getValue,
    // through the sync GetFieldValue, and through the async GetFieldValue to ensure we are
    // getting back the same thing from all calls.
    // Then grab through the Sql getter to make sure it works. (should we compare again?)
    // Then verify that the field types are as we expect.
    // Note: For the array-based getters we can't do the sync/async comparison.

    // These are indexes into our .NET type array.
    int ItemOrdinalResult = 1;
    int ItemNameResult = 2;
    int GetResult = 3;
    Object[] results = new Object[GetResult + 1];

    // These first three we can pull consistently for all fields.
    // The rest have type specific getters.
    results[ItemOrdinalResult] = reader.getObject(ordinal);
    results[ItemNameResult] = reader.getObject(column.getTestColumnName());

    switch (column.getDbType()) {
      case Types.BIGINT:
        if (reader.next()) {
          results[GetResult] = reader.getLong(ordinal);
          AssertAllAreEqual(results);

          assert reader.getMetaData().getColumnType(ordinal) == Types.BIGINT;
        }
        break;
      case Types.BINARY:
      case Types.VARBINARY:
        if (reader.next()) {
          // Do the bytes and the stream.  Can also compare them.
          Byte[] byteBuffer = new Byte[column.getFieldLength()];
          reader.getByte(ordinal);

          InputStream theStream = reader.getBinaryStream(ordinal);
          Byte[] byteBufferFromStream = new Byte[column.getFieldLength()];
          assert byteBufferFromStream.length == column.getFieldLength();

          //TODO? int bytesRead = theStream.read(byteBufferFromStream, 0, column.getFieldLength());
          this.PerformArrayComparison(byteBuffer, byteBufferFromStream);

          // The value getter comes through as a SqlBinary, so we don't pull
          // the Sql getter here.
          reader.getByte(ordinal);

          assert reader.getObject(ordinal) == byte[].class;
        }
        break;
      case Types.BIT:
        if (reader.next()) {
          results[GetResult] = reader.getBoolean(ordinal);
          AssertAllAreEqual(results);

          assert reader.getObject(ordinal) == Boolean.class;
        }
        break;
      case Types.CHAR:
      case Types.NCHAR:
      case Types.NVARCHAR:
      case Types.VARCHAR:
        if (reader.next()) {
          /*char[] charBuffer = new char[column.getFieldLength()];
          long bufferLength = reader.getLong(ordinal);
          charBuffer = new char[bufferLength]; //size it right for the string compare below.
          reader.getLong(ordinal);*/

          // The value getter comes through as a SqlString, so we
          // don't pull the Sql getter here.
          reader.getCharacterStream(ordinal);

          results[GetResult] = reader.getString(ordinal);
          AssertAllAreEqual(results);

          // Also compare the string result to our char result.
          /*this.<Character>PerformArrayComparison(charBuffer,
              reader.getString(ordinal).trim().toCharArray());*/

          // and get a text reader.
          String fromTr = reader.getString(ordinal);
          assert fromTr == results[GetResult];

          assert reader.getObject(ordinal) == String.class;
        }
        break;
      case Types.DATE:
        // NOTE: docs say this can come back via SqlDateTime, but apparently it can't.
        // differs from above in the sql specific Getter.
        if (reader.next()) {
          results[GetResult] = reader.getDate(ordinal);
          AssertAllAreEqual(results);

          assert reader.getObject(ordinal) == LocalDateTime.class;
        }
        break;
      /*case microsoft.sql.Types.DATETIMEOFFSET:
        if (reader.next()) {
          results[GetResult] = reader.GetDateTimeOffset(ordinal);
          AssertAllAreEqual(results);

          assert reader.getObject(ordinal) == DateTimeOffset.class;
        }
        break;*/
      case Types.DECIMAL:
        if (reader.next()) {
          results[GetResult] = reader.getBigDecimal(ordinal);
          AssertAllAreEqual(results);

          assert reader.getObject(ordinal) == BigDecimal.class;
        }
        break;
      case Types.DOUBLE:
        if (reader.next()) {
          results[GetResult] = reader.getDouble(ordinal);
          AssertAllAreEqual(results);

          assert reader.getObject(ordinal) == Double.class;
        }
        break;
      case Types.INTEGER:
        if (reader.next()) {
          results[GetResult] = reader.getInt(ordinal);
          AssertAllAreEqual(results);

          assert reader.getObject(ordinal) == Integer.class;
        }
        break;
      case Types.REAL:
        if (reader.next()) {
          results[GetResult] = reader.getFloat(ordinal);
          AssertAllAreEqual(results);

          assert reader.getObject(ordinal) == Float.class;
        }
        break;
      case Types.SMALLINT:
        if (reader.next()) {
          results[GetResult] = reader.getShort(ordinal);
          AssertAllAreEqual(results);

          assert reader.getObject(ordinal) == Short.class;
        }
        break;
      case Types.TIME:
        // NOTE: docs say this can come back via GetDateTime, but apparently it can't.
        if (reader.next()) {
          results[GetResult] = reader.getTime(ordinal);
          AssertAllAreEqual(results);

          assert reader.getObject(ordinal) == Time.class;
        }
        break;
      case Types.TIMESTAMP:
        if (reader.next()) {
          // Differs from the other binaries in that it doesn't
          // support GetStream.
          byte[] byteBuffer = new byte[column.getFieldLength()];
          reader.getByte(ordinal);

          // The value getter comes through as a SqlBinary, so we don't pull
          // the Sql getter here.
          reader.getByte(ordinal);

          assert reader.getObject(ordinal) == byte[].class;
        }
        break;
      case Types.TINYINT:
        if (reader.next()) {
          results[GetResult] = reader.getByte(ordinal);
          AssertAllAreEqual(results);

          assert reader.getObject(ordinal) == Byte.class;
        }
        break;
      default:
        throw new IllegalArgumentException(Integer.toString(column.getDbType()));
    }
  }

  private void AssertAllAreEqual(Object[] toCheck) {
    Object baseline = toCheck[0];
    for (Object curObject : toCheck) {
      assert baseline == curObject;
    }
  }

  private <T> void PerformArrayComparison(T[] first, T[] second) {
    assert first.length == second.length;
    for (int i = 0; i < first.length; i++) {
      assert first[i] == second[i];
    }
  }

  /**
   * Gets a SqlDataReader by executing the passed in t-sql over the passed in connection.
   *
   * @param conn Connection to the database we wish to execute the t-sql against.
   * @param tsql The t-sql to execute.
   * @return The SqlDataReader obtained by executin the passed in t-sql over the passed in
   * connection.
   */
  private LabeledResultSet GetReader(Connection conn, String tsql) throws SQLException {
    String connStr = conn.getMetaData().getURL();
    SqlConnectionStringBuilder connStrBldr = new SqlConnectionStringBuilder(connStr);
    if (conn.isClosed()) {
      conn = DriverManager.getConnection(connStr);
    }
    Statement cmd = conn.createStatement();
    ResultSet sdr = cmd.executeQuery(tsql);

    return new LabeledResultSet(sdr, new ShardLocation(connStrBldr.getDataSource(),
        connStrBldr.getDatabaseName()), cmd);
  }

  /**
   * Helper that grabs a MultiShardResultSet based on a MultiShardConnection and a tsql string to
   * execute.
   *
   * @param conn The MultiShardConnection to use to get the command/reader.
   * @param tsql The tsql to execute on the shards.
   * @return The MultiShardResultSet resulting from executing the given tsql on the given
   * connection.
   */
  private MultiShardResultSet GetShardedDbReader(MultiShardConnection conn, String tsql)
      throws MultiShardAggregateException {
    MultiShardStatement cmd = conn.createCommand();
    cmd.setCommandText(tsql);
    cmd.setExecutionOptions(MultiShardExecutionOptions.IncludeShardNameColumn);
    return cmd.executeQuery();
  }

  /**
   * Helper that grabs a MultiShardResultSet based on a MultiShardConnection and a tsql string to
   * execute. This is different from the GetShardedDbReader method in that it uses
   * ExecuteReaderAsync() API under the hood and is cancellable.
   *
   * @param conn The MultiShardConnection to use to get the command/reader.
   * @param tsql The tsql to execute on the shards.
   * @return The MultiShardResultSet resulting from executing the given tsql on the given
   * connection.
   */
  private MultiShardResultSet GetShardedDbReaderAsync(MultiShardConnection conn, String tsql)
      throws Exception {
    MultiShardStatement cmd = conn.createCommand();
    cmd.setCommandText(tsql);
    cmd.setExecutionOptions(MultiShardExecutionOptions.IncludeShardNameColumn);
    return cmd.executeQueryAsync().call();
  }

  private MultiShardResultSet GetShardedDbReader(MultiShardConnection conn, String tsql,
      boolean includeShardName) throws MultiShardAggregateException {
    MultiShardStatement cmd = conn.createCommand();
    cmd.setCommandText(tsql);
    cmd.setExecutionOptions(includeShardName ? MultiShardExecutionOptions.IncludeShardNameColumn
        : MultiShardExecutionOptions.None);
    cmd.setExecutionPolicy(MultiShardExecutionPolicy.PartialResults);
    return cmd.executeQuery(CommandBehavior.Default);
  }

  /**
   * Helper method that sets up a MultiShardResultSet based on the given ResultSets so that
   * the MultiShardResultSet is ready to use.
   *
   * @param readers The ResultSets that will underlie this MultiShardResultSet.
   * @param exceptions Populated with any SchemaMismatchExceptions encountered while setting up the
   * MultiShardResultSet.
   * @return A new MultiShardResultSet object that is ready to use.
   *
   * Note that normally this setup and marking as complete would be hidden from the client (inside
   * the MultiShardStatement), but since we are doing unit testing at a lower level than the command
   * we need to perform it ourselves here.
   */
  private MultiShardResultSet GetMultiShardDataReaderFromResultSets(LabeledResultSet[] readers,
      ReferenceObjectHelper<List<MultiShardSchemaMismatchException>> exceptions) {
    exceptions.argValue = new ArrayList<>();

    MultiShardResultSet sdr = new MultiShardResultSet(Arrays.asList(readers));

    for (MultiShardException exception : sdr.getMultiShardExceptions()) {
      exceptions.argValue.add((MultiShardSchemaMismatchException) exception);
    }

    return sdr;
  }
}
