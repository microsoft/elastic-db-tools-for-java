package com.microsoft.azure.elasticdb.query.unittests;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.FutureTask;

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

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

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
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;

/**
 * Very basic unit tests for the MultiShardResultSet class. Just enough to ensure that simple scenarios working as expected. Purpose: Basic unit
 * testing for the MultiShardResultSet class. Will integrate with build at a later date. Notes: Aim is to integrate this within a broader cleint-side
 * wrapper framework. As a result, unit testing will likely be relatively significantly restructured once we have the rest of the wrapper classes in
 * place. NOTE: Unit tests currently assume that a sql server instance is accessible on localhost. NOTE: Unit tests will blow away and recreate
 * databases called Test1, Test2, and Test3. Should change these database names to guids at some point, but deferring that until our unit testing (and
 * functional testing) framework is more settled. NOTE: Unit tests will blow away and recreate a login called TestUser and grant it "control server"
 * permissions. Will likely need to revisit this at some point in the future.
 */
public class MultiShardResultSetTests {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private Connection conn1;
    private Connection conn2;
    private Connection conn3;
    /**
     * Handle on conn1, conn2 and conn3.
     */
    private MultiShardConnection shardConnection;

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
        // For now I have hardcoded the server location and database names. A better approach would be
        // to make the server location configurable and the database names be guids.
        // Not the top priority right now, though.
        MultiShardTestUtils.dropAndCreateDatabases();
        MultiShardTestUtils.createAndPopulateTables();
    }

    /**
     * Blow away our three test databases that we drove the tests off of. Doing this so that we don't leave objects littered around.
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
        shardConnection = new MultiShardConnection(MultiShardTestUtils.MULTI_SHARD_CONN_STRING, shards.toArray(new Shard[shards.size()]));

        List<Pair<ShardLocation, Connection>> shardConnections = shardConnection.getShardConnections();

        conn1 = shardConnections.get(0).getRight();
        conn2 = shardConnections.get(1).getRight();
        conn3 = shardConnections.get(2).getRight();
    }

    /**
     * Close our connections to each test database after each test.
     */
    @After
    public final void myTestCleanup() {
        for (Pair<ShardLocation, Connection> conn : shardConnection.getShardConnections()) {
            try {
                conn.getRight().close();
            }
            catch (SQLException e) {
                Assert.fail(e.getMessage());
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
        String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field" + " FROM ConsistentShardedTable";

        try (MultiShardResultSet sdr = getShardedDbReader(shardConnection, selectSql)) {
            if (sdr.next()) {
                Assert.assertEquals("Expected 9 rows in the result set", 9, sdr.getRowCount());

                int recordsRetrieved = 0;
                while (sdr.next()) {
                    recordsRetrieved++;
                    String dbNameField = sdr.getString(1);
                    int testIntField = sdr.getInt(2);
                    long testBigIntField = sdr.getLong(3);
                    String shardIdPseudoColumn = sdr.getLocation();
                    String logRecord = String.format(
                            "RecordRetrieved: dbNameField: %1$s, TestIntField: %2$s,"
                                    + " TestBigIntField: %3$s, shardIdPseudoColumnField: %4$s, RecordCount: %5$s",
                            dbNameField, testIntField, testBigIntField, shardIdPseudoColumn, recordsRetrieved);
                    log.info(logRecord);
                }
                assert recordsRetrieved == 9;
            }
        }
        catch (SQLException | MultiShardAggregateException e) {
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
        String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field" + " FROM ConsistentShardedTable";
        boolean[] pseudoColumnPresentOptions = new boolean[2];
        pseudoColumnPresentOptions[0] = true;
        pseudoColumnPresentOptions[1] = false;

        for (boolean pseudoColumnPresent : pseudoColumnPresentOptions) {
            LabeledResultSet[] readers = new LabeledResultSet[3];
            readers[0] = getReader(conn1, selectSql, "Test0");
            readers[1] = getReader(conn2, selectSql, "Test1");
            readers[2] = getReader(conn3, selectSql, "Test2");

            List<MultiShardSchemaMismatchException> exceptions;

            ReferenceObjectHelper<List<MultiShardSchemaMismatchException>> tempRefExceptions = new ReferenceObjectHelper<>(null);
            try (MultiShardResultSet sdr = getMultiShardDataReaderFromResultSets(readers, tempRefExceptions, pseudoColumnPresent)) {
                exceptions = tempRefExceptions.argValue;
                assert 0 == exceptions.size();

                int recordsRetrieved = 0;

                assert 3 == sdr.getMetaData().getColumnCount();

                while (sdr.next()) {
                    recordsRetrieved++;
                    sdr.getLong(3);
                    sdr.getInt(2);
                    sdr.getString(1);

                    String shardIdPseudoColumn = sdr.getLocation();
                    if (!pseudoColumnPresent && !Objects.equals(shardIdPseudoColumn, "")) {
                        Assert.fail("Should not have been able to pull the pseudo column.");
                    }
                    if (pseudoColumnPresent && Objects.equals(shardIdPseudoColumn, "")) {
                        Assert.fail("Should have been able to pull the pseudo column.");
                    }
                }
                sdr.close();
                assert recordsRetrieved == 9;
            }
            catch (Exception e) {
                Assert.fail(e.getMessage());
            }
        }
    }

    /**
     * Check that we can handle empty result sets interspersed with non-empty result sets as expected.
     */
    //@Test
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
        readers[0] = getReader(conn1, selectSql, "Test0");
        readers[1] = getReader(conn2, selectSql, "Test1");
        readers[2] = getReader(conn3, selectSql, "Test2");

        List<MultiShardSchemaMismatchException> exceptions;
        ReferenceObjectHelper<List<MultiShardSchemaMismatchException>> tempRefExceptions = new ReferenceObjectHelper<>(null);
        try (MultiShardResultSet sdr = getMultiShardDataReaderFromResultSets(readers, tempRefExceptions)) {
            exceptions = tempRefExceptions.argValue;
            assert 0 == exceptions.size();

            int recordsRetrieved = 0;
            while (sdr.next()) {
                recordsRetrieved++;
            }

            sdr.close();

            assert recordsRetrieved == 6;
        }
        catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Check that we can handle non-empty result sets interspersed with empty result sets as expected.
     */
    //@Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public final void testOuterResultsEmptyOnSelect() throws SQLException {
        // What we're doing:
        // Grab all rows from each test database that satisfy a particular predicate
        // (there should be 0 from db1 and db3 and 3 from db2).
        // Load them into a MultiShardResultSet.
        // Iterate through the rows and make sure that we have 3 rows.
        String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field" + " FROM ConsistentShardedTable WHERE dbNameField='Test1'";
        LabeledResultSet[] readers = new LabeledResultSet[3];
        readers[0] = getReader(conn1, selectSql, "Test0");
        readers[1] = getReader(conn2, selectSql, "Test1");
        readers[2] = getReader(conn3, selectSql, "Test2");

        List<MultiShardSchemaMismatchException> exceptions;

        ReferenceObjectHelper<List<MultiShardSchemaMismatchException>> tempRefExceptions = new ReferenceObjectHelper<>(null);
        try (MultiShardResultSet sdr = getMultiShardDataReaderFromResultSets(readers, tempRefExceptions)) {
            exceptions = tempRefExceptions.argValue;
            assert 0 == exceptions.size();

            int recordsRetrieved = 0;
            while (sdr.next()) {
                recordsRetrieved++;
            }

            sdr.close();

            assert recordsRetrieved == 3;
        }
        catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Validate basic ReadAsync behavior.
     */
    @Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public final void testReadAsync() throws SQLException {
        LabeledResultSet[] readers = new LabeledResultSet[1];
        readers[0] = getReader(conn1, "select 1", "Test0");
        int numRowsRead = 0;

        try (MultiShardResultSet sdr = new MultiShardResultSet(Arrays.asList(readers))) {
            while (sdr.next()) {
                numRowsRead++;
            }
        }
        catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertEquals("ReadAsync didn't return the expeceted number of rows.", 1, numRowsRead);
    }

    /**
     * Check that we do not stop responding when trying to read after adding null readers.
     */
    @Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public final void testReadFromNullReader() throws SQLException {
        // The code below exposes a flaw in our current implementation related to
        // CompleteResults semantics and the internal c-tor. The flaw does not
        // leak out to customers because the MultiShardStatement object manages the
        // necessary logic, but we need to patch the flaw so it doesn't end up
        // inadvertently leaking out to customers.
        // See VSTS 2616238 (i believe). Philip will be modofying logic and
        // augmenting tests to deal with this issue.

        // Pass a null reader and verify that read goes through and does not terminate.
        LabeledResultSet[] readers = new LabeledResultSet[2];
        readers[0] = getReader(conn1, "select 1", "Test0");
        readers[1] = null;

        try (MultiShardResultSet sdr = new MultiShardResultSet(Arrays.asList(readers))) {
            FutureTask task = new FutureTask<>(() -> {
                int count = 0;
                while (sdr.next()) {
                    count++;
                }
                return count;
            });

            task.run();
            Thread.sleep(500);

            Assert.assertTrue("Read did not respond on the garbage reader.", task.isDone());
        }
        catch (SQLException | InterruptedException e) {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Check that we do not stop responding when trying to read after adding a reader with an exception.
     */
    @Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public final void testReadFromReaderWithException() throws SQLException {
        // The code below exposes a flaw in our current implementation related to
        // CompleteResults semantics and the internal c-tor. The flaw does not
        // leak out to customers because the MultiShardStatement object manages the
        // necessary logic, but we need to patch the flaw so it doesn't end up
        // inadvertently leaking out to customers.
        // See VSTS 2616238 (i believe). Philip will be modofying logic and
        // augmenting tests to deal with this issue.

        // Pass a reader with an exception and verify that read goes through and does not terminate.
        LabeledResultSet[] readers = new LabeledResultSet[2];
        readers[0] = getReader(conn1, "select 1", "Test0");
        readers[1] = new LabeledResultSet(new MultiShardException(), new ShardLocation("foo", "bar"), conn2.createStatement());

        try (MultiShardResultSet sdr = new MultiShardResultSet(Arrays.asList(readers))) {
            FutureTask task = new FutureTask<>(() -> {
                while (sdr.next()) {
                }
                return 0;
            });

            task.run();
            Thread.sleep(500);

            Assert.assertTrue("Read did not respond on the garbage reader.", task.isDone());
        }
        catch (SQLException | InterruptedException e) {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Check that we throw as expected when trying to add a LabeledResultSet with a null ResultSet underneath.
     */
    @Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public final void testAddLabeledResultSetWithNullResultSet() throws SQLException {
        // What we're doing:
        // Set up a new sharded reader
        // Add two readers to it.
        // Try to add a third reader to it that has a null ResultSet underneath.
        // Verify that we threw as expected.
        String selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field" + " FROM ConsistentShardedTable WHERE Test_int_Field = 876";
        LabeledResultSet[] readers = new LabeledResultSet[3];
        readers[0] = getReader(conn1, selectSql, "Test0");
        readers[1] = getReader(conn2, selectSql, "Test1");

        SqlConnectionStringBuilder str = new SqlConnectionStringBuilder(conn3.getMetaData().getURL());
        ResultSet res = null;
        try {
            readers[2] = new LabeledResultSet(res, new ShardLocation(str.getDataSource(), "Test2"), conn3.createStatement());
        }
        catch (IllegalArgumentException ex) {
            assert ex.getMessage().equals("resultSet");
        }
    }

    /**
     * Check that we can iterate through the result sets as expected comparing all the values returned from the getters plus some of the properties.
     * Check everythign both with and without the $ShardName pseudo column.
     */
    @Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public final void testGettersPositiveCases() {
        testGettersPositiveCasesHelper(true);
        testGettersPositiveCasesHelper(false);
    }

    /**
     * Check that we can iterate through the result sets as expected comparing all the values returned from the getters plus some of the properties.
     */
    private void testGettersPositiveCasesHelper(boolean includeShardNamePseudoColumn) {
        // What we're doing:
        // Grab all rows from each test database.
        // Load them into a MultiShardResultSet.
        // Iterate through the rows and make sure that we have 9 total.
        // Also iterate through all columns and make sure that the getters that should work do work.
        List<MultiShardTestCaseColumn> toCheck = MultiShardTestCaseColumn.getDefinedColumns();

        for (MultiShardTestCaseColumn curCol : toCheck) {
            String selectSql = String.format("SELECT %1$s FROM ConsistentShardedTable", curCol.getTestColumnName());

            try (MultiShardResultSet sdr = getShardedDbReader(shardConnection, selectSql, includeShardNamePseudoColumn)) {
                int recordsRetrieved = 0;
                log.info("Starting to get records");
                while (sdr.next()) {
                    assert 1 == sdr.getMetaData().getColumnCount();

                    recordsRetrieved++;

                    // Do verification for the test column.
                    checkColumnName(sdr, curCol, 1);
                    verifyAllGettersPositiveCases(sdr, curCol, 1);

                    // Then also verify PseudoColumn if necessary.
                    if (includeShardNamePseudoColumn) {
                        assert !StringUtilsLocal.isNullOrEmpty(sdr.getLocation());
                    }
                }

                sdr.close();

                assert recordsRetrieved == 9;
            }
            catch (Exception e) {
                e.printStackTrace();
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
        try (MultiShardResultSet sdr = getShardedDbReader(shardConnection, selectSql)) {
            try {
                sdr.getInt(1);
                Assert.fail(String.format("Should have hit %1$s.", IllegalStateException.class));
            }
            catch (IllegalStateException ex) {
                assert ex.getClass().equals(IllegalStateException.class);
            }

            while (sdr.next()) {
                sdr.getInt(1);
            }

            try {
                sdr.getInt(1);
                Assert.fail(String.format("Should have hit %1$s.", IllegalStateException.class));
            }
            catch (IllegalStateException ex) {
                assert ex.getClass().equals(IllegalStateException.class);
            }

            sdr.close();

            try {
                sdr.getInt(1);
                Assert.fail(String.format("Should have hit %1$s.", MultiShardResultSetClosedException.class));
            }
            catch (MultiShardResultSetClosedException ex) {
                assert ex.getClass().equals(MultiShardResultSetClosedException.class);
            }

            // And try to close it again.
            sdr.close();
        }
        catch (SQLException | MultiShardAggregateException e) {
            Assert.fail(e.getMessage());
        }
    }

    private FutureTask getLowRowTask(boolean rollback,
            String dbToUpdate) {
        return new FutureTask<>(() -> {
            conn2.setAutoCommit(false);
            try (Statement cmd = conn2.createStatement()) {
                String query = String.format("UPDATE ConsistentShardedTable SET dbNameField='TestN'" + " WHERE dbNameField='%1$s'", dbToUpdate);

                // This will X-lock all rows in the second shard.
                cmd.execute(query);

                if (rollback) {
                    conn2.rollback();
                }
            }
            return 0;
        });
    }

    private void checkColumnName(MultiShardResultSet reader,
            MultiShardTestCaseColumn column,
            int ordinal) throws SQLException {
        assert Objects.equals(column.getTestColumnName(), reader.getMetaData().getColumnName(ordinal));
        assert ordinal == reader.findColumn(column.getTestColumnName());
    }

    private void verifyAllGettersPositiveCases(MultiShardResultSet reader,
            MultiShardTestCaseColumn column,
            int ordinal) throws SQLException {
        // General pattern here:
        // Grab the value through the regular getter (Object type) and type specific getter using both
        // column index and column name.
        // Then verify that the field types are as we expect.

        // These are indexes into our type array.
        int objectResultOrdinal = 0;
        int objectResultName = 1;
        int typeResultOrdinal = 2;
        int typeResultName = 3;
        Object[] results = new Object[4];

        String colName = column.getTestColumnName();
        int colType = column.getDbType();

        // These two we can pull consistently for all fields. The rest have type specific getters.
        results[objectResultOrdinal] = reader.getObject(ordinal);
        results[objectResultName] = reader.getObject(colName);

        switch (colType) {
            case Types.BINARY:
                // SQL Type: binary
            case Types.VARBINARY:
                // SQL Type: varbinary
            case Types.LONGVARBINARY:
                // SQL Type: image
                results[typeResultOrdinal] = reader.getBytes(ordinal);
                results[typeResultName] = reader.getBytes(colName);

                byte[] byteArray = (byte[]) results[0];
                for (Object curObject : results) {
                    Assert.assertArrayEquals(String.format("Expected %1$s to be equal to %2$s", Arrays.toString(byteArray), curObject), byteArray,
                            (byte[]) curObject);
                }

                assert reader.getMetaData().getColumnType(ordinal) == colType;
                return;

            case Types.CHAR:
                // SQL Type: char
            case Types.VARCHAR:
                // SQL Type: varchar
            case Types.LONGVARCHAR:
                // SQL Type: text
            case Types.NCHAR:
                // SQL Type: nchar
            case Types.NVARCHAR:
                // SQL Type: nvarchar
            case Types.LONGNVARCHAR:
                // SQL Type: ntext
                results[typeResultOrdinal] = reader.getString(ordinal);
                results[typeResultName] = reader.getString(colName);
                assertAllAreEqual(results);

                assert reader.getMetaData().getColumnType(ordinal) == colType;
                return;

            case Types.BIT:
                // SQL Type: bit
                results[typeResultOrdinal] = Boolean.valueOf(reader.getBoolean(ordinal));
                results[typeResultName] = Boolean.valueOf(reader.getBoolean(colName));
                assertAllAreEqual(results);

                assert reader.getMetaData().getColumnType(ordinal) == colType;
                return;

            case Types.TINYINT:
                // SQL Type: tinyint
            case Types.SMALLINT:
                // SQL Type: smallint
                results[typeResultOrdinal] = reader.getShort(ordinal);
                results[typeResultName] = reader.getShort(colName);
                assertAllAreEqual(results);

                assert reader.getMetaData().getColumnType(ordinal) == colType;
                return;

            case Types.INTEGER:
                // SQL Type: int
                results[typeResultOrdinal] = reader.getInt(ordinal);
                results[typeResultName] = reader.getInt(colName);
                assertAllAreEqual(results);

                assert reader.getMetaData().getColumnType(ordinal) == colType;
                return;

            case Types.REAL:
                // SQL Type: real
                results[typeResultOrdinal] = reader.getFloat(ordinal);
                results[typeResultName] = reader.getFloat(colName);
                assertAllAreEqual(results);

                assert reader.getMetaData().getColumnType(ordinal) == colType;
                return;

            case Types.BIGINT:
                // SQL Type: bigint
                results[typeResultOrdinal] = reader.getLong(ordinal);
                results[typeResultName] = reader.getLong(colName);
                assertAllAreEqual(results);

                assert reader.getMetaData().getColumnType(ordinal) == colType;
                return;

            case Types.DECIMAL:
                // SQL Type: decimal
            case microsoft.sql.Types.MONEY:
                // SQL Type: money
            case microsoft.sql.Types.SMALLMONEY:
                // SQL Type: smallmoney
                results[typeResultOrdinal] = reader.getBigDecimal(ordinal);
                results[typeResultName] = reader.getBigDecimal(colName);
                assertAllAreEqual(results);

                assert reader.getMetaData().getColumnType(ordinal) == Types.DECIMAL;
                return;

            case Types.NUMERIC:
                // SQL Type: numeric
                results[typeResultOrdinal] = reader.getBigDecimal(ordinal);
                results[typeResultName] = reader.getBigDecimal(colName);
                assertAllAreEqual(results);

                assert reader.getMetaData().getColumnType(ordinal) == colType;
                return;

            case Types.DOUBLE:
                // SQL Type: float
                results[typeResultOrdinal] = reader.getDouble(ordinal);
                results[typeResultName] = reader.getDouble(colName);
                assertAllAreEqual(results);

                assert reader.getMetaData().getColumnType(ordinal) == colType;
                return;

            case Types.DATE:
                // SQL Type: date
                results[typeResultOrdinal] = reader.getDate(ordinal);
                results[typeResultName] = reader.getDate(colName);
                assertAllAreEqual(results);

                assert reader.getMetaData().getColumnType(ordinal) == colType;
                return;

            case microsoft.sql.Types.DATETIME:
                // SQL Type: datetime2
            case microsoft.sql.Types.SMALLDATETIME:
                // SQL Type: smalldatetime
                results[typeResultOrdinal] = reader.getTimestamp(ordinal);
                results[typeResultName] = reader.getTimestamp(colName);
                assertAllAreEqual(results);

                assert reader.getMetaData().getColumnType(ordinal) == Types.TIMESTAMP;
                return;

            case microsoft.sql.Types.DATETIMEOFFSET:
                // SQL Type: datetimeoffset
                results[typeResultOrdinal] = reader.getDateTimeOffset(ordinal);
                results[typeResultName] = reader.getDateTimeOffset(colName);
                assertAllAreEqual(results);

                assert reader.getMetaData().getColumnType(ordinal) == colType;
                return;

            case Types.TIME:
                // SQL Type: time
                results[typeResultOrdinal] = reader.getTime(ordinal);
                results[typeResultName] = reader.getTime(colName);
                assertAllAreEqual(results);

                assert reader.getMetaData().getColumnType(ordinal) == colType;
                return;

            case Types.TIMESTAMP:
                // SQL Type: timestamp
                results[typeResultOrdinal] = reader.getBytes(ordinal);
                results[typeResultName] = reader.getBytes(colName);

                byte[] bytes = (byte[]) results[0];
                for (Object curObject : results) {
                    Assert.assertArrayEquals(String.format("Expected %1$s to be equal to %2$s", Arrays.toString(bytes), curObject), bytes,
                            (byte[]) curObject);
                }

                assert reader.getMetaData().getColumnType(ordinal) == Types.BINARY;
                return;

            case microsoft.sql.Types.GUID:
                // SQL Type: uniqueidentifier
                results[typeResultOrdinal] = reader.getUniqueIdentifier(ordinal);
                results[typeResultName] = reader.getUniqueIdentifier(colName);
                assertAllAreEqual(results);

                assert reader.getMetaData().getColumnType(ordinal) == Types.CHAR;
                return;

            default:
                throw new IllegalArgumentException(Integer.toString(colType));
        }
    }

    private void assertAllAreEqual(Object[] toCheck) {
        Object baseline = toCheck[0];
        for (Object curObject : toCheck) {
            Assert.assertEquals(String.format("Expected %1$s to be equal to %2$s", baseline, curObject), baseline, curObject);
        }
    }

    private <T> void performArrayComparison(T[] first,
            T[] second) {
        assert first.length == second.length;
        for (int i = 0; i < first.length; i++) {
            assert first[i] == second[i];
        }
    }

    /**
     * Gets a ResultSet by executing the passed in t-sql over the passed in connection.
     *
     * @param conn
     *            Connection to the database we wish to execute the t-sql against.
     * @param tsql
     *            The t-sql to execute.
     * @return The ResultSet obtained by executing the passed in t-sql over the passed in connection.
     */
    private LabeledResultSet getReader(Connection conn,
            String tsql,
            String dbName) throws SQLException {
        String connStr = conn.getMetaData().getURL();
        SqlConnectionStringBuilder connStrBldr = new SqlConnectionStringBuilder(connStr);
        if (conn.isClosed()) {
            conn = DriverManager.getConnection(connStr);
        }
        Statement cmd = conn.createStatement();
        ResultSet sdr = cmd.executeQuery(tsql);

        return new LabeledResultSet(sdr, new ShardLocation(connStrBldr.getDataSource(), dbName), cmd);
    }

    /**
     * Helper that grabs a MultiShardResultSet based on a MultiShardConnection and a tsql string to execute.
     *
     * @param conn
     *            The MultiShardConnection to use to get the command/reader.
     * @param tsql
     *            The tsql to execute on the shards.
     * @return The MultiShardResultSet resulting from executing the given tsql on the given connection.
     */
    private MultiShardResultSet getShardedDbReader(MultiShardConnection conn,
            String tsql) throws MultiShardAggregateException {
        MultiShardStatement cmd = conn.createCommand();
        cmd.setCommandText(tsql);
        cmd.setExecutionOptions(MultiShardExecutionOptions.IncludeShardNameColumn);
        return cmd.executeQuery();
    }

    private MultiShardResultSet getShardedDbReader(MultiShardConnection conn,
            String tsql,
            boolean includeShardName) throws MultiShardAggregateException {
        MultiShardStatement cmd = conn.createCommand();
        cmd.setCommandText(tsql);
        cmd.setExecutionOptions(includeShardName ? MultiShardExecutionOptions.IncludeShardNameColumn : MultiShardExecutionOptions.None);
        cmd.setExecutionPolicy(MultiShardExecutionPolicy.PartialResults);
        return cmd.executeQuery(CommandBehavior.Default);
    }

    /**
     * Helper that grabs a MultiShardResultSet based on a MultiShardConnection and a tsql string to execute. This is different from the
     * GetShardedDbReader method in that it uses ExecuteReaderAsync() API under the hood and is cancellable.
     *
     * @param conn
     *            The MultiShardConnection to use to get the command/reader.
     * @param tsql
     *            The tsql to execute on the shards.
     * @return The MultiShardResultSet resulting from executing the given tsql on the given connection.
     */
    private MultiShardResultSet getShardedDbReaderAsync(MultiShardConnection conn,
            String tsql) throws Exception {
        MultiShardStatement cmd = conn.createCommand();
        cmd.setCommandText(tsql);
        cmd.setExecutionOptions(MultiShardExecutionOptions.IncludeShardNameColumn);
        return cmd.executeQueryAsync().call();
    }

    /**
     * Helper method that sets up a MultiShardResultSet based on the given ResultSets so that the MultiShardResultSet is ready to use.
     *
     * @param readers
     *            The ResultSets that will underlie this MultiShardResultSet.
     * @param exceptions
     *            Populated with any SchemaMismatchExceptions encountered while setting up the MultiShardResultSet.
     * @return A new MultiShardResultSet object that is ready to use.
     *
     *         Note that normally this setup and marking as complete would be hidden from the client (inside the MultiShardStatement), but since we
     *         are doing unit testing at a lower level than the command we need to perform it ourselves here.
     */
    private MultiShardResultSet getMultiShardDataReaderFromResultSets(LabeledResultSet[] readers,
            ReferenceObjectHelper<List<MultiShardSchemaMismatchException>> exceptions) {
        exceptions.argValue = new ArrayList<>();

        MultiShardResultSet sdr = new MultiShardResultSet(Arrays.asList(readers));

        for (MultiShardException exception : sdr.getMultiShardExceptions()) {
            exceptions.argValue.add((MultiShardSchemaMismatchException) exception);
        }

        return sdr;
    }

    /**
     * Helper method that sets up a MultiShardResultSet based on the given ResultSets so that the MultiShardResultSet is ready to use.
     *
     * @param readers
     *            The ResultSets that will underlie this MultiShardResultSet.
     * @param exceptions
     *            Populated with any SchemaMismatchExceptions encountered while setting up the MultiShardResultSet.
     * @return A new MultiShardResultSet object that is ready to use.
     *
     *         Note that normally this setup and marking as complete would be hidden from the client (inside the MultiShardStatement), but since we
     *         are doing unit testing at a lower level than the command we need to perform it ourselves here.
     */
    private MultiShardResultSet getMultiShardDataReaderFromResultSets(LabeledResultSet[] readers,
            ReferenceObjectHelper<List<MultiShardSchemaMismatchException>> exceptions,
            boolean includePseudoColumn) {
        exceptions.argValue = new ArrayList<>();

        MultiShardResultSet sdr = new MultiShardResultSet(Arrays.asList(readers));

        if (includePseudoColumn) {
            sdr.getResults().forEach(r -> r.setShardLabel(r.getShardLocation().getDatabase()));
        }

        for (MultiShardException exception : sdr.getMultiShardExceptions()) {
            exceptions.argValue.add((MultiShardSchemaMismatchException) exception);
        }

        return sdr;
    }
}
