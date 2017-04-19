package com.microsoft.azure.elasticdb.shard.mapmanager.unittests;

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryBehavior;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerCreateMode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerFactory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerLoadPolicy;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import org.junit.*;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.*;

/**
 * Tests related to ShardMapManagerFactory class and it's methods.
 */
public class ShardMapManagerFactoryTests {
    /**
     * Initializes common state for tests in this class.
     *
     * @throws SQLServerException
     */
    @BeforeClass
    public static void shardMapManagerFactoryTestsInitialize() throws SQLServerException {
        SQLServerConnection conn = null;
        try {
            conn = (SQLServerConnection) DriverManager.getConnection(Globals.ShardMapManagerTestConnectionString);
            // Create ShardMapManager database
            try (Statement stmt = conn.createStatement()) {
                String query = String.format(Globals.CreateDatabaseQuery, Globals.ShardMapManagerDatabaseName);
                stmt.executeUpdate(query);
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            /**
             * Testing TryGetSqlShardMapManager failure case here instead of in TryGetShardMapManager_Fail()
             There is no method to cleanup GSM objects, so if some other test runs in lab before
             TryGetShardMapManager_Fail, then this call will actually suceed as it will find earlier SMM structures.
             Calling it just after creating database makes sure that GSM does not exist.
             Other options were to recreate SMM database in tests (this will increase test duration) or
             delete storage structures (t-sql delete) in the test which is not very clean solution.
             */
            ShardMapManager smm = null;
            ReferenceObjectHelper<ShardMapManager> smmref = new ReferenceObjectHelper<ShardMapManager>(smm);
            boolean lookupSmm = ShardMapManagerFactory.TryGetSqlShardMapManager(Globals.ShardMapManagerConnectionString, ShardMapManagerLoadPolicy.Eager, RetryBehavior.getDefaultRetryBehavior(), smmref);
            assertFalse(lookupSmm);
        } catch (Exception e) {
            System.out.printf("Failed to connect to SQL database with connection string:", e.getMessage());
        } finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
    }

    /**
     * Cleans up common state for the all tests in this class.
     */
    @AfterClass
    public static void shardMapManagerFactoryTestsCleanup() throws SQLServerException {
        SQLServerConnection conn = null;
        try {
            conn = (SQLServerConnection) DriverManager.getConnection(Globals.ShardMapManagerTestConnectionString);
            // Create ShardMapManager database
            try (Statement stmt = conn.createStatement()) {
                String query = String.format(Globals.DropDatabaseQuery, Globals.ShardMapManagerDatabaseName);
                stmt.executeUpdate(query);
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } catch (Exception e) {
            System.out.printf("Failed to connect to SQL database with connection string:", e.getMessage());
        } finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
    }

    /**
     * Initializes common state per-test.
     */
    @Before
    public void ShardMapManagerFactoryTestInitialize() {

    }

    /**
     * Get shard map manager, expects success.
     */
    @Test
    public void getShardMapManager_Success() {
        ShardMapManagerFactory.CreateSqlShardMapManager(Globals.ShardMapManagerConnectionString, ShardMapManagerCreateMode.ReplaceExisting);
        for (ShardMapManagerLoadPolicy loadPolicy : ShardMapManagerLoadPolicy.values()) {
            ShardMapManager smm1 = ShardMapManagerFactory.GetSqlShardMapManager(Globals.ShardMapManagerConnectionString, loadPolicy);
            assertNotNull(smm1);

            ShardMapManager smm2 = ShardMapManagerFactory.GetSqlShardMapManager(Globals.ShardMapManagerConnectionString, loadPolicy, RetryBehavior.getDefaultRetryBehavior());
            assertNotNull(smm2);
        }
    }

    /**
     * Tries to get shard map manager, expects success.
     */
    @SuppressWarnings("unused")
    @Test
    public void tryGetShardMapManager_Success() {
        ShardMapManagerFactory.CreateSqlShardMapManager(Globals.ShardMapManagerConnectionString, ShardMapManagerCreateMode.ReplaceExisting);
        for (ShardMapManagerLoadPolicy loadPolicy : ShardMapManagerLoadPolicy.values()) {
            ShardMapManager smm = null;
            ReferenceObjectHelper<ShardMapManager> smmref = new ReferenceObjectHelper<ShardMapManager>(smm);
            boolean success;

            success = ShardMapManagerFactory.TryGetSqlShardMapManager(Globals.ShardMapManagerConnectionString, loadPolicy, smmref);
            assertTrue(success);
            assertNotNull(smmref.argValue);

            success = ShardMapManagerFactory.TryGetSqlShardMapManager(Globals.ShardMapManagerConnectionString, loadPolicy, RetryBehavior.getDefaultRetryBehavior(), smmref);
            assertTrue(success);
            assertNotNull(smmref.argValue);
        }
    }

    /**
     * Tries to get shard map manager, expects failure.
     */
    @Test(expected = IllegalArgumentException.class)
    public void tryGetShardMapManager_Fail() {
        ShardMapManagerFactory.CreateSqlShardMapManager(Globals.ShardMapManagerConnectionString, ShardMapManagerCreateMode.ReplaceExisting);

        ShardMapManager smm = null;
        ReferenceObjectHelper<ShardMapManager> smmref = new ReferenceObjectHelper<ShardMapManager>(smm);
        boolean success = false;

        success = ShardMapManagerFactory.TryGetSqlShardMapManager(Globals.ShardMapManagerConnectionString, ShardMapManagerLoadPolicy.Eager, null, smmref);
        assertFalse(success);
        assertNull(smmref.argValue);
    }

    /**
     * Cleans up common state per-test.
     */
    @After
    public void ShardMapManagerFactoryTestCleanup() {
    }


}
