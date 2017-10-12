package com.microsoft.azure.elasticdb.shard.unittests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.microsoft.azure.elasticdb.shard.base.MappingStatus;
import com.microsoft.azure.elasticdb.shard.base.PointMapping;
import com.microsoft.azure.elasticdb.shard.base.PointMappingCreationInfo;
import com.microsoft.azure.elasticdb.shard.base.Range;
import com.microsoft.azure.elasticdb.shard.base.RangeMapping;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.category.ExcludeFromGatedCheckin;
import com.microsoft.azure.elasticdb.shard.map.ListShardMap;
import com.microsoft.azure.elasticdb.shard.map.RangeShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerCreateMode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerFactory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerLoadPolicy;
import com.microsoft.azure.elasticdb.shard.recovery.MappingDifferenceResolution;
import com.microsoft.azure.elasticdb.shard.recovery.MappingLocation;
import com.microsoft.azure.elasticdb.shard.recovery.RecoveryManager;
import com.microsoft.azure.elasticdb.shard.recovery.RecoveryToken;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.Version;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationLocal;

public class RecoveryManagerTests {

    /**
     * Sharded databases to create for the test.
     */
    private static String[] shardDBs = new String[] {"shard1", "shard2"};

    /**
     * GSM table names used in cleanup function.
     */
    private static String[] gsmTables = new String[] {"__ShardManagement.ShardMappingsGlobal", "__ShardManagement.ShardsGlobal",
            "__ShardManagement.ShardMapsGlobal", "__ShardManagement.OperationsLogGlobal"};

    /**
     * LSM table names used in cleanup function.
     */
    private static String[] lsmTables = new String[] {"__ShardManagement.ShardMappingsLocal", "__ShardManagement.ShardsLocal",
            "__ShardManagement.ShardMapsLocal"};

    /**
     * List shard map name.
     */
    private static String listShardMapName = "CustomersList";

    /**
     * Range shard map name.
     */
    private static String rangeShardMapName = "CustomersRange";

    /**
     * Helper function to create list and range shard maps.
     */
    private static void createShardMapsHelper() {
        // Create list shard map.
        ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

        ListShardMap<Integer> lsm = smm.createListShardMap(RecoveryManagerTests.listShardMapName, ShardKeyType.Int32);

        assert Objects.equals(RecoveryManagerTests.listShardMapName, lsm.getName());

        // Create range shard map.
        RangeShardMap<Integer> rsm = smm.createRangeShardMap(RecoveryManagerTests.rangeShardMapName, ShardKeyType.Int32);

        assert Objects.equals(RecoveryManagerTests.rangeShardMapName, rsm.getName());
    }

    /**
     * Helper function to clean SMM tables from all shards and GSM.
     */
    private static void cleanTablesHelper() throws SQLException {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);
            // Clean LSM tables.
            for (String dbName : RecoveryManagerTests.shardDBs) {
                for (String tableName : lsmTables) {
                    try (Statement stmt = conn.createStatement()) {
                        String query = String.format(Globals.CLEAN_DATABASE_QUERY, dbName, tableName);
                        stmt.executeUpdate(query);
                    }
                    catch (SQLException ex) {
                        ex.printStackTrace();
                    }
                }
            }

            // Clean GSM tables
            for (String tableName : gsmTables) {
                try (Statement stmt = conn.createStatement()) {
                    String query = String.format(Globals.CLEAN_DATABASE_QUERY, Globals.SHARD_MAP_MANAGER_DATABASE_NAME, tableName);
                    stmt.executeUpdate(query);
                }
                catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        }
        catch (Exception e) {
            System.out.printf("Failed to connect to SQL database with connection string: " + e.getMessage());
        }
        finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
    }

    /**
     * Initializes common state for tests in this class.
     */
    @BeforeClass
    public static void recoveryManagerTestsInitialize() throws SQLException {
        // Clear all connection pools.
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);
            // Create ShardMapManager database
            try (Statement stmt = conn.createStatement()) {
                String query = String.format(Globals.CREATE_DATABASE_QUERY, Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
                stmt.executeUpdate(query);
            }
            // Create shard databases
            for (int i = 0; i < RecoveryManagerTests.shardDBs.length; i++) {
                try (Statement stmt = conn.createStatement()) {
                    String query = String.format(Globals.DROP_DATABASE_QUERY, RecoveryManagerTests.shardDBs[i]);
                    stmt.executeUpdate(query);
                }

                try (Statement stmt = conn.createStatement()) {
                    String query = String.format(Globals.CREATE_DATABASE_QUERY, RecoveryManagerTests.shardDBs[i]);
                    stmt.executeUpdate(query);
                }
            }

            // Create shard map manager.
            ShardMapManagerFactory.createSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerCreateMode.ReplaceExisting);
        }
        catch (Exception e) {
            System.out.printf("Failed to connect to SQL database with connection string: " + e.getMessage());
        }
        finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
    }

    /**
     * Cleans up common state for the all tests in this class.
     */
    @AfterClass
    public static void recoveryManagerTestsCleanup() throws SQLException {
        // Clear all connection pools.
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);
            // Drop shard databases
            for (int i = 0; i < RecoveryManagerTests.shardDBs.length; i++) {
                try (Statement stmt = conn.createStatement()) {
                    String query = String.format(Globals.DROP_DATABASE_QUERY, RecoveryManagerTests.shardDBs[i]);
                    stmt.executeUpdate(query);
                }
            }

            // Drop shard map manager database
            try (Statement stmt = conn.createStatement()) {
                String query = String.format(Globals.DROP_DATABASE_QUERY, Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
                stmt.executeUpdate(query);
            }
        }
        catch (Exception e) {
            System.out.printf("Failed to connect to SQL database with connection string: " + e.getMessage());
        }
        finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
    }

    private void deleteAllMappingsFromGsm() throws SQLException {
        // Delete all mappings from GSM
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

            try (Statement stmt = conn.createStatement()) {
                String query = String.format("DELETE FROM %1$s.__ShardManagement.ShardMappingsGlobal", Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
                stmt.executeUpdate(query);
            }
        }
        catch (Exception e) {
            System.out.printf("Failed to connect to SQL database with connection string: " + e.getMessage());
        }
        finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
    }

    /**
     * Initializes common state per-test.
     */
    @Before
    public void shardMapperTestInitialize() {
        createShardMapsHelper();
    }

    /**
     * Cleans up common state per-test.
     */
    @After
    public void shardMapperTestCleanup() throws SQLException {
        cleanTablesHelper();
    }

    /**
     * Test Detach and Attach Shard Scenario. (This is just a stub for now.)
     */
    @Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public void testDetachAttachShard() {
        ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

        RangeShardMap<Integer> rsm = smm.getRangeShardMap(RecoveryManagerTests.rangeShardMapName, ShardKeyType.Int32);

        assert rsm != null;

        ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, RecoveryManagerTests.shardDBs[0]);

        Shard s = rsm.createShard(sl);

        assert s != null;

        // I am still not fully clear on the use case for AttachShard and DetachShard, but here's a
        // simple test validating that
        // they don't throw exceptions if they get called against themselves.
        RecoveryManager rm = new RecoveryManager(smm);
        rm.detachShard(sl);
        rm.attachShard(sl);
    }

    /**
     * Test that consistency detection works when there are no conflicts.
     */
    @Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public void testConsistencyDetectionAndViewingWithNoConflicts() {
        ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

        RangeShardMap<Integer> rsm = smm.getRangeShardMap(RecoveryManagerTests.rangeShardMapName, ShardKeyType.Int32);

        assert rsm != null;

        ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, RecoveryManagerTests.shardDBs[0]);

        Shard s = rsm.createShard(sl);

        assert s != null;

        RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);

        assert r1 != null;

        RecoveryManager rm = new RecoveryManager(smm);

        List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

        assertEquals("The test environment was not expecting more than one local shardMap.", 1, gs.size());

        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            assertEquals("An unexpected conflict was detected", 0, kvps.keySet().size());

            for (Map.Entry<ShardRange, MappingLocation> kvp : kvps.entrySet()) {
                assertEquals(
                        "An unexpected difference between global and local shard maps was detected."
                                + " This is likely a false positive and implies a bug in the detection code.",
                        MappingLocation.MappingInShardMapAndShard, kvp.getValue());
            }
        }
    }

    /**
     * Test that consistency detection works when there are only version conflicts.
     */
    @Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public void testConsistencyDetectionAndViewingWithVersionOnlyConflict() throws SQLException {
        ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

        RangeShardMap<Integer> rsm = smm.getRangeShardMap(RecoveryManagerTests.rangeShardMapName, ShardKeyType.Int32);

        assert rsm != null;

        // Make sure no other range mappings are floating around here.
        List<RangeMapping> rangeMappings = rsm.getMappings();
        for (RangeMapping rangeMapping : rangeMappings) {
            rsm.deleteMapping(rangeMapping);
        }

        ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, RecoveryManagerTests.shardDBs[0]);

        Shard s = rsm.createShard(sl);

        assert s != null;

        RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);

        assert r1 != null;

        // Corrupt the mapping id number on the global shardMap.

        Connection conn = null;
        try {
            conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

            try (Statement stmt = conn.createStatement()) {
                String query = String.format("update %1$s.__ShardManagement.ShardMappingsGlobal" + " set MappingId = newid()",
                        Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
                stmt.executeUpdate(query);
            }
        }
        catch (Exception e) {
            System.out.printf("Failed to connect to SQL database with connection string: " + e.getMessage());
        }
        finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }

        RecoveryManager rm = new RecoveryManager(smm);

        List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

        assertEquals("The test environment was not expecting more than one local shardMap.", 1, gs.size());

        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            assertEquals("An unexpected conflict was detected", 1, kvps.keySet().size());

            for (Map.Entry<ShardRange, MappingLocation> kvp : kvps.entrySet()) {
                assertEquals(
                        "An unexpected difference between global and local shard maps was detected."
                                + " This is likely a false positive and implies a bug in the detection code.",
                        MappingLocation.MappingInShardMapAndShard, kvp.getValue());
            }
        }
    }

    /**
     * Test that consistency detection works when the range in GSM is expanded while the LSM is left untouched.
     */
    @Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public void testConsistencyDetectionAndViewingWithWiderRangeInLsm() throws SQLException {
        ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

        RangeShardMap<Integer> rsm = smm.getRangeShardMap(RecoveryManagerTests.rangeShardMapName, ShardKeyType.Int32);

        assert rsm != null;

        ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, RecoveryManagerTests.shardDBs[0]);

        Shard s = rsm.createShard(sl);

        assert s != null;

        RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);

        assert r1 != null;

        // Corrupt the lsm by increasing the max range and decreasing min range. We should see two
        // ranges show up in the list of differences. The shared range
        // in the middle artificially has the same version number, so it should not register as a
        // conflicting range.

        Connection conn = null;
        try {
            conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

            try (Statement stmt = conn.createStatement()) {
                String query = "UPDATE shard1.__ShardManagement.ShardMappingsLocal" + " SET MinValue = MinValue - 1, MaxValue = MaxValue + 1";
                stmt.executeUpdate(query);
            }
        }
        catch (Exception e) {
            System.out.printf("Failed to connect to SQL database with connection string: " + e.getMessage());
        }
        finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }

        RecoveryManager rm = new RecoveryManager(smm);

        List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

        assertEquals("The test environment was not expecting more than one local shardMap.", 1, gs.size());

        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            assertEquals("The count of differences does not match the expected.", 2, kvps.keySet().size());

            for (Map.Entry<ShardRange, MappingLocation> kvp : kvps.entrySet()) {
                ShardRange range = kvp.getKey();
                MappingLocation mappingLocation = kvp.getValue();
                assertEquals("The ranges reported differed from those expected.", 1,
                        (int) range.getHigh().getValue() - (int) range.getLow().getValue());
                assertEquals(
                        "An unexpected difference between global and local shard maps was detected."
                                + " This is likely a false positive and implies a bug in the detection code.",
                        MappingLocation.MappingInShardOnly, mappingLocation);
            }
        }
    }

    /**
     * Test that consistency detection works when the range in GSM is expanded while the LSM is left untouched.
     */
    @Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public void testConsistencyDetectionAndViewingWithWiderRangeInGsm() throws SQLException {
        ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

        RangeShardMap<Integer> rsm = smm.getRangeShardMap(RecoveryManagerTests.rangeShardMapName, ShardKeyType.Int32);

        assert rsm != null;

        ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, RecoveryManagerTests.shardDBs[0]);

        Shard s = rsm.createShard(sl);

        assert s != null;

        RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);

        assert r1 != null;

        // Corrupt the gsm by increasing the max range and decreasing min range. We should see two
        // ranges show up in the list of differences. The shared range
        // in the middle artificially has the same version number, so it should not register as a
        // conflicting range.

        Connection conn = null;
        try {
            conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

            try (Statement stmt = conn.createStatement()) {
                String query = String.format(
                        "update %1$s.__ShardManagement.ShardMappingsGlobal" + " set MinValue = MinValue - 1, MaxValue = MaxValue + 1",
                        Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
                stmt.executeUpdate(query);
            }
        }
        catch (Exception e) {
            System.out.printf("Failed to connect to SQL database with connection string: " + e.getMessage());
        }
        finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }

        RecoveryManager rm = new RecoveryManager(smm);

        List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

        assertEquals("The test environment was not expecting more than one local shardMap.", 1, gs.size());

        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            assertEquals("The count of differences does not match the expected.", 2, kvps.keySet().size());
            for (Map.Entry<ShardRange, MappingLocation> kvp : kvps.entrySet()) {
                ShardRange range = kvp.getKey();
                MappingLocation mappingLocation = kvp.getValue();
                assertEquals("The ranges reported differed from those expected.", 1,
                        (int) range.getHigh().getValue() - (int) range.getLow().getValue());
                assertEquals(
                        "An unexpected difference between global and local shard maps was detected."
                                + " This is likely a false positive and implies a bug in the detection code.",
                        MappingLocation.MappingInShardMapOnly, mappingLocation);
            }
        }
    }

    /**
     * Test that consistency detection works the GSM is missing a range added to the LSM.
     */
    @Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public void testConsistencyDetectionAndViewingWithAdditionalRangeInLsm() throws SQLException {
        ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

        RangeShardMap<Integer> rsm = smm.getRangeShardMap(RecoveryManagerTests.rangeShardMapName, ShardKeyType.Int32);

        assert rsm != null;

        ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, RecoveryManagerTests.shardDBs[0]);

        Shard s = rsm.createShard(sl);
        assert s != null;

        RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);
        assert r1 != null;

        // Add a range to the gsm
        RangeMapping r2 = rsm.createRangeMapping(new Range(11, 20), s);
        assert r2 != null;

        // Now, delete the new range from the GSM
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

            try (Statement stmt = conn.createStatement()) {
                String query = String.format("delete from %1$s.__ShardManagement.ShardMappingsGlobal" + " where MinValue = 0x8000000B",
                        Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
                stmt.executeUpdate(query);
            }
        }
        catch (Exception e) {
            System.out.printf("Failed to connect to SQL database with connection string: " + e.getMessage());
        }
        finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }

        RecoveryManager rm = new RecoveryManager(smm);

        List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

        assertEquals("The test environment was not expecting more than one local shardMap.", 1, gs.size());

        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            assertEquals("The count of differences does not match the expected.", 1, kvps.keySet().size());
            for (Map.Entry<ShardRange, MappingLocation> kvp : kvps.entrySet()) {
                ShardRange range = kvp.getKey();
                MappingLocation mappingLocation = kvp.getValue();
                assertEquals("The range reported differed from that expected.", 20, (int) range.getHigh().getValue());
                assertEquals(
                        "An unexpected difference between global and local shard maps was detected."
                                + " This is likely a false positive and implies a bug in the detection code.",
                        MappingLocation.MappingInShardOnly, mappingLocation);
            }
        }
    }

    /**
     * Test that consistency detection works with some arbitrary point mappings.
     */
    @Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public void testConsistencyDetectionOnListMapping() throws SQLException {
        ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

        ListShardMap<Integer> rsm = smm.getListShardMap(RecoveryManagerTests.listShardMapName, ShardKeyType.Int32);

        assert rsm != null;

        ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, RecoveryManagerTests.shardDBs[0]);
        Shard s = rsm.createShard(sl);
        assert s != null;

        for (int i = 0; i < 5; i++) {
            PointMapping p = rsm.createPointMapping(2 * i, s);
            assert p != null;
        }

        // Now, delete some points from both, and change the version of a shared shard mapping in the
        // middle.
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

            try (Statement stmt = conn.createStatement()) {
                String query = String.format("delete from %1$s.__ShardManagement.ShardMappingsGlobal" + " where MinValue IN (0x80000000, 0x80000002)",
                        Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
                stmt.executeUpdate(query);
            }
            catch (SQLException ex) {
                ex.printStackTrace();
            }

            try (Statement stmt = conn.createStatement()) {
                String query = "DELETE FROM shard1.__ShardManagement.ShardMappingsLocal" + " WHERE MinValue = 0x80000008";
                stmt.executeUpdate(query);
            }
            catch (SQLException ex) {
                ex.printStackTrace();
            }

            try (Statement stmt = conn.createStatement()) {
                String query = "UPDATE shard1.__ShardManagement.ShardMappingsLocal SET MappingId = newid()" + " WHERE MinValue = 0x80000006";
                stmt.executeUpdate(query);
            }
            catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
        catch (Exception e) {
            System.out.printf("Failed to connect to SQL database with connection string: " + e.getMessage());
        }
        finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }

        RecoveryManager rm = new RecoveryManager(smm);

        List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

        assertEquals("The test environment was not expecting more than one local shardMap.", 1, gs.size());

        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            assertEquals("The count of differences does not match the expected.", 4, kvps.keySet().size());
            assertEquals("The count of shardMap only differences does not match the expected.", 1,
                    kvps.values().stream().filter(l -> l == MappingLocation.MappingInShardMapOnly).count());
            assertEquals("The count of shard only differences does not match the expected.", 2,
                    kvps.values().stream().filter(l -> l == MappingLocation.MappingInShardOnly).count());
            assertEquals("The count of shard only differences does not match the expected.", 1,
                    kvps.values().stream().filter(l -> l == MappingLocation.MappingInShardMapAndShard).count());
        }
    }

    /**
     * Test that consistency detection works when the ranges on the LSM and GSM are disjoint.
     */
    @Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public void testConsistencyDetectionAndViewingWithDisjointRanges() throws SQLException {
        ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

        RangeShardMap<Integer> rsm = smm.getRangeShardMap(RecoveryManagerTests.rangeShardMapName, ShardKeyType.Int32);

        assert rsm != null;

        // Make sure no other range mappings are floating around here.
        List<RangeMapping> rangeMappings = rsm.getMappings();
        for (RangeMapping rangeMapping : rangeMappings) {
            rsm.deleteMapping(rangeMapping);
        }

        ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, RecoveryManagerTests.shardDBs[0]);

        Shard s = rsm.createShard(sl);

        assert s != null;

        RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);
        assert r1 != null;

        // Add a range to the gsm
        RangeMapping r2 = rsm.createRangeMapping(new Range(11, 20), s);
        assert r2 != null;

        // Delete the original range from the GSM.
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

            try (Statement stmt = conn.createStatement()) {
                String query = String.format("delete from %1$s.__ShardManagement.ShardMappingsGlobal" + " where MinValue = 0x80000001",
                        Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
                stmt.executeUpdate(query);
            }
        }
        catch (Exception e) {
            System.out.printf("Failed to connect to SQL database with connection string: " + e.getMessage());
        }
        finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }

        // Delete the new range from the LSM, so the LSM and GSM now have non-intersecting ranges.
        try {
            conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

            try (Statement stmt = conn.createStatement()) {
                String query = String.format("delete from %1$s.__ShardManagement.ShardMappingsLocal" + " where MinValue = 0x8000000B",
                        sl.getDatabase());
                stmt.executeUpdate(query);
            }
        }
        catch (Exception e) {
            System.out.printf("Failed to connect to SQL database with connection string: " + e.getMessage());
        }
        finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }

        RecoveryManager rm = new RecoveryManager(smm);

        List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

        assertEquals("The test environment was not expecting more than one local shardMap.", 1, gs.size());

        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            assertEquals("The count of differences does not match the expected.", 2, kvps.keySet().size());

            for (Map.Entry<ShardRange, MappingLocation> kvp : kvps.entrySet()) {
                ShardRange range = kvp.getKey();
                MappingLocation mappingLocation = kvp.getValue();
                if ((int) range.getHigh().getValue() == 10) {
                    assertEquals(
                            "An unexpected difference between global and local shard maps was detected."
                                    + " This is likely a false positive and implies a bug in the detection code.",
                            MappingLocation.MappingInShardOnly, mappingLocation);
                    continue;
                }
                else if ((int) range.getHigh().getValue() == 20) {
                    assertEquals(
                            "An unexpected difference between global and local shard maps was detected."
                                    + " This is likely a false positive and implies a bug in the detection code.",
                            MappingLocation.MappingInShardMapOnly, mappingLocation);
                    continue;
                }
                fail("Unexpected range detected.");
            }
        }
    }

    /**
     * Test that consistency detection method produces usable LSMs when shards themselves disagree. In particular, make sure it reports on
     * subintervals not tagged to the current LSM.
     */
    @Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public void testConsistencyDetectionWithDivergence() throws SQLException {
        ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

        RangeShardMap<Integer> rsm = smm.getRangeShardMap(RecoveryManagerTests.rangeShardMapName, ShardKeyType.Int32);

        assert rsm != null;

        ShardLocation sl1 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, RecoveryManagerTests.shardDBs[0]);
        ShardLocation sl2 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, RecoveryManagerTests.shardDBs[1]);

        Shard s1 = rsm.createShard(sl1);
        Shard s2 = rsm.createShard(sl2);

        // set initial ranges as non-intersecting.
        RangeMapping r1 = rsm.createRangeMapping(new Range(1, 6), s1);
        assert r1 != null;

        RangeMapping r2 = rsm.createRangeMapping(new Range(6, 10), s2);
        assert r2 != null;

        // Perturb the first LSM so that it has a
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

            try (Statement stmt = conn.createStatement()) {
                String query = "UPDATE shard1.__ShardManagement.ShardMappingsLocal" + " SET MaxValue = 0x8000000B";
                stmt.executeUpdate(query);
            }
        }
        catch (Exception e) {
            System.out.printf("Failed to connect to SQL database with connection string: " + e.getMessage());
        }
        finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
        RecoveryManager rm = new RecoveryManager(smm);
        List<RecoveryToken> gs = rm.detectMappingDifferences(sl1);
        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);

            assertEquals("The count of differences does not match the expected.", 2, kvps.keySet().size());

            // We expect 6-10, and 10-11. If we did not detect intersected ranges, and only used tagged
            // ranges, we would have only 6-11 as a single range, which would be insufficient for rebuild.
            for (Map.Entry<ShardRange, MappingLocation> kvp : kvps.entrySet()) {
                ShardRange range = kvp.getKey();
                MappingLocation mappingLocation = kvp.getValue();
                if ((int) range.getHigh().getValue() == 10) {
                    assertEquals(
                            "An unexpected difference between global and local shard maps was detected."
                                    + " This is likely a false positive and implies a bug in the detection code.",
                            MappingLocation.MappingInShardMapAndShard, mappingLocation);
                    continue;
                }
                else if ((int) range.getHigh().getValue() == 11) {
                    assertEquals(
                            "An unexpected difference between global and local shard maps was detected."
                                    + " This is likely a false positive and implies a bug in the detection code.",
                            MappingLocation.MappingInShardOnly, mappingLocation);
                    continue;
                }
                fail("Unexpected range detected.");
            }
        }
    }

    /**
     * Test the "resolve using GSM" scenario.
     */
    @Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public void testCopyGsmToLsm() throws SQLException {
        ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

        RangeShardMap<Integer> rsm = smm.getRangeShardMap(RecoveryManagerTests.rangeShardMapName, ShardKeyType.Int32);

        assert rsm != null;

        ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, RecoveryManagerTests.shardDBs[0]);

        Shard s = rsm.createShard(sl);

        assert s != null;

        // Remove any garbage that might be floating around.
        List<RangeMapping> rangeMappings = rsm.getMappings();
        for (RangeMapping rangeMapping : rangeMappings) {
            rsm.deleteMapping(rangeMapping);
        }

        RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);
        assert r1 != null;

        // Add a range to the gsm
        RangeMapping r2 = rsm.createRangeMapping(new Range(11, 20), s);
        assert r2 != null;

        // Delete the new range from the LSM, so the LSM is missing all mappings from the GSM.
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

            try (Statement stmt = conn.createStatement()) {
                String query = "DELETE FROM shard1.__ShardManagement.ShardMappingsLocal";
                stmt.executeUpdate(query);
            }
        }
        catch (Exception e) {
            System.out.printf("Failed to connect to SQL database with connection string: " + e.getMessage());
        }
        finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }

        RecoveryManager rm = new RecoveryManager(smm);

        // Briefly validate that there are, in fact, the two ranges of inconsistency we are expecting.
        List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

        assertEquals("The test environment was not expecting more than one local shardMap.", 1, gs.size());

        // Briefly validate that
        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            assertEquals("The count of differences does not match the expected.", 2, kvps.keySet().size());
            for (Map.Entry<ShardRange, MappingLocation> kvp : kvps.entrySet()) {
                assertEquals(
                        "An unexpected difference between global and local shard maps was detected."
                                + " This is likely a false positive and implies a bug in the detection code.",
                        MappingLocation.MappingInShardMapOnly, kvp.getValue());
            }
            // Recover the LSM from the GSM
            rm.resolveMappingDifferences(g, MappingDifferenceResolution.KeepShardMapMapping);
        }

        // Validate that there are no more differences.
        List<RecoveryToken> gsAfterFix = rm.detectMappingDifferences(sl);
        for (RecoveryToken g : gsAfterFix) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            assertEquals("There were still differences after resolution.", 0, kvps.keySet().size());
        }
    }

    /**
     * Test the "resolve using LSM" scenario.
     */
    @Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public void testCopyLsmToGsm() throws SQLException {
        ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

        RangeShardMap<Integer> rsm = smm.getRangeShardMap(RecoveryManagerTests.rangeShardMapName, ShardKeyType.Int32);

        assert rsm != null;

        ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, RecoveryManagerTests.shardDBs[0]);

        Shard s = rsm.createShard(sl);

        assert s != null;

        RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);
        assert r1 != null;

        // Add a range to the gsm
        RangeMapping r2 = rsm.createRangeMapping(new Range(11, 20), s);
        assert r2 != null;

        // Delete everything from GSM (yes, this is overkill.)
        deleteAllMappingsFromGsm();

        RecoveryManager rm = new RecoveryManager(smm);

        // Briefly validate that there are, in fact, the two ranges of inconsistency we are expecting.
        List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

        assertEquals("The test environment was not expecting more than one local shardMap.", 1, gs.size());

        // Briefly validate that
        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            assertEquals("The count of differences does not match the expected.", 2, kvps.keySet().size());
            for (Map.Entry<ShardRange, MappingLocation> kvp : kvps.entrySet()) {
                assertEquals(
                        "An unexpected difference between global and local shard maps was detected."
                                + " This is likely a false positive and implies a bug in the detection code.",
                        MappingLocation.MappingInShardOnly, kvp.getValue());
            }
            // Recover the GSM from the LSM
            rm.resolveMappingDifferences(g, MappingDifferenceResolution.KeepShardMapping);
        }

        // Validate that there are no more differences.
        List<RecoveryToken> gsAfterFix = rm.detectMappingDifferences(sl);
        for (RecoveryToken g : gsAfterFix) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            assertEquals("There were still differences after resolution.", 0, kvps.keySet().size());
        }
    }

    /**
     * Test a restore of GSM from multiple different LSMs. (range)
     */
    @Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public void testRestoreGsmFromLsmsRange() throws SQLException {
        ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

        RangeShardMap<Integer> rsm = smm.getRangeShardMap(RecoveryManagerTests.rangeShardMapName, ShardKeyType.Int32);
        assert rsm != null;

        List<ShardLocation> sls = new ArrayList<>();
        int i = 0;
        ArrayList<RangeMapping> ranges = new ArrayList<>();
        for (String dbName : RecoveryManagerTests.shardDBs) {
            ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, dbName);
            sls.add(sl);
            Shard s = rsm.createShard(sl);
            assert s != null;
            RangeMapping r = rsm.createRangeMapping(new Range(1 + i * 10, 10 + i * 10), s);
            assert r != null;
            ranges.add(r);
            i++;
        }

        // Delete all mappings from GSM
        deleteAllMappingsFromGsm();

        RecoveryManager rm = new RecoveryManager(smm);

        // Validate that we detect the inconsistencies in all the LSMs.
        for (ShardLocation sl : sls) {
            List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

            assertEquals("The test environment was not expecting more than one local shardMap.", 1, gs.size());

            // Briefly validate that
            for (RecoveryToken g : gs) {
                Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
                assertEquals("The count of differences does not match the expected.", 1, kvps.keySet().size());

                for (Map.Entry<ShardRange, MappingLocation> kvp : kvps.entrySet()) {
                    assertEquals(
                            "An unexpected difference between global and local shard maps was detected."
                                    + " This is likely a false positive and implies a bug in the detection code.",
                            MappingLocation.MappingInShardOnly, kvp.getValue());
                }
            }
        }

        // Recover the LSM from the GSM
        rm.rebuildMappingsOnShardMapManagerFromShards(sls);

        // Validate that we fixed all the inconsistencies.
        for (ShardLocation sl : sls) {
            List<RecoveryToken> gs = rm.detectMappingDifferences(sl);
            // Briefly validate that
            for (RecoveryToken g : gs) {
                Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
                assertEquals("There were still differences after resolution.", 0, kvps.keySet().size());
            }
        }
    }

    /**
     * Test a restore of GSM from multiple different LSMs. (range)
     */
    @Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public void testRestoreGsmFromLsmsRangeWithGarbageInGsm() throws SQLException {
        ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

        RangeShardMap<Integer> rsm = smm.getRangeShardMap(RecoveryManagerTests.rangeShardMapName, ShardKeyType.Int32);

        assert rsm != null;
        List<ShardLocation> sls = new ArrayList<>();
        int i = 0;
        ArrayList<RangeMapping> ranges = new ArrayList<>();
        for (String dbName : RecoveryManagerTests.shardDBs) {
            ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, dbName);
            sls.add(sl);
            Shard s = rsm.createShard(sl);
            assert s != null;
            RangeMapping r = rsm.createRangeMapping(new Range(1 + i * 10, 10 + i * 10), s);
            assert r != null;
            ranges.add(r);
            i++;
        }

        // Perturb the mappings in the GSM.
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

            try (Statement stmt = conn.createStatement()) {
                String query = String.format(
                        "update %1$s.__ShardManagement.ShardMappingsGlobal" + " set MaxValue = MaxValue + 1, MinValue = MinValue + 1",
                        Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
                stmt.executeUpdate(query);
            }
        }
        catch (Exception e) {
            System.out.printf("Failed to connect to SQL database with connection string: " + e.getMessage());
        }
        finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }

        RecoveryManager rm = new RecoveryManager(smm);

        // Validate that we detect the inconsistencies in all the LSMs.
        for (ShardLocation sl : sls) {
            List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

            assertEquals("The test environment was not expecting more than one local shardMap.", 1, gs.size());

            // Briefly validate that
            for (RecoveryToken g : gs) {
                Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
                assertEquals("The count of differences does not match the expected.", 2, kvps.keySet().size());
            }
        }

        // Recover the LSM from the GSM
        rm.rebuildMappingsOnShardMapManagerFromShards(sls);

        // Validate that we fixed all the inconsistencies.
        for (ShardLocation sl : sls) {
            List<RecoveryToken> gs = rm.detectMappingDifferences(sl);
            // Briefly validate that
            for (RecoveryToken g : gs) {
                Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
                assertEquals("There were still differences after resolution.", 0, kvps.keySet().size());
            }
        }
    }

    /**
     * Test a restore of GSM from multiple different LSMs.
     */
    @Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public void testRestoreGsmFromLsmsList() throws SQLException {
        ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

        ListShardMap<Integer> lsm = smm.getListShardMap(RecoveryManagerTests.listShardMapName, ShardKeyType.Int32);

        assert lsm != null;
        List<ShardLocation> sls = new ArrayList<>();
        int i = Integer.MAX_VALUE;
        ArrayList<PointMapping> points = new ArrayList<>();
        for (String dbName : RecoveryManagerTests.shardDBs) {
            ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, dbName);
            sls.add(sl);
            Shard s = lsm.createShard(sl);
            assert s != null;
            PointMapping p = lsm.createPointMapping(i, s);
            assert p != null;
            points.add(p);
            i--;
        }

        // Delete all mappings from GSM
        deleteAllMappingsFromGsm();

        RecoveryManager rm = new RecoveryManager(smm);

        // Validate that we detect the inconsistencies in all the LSMs.
        for (ShardLocation sl : sls) {
            List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

            assertEquals("The test environment was not expecting more than one local shardMap.", 1, gs.size());

            // Briefly validate that
            for (RecoveryToken g : gs) {
                Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
                assertEquals("The count of differences does not match the expected.", 1, kvps.keySet().size());

                for (Map.Entry<ShardRange, MappingLocation> kvp : kvps.entrySet()) {
                    ShardRange range = kvp.getKey();
                    MappingLocation mappingLocation = kvp.getValue();
                    assertEquals(
                            "An unexpected difference between global and local shard maps was detected."
                                    + " This is likely a false positive and implies a bug in the detection code.",
                            MappingLocation.MappingInShardOnly, mappingLocation);
                }
            }
        }
    }

    /**
     * Test a restore of GSM from multiple different LSMs.
     */
    @Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public void testRestoreGsmFromLsmsListWithGarbageInGsm() throws SQLException {
        ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

        ListShardMap<Integer> lsm = smm.getListShardMap(RecoveryManagerTests.listShardMapName, ShardKeyType.Int32);

        assert lsm != null;
        List<ShardLocation> sls = new ArrayList<>();
        int i = 0;
        ArrayList<PointMapping> points = new ArrayList<>();
        for (String dbName : RecoveryManagerTests.shardDBs) {
            ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, dbName);
            sls.add(sl);
            Shard s = lsm.createShard(sl);
            assert s != null;
            PointMapping p = lsm.createPointMapping(i, s);
            assert p != null;
            points.add(p);
            i++;
        }

        // Delete all mappings from GSM
        deleteAllMappingsFromGsm();

        RecoveryManager rm = new RecoveryManager(smm);

        // Validate that we detect the inconsistencies in all the LSMs.
        for (ShardLocation sl : sls) {
            List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

            assertEquals("The test environment was not expecting more than one local shardMap.", 1, gs.size());

            // Briefly validate that
            for (RecoveryToken g : gs) {
                Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
                assertEquals("The count of differences does not match the expected.", 1, kvps.keySet().size());
            }
        }

        // Recover the LSM from the GSM
        rm.rebuildMappingsOnShardMapManagerFromShards(sls);

        // Validate that we fixed all the inconsistencies.
        for (ShardLocation sl : sls) {
            List<RecoveryToken> gs = rm.detectMappingDifferences(sl);
            // Briefly validate that
            for (RecoveryToken g : gs) {
                Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
                assertEquals("There were still differences after resolution.", 0, kvps.keySet().size());
            }
        }
    }

    /**
     * Test that the RebuildShard method produces usable LSMs for subsequent recovery action (range).
     */
    @Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public void testRebuildShardFromGsmRange() throws SQLException {
        ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

        RangeShardMap<Integer> rsm = smm.getRangeShardMap(RecoveryManagerTests.rangeShardMapName, ShardKeyType.Int32);

        assert rsm != null;

        ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, RecoveryManagerTests.shardDBs[0]);

        Shard s = rsm.createShard(sl);

        assert s != null;

        for (int i = 0; i < 5; i++) {
            RangeMapping r = rsm.createRangeMapping(new Range(1 + i, 2 + i), s);
            assert r != null;
        }

        // Delete all the ranges and shard maps from the shard location.
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

            try (Statement stmt = conn.createStatement()) {
                String query = "DELETE FROM shard1.__ShardManagement.ShardMappingsLocal";
                stmt.executeUpdate(query);
            }
        }
        catch (Exception e) {
            System.out.printf("Failed to connect to SQL database with connection string: " + e.getMessage());
        }
        finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }

        RecoveryManager rm = new RecoveryManager(smm);

        List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

        // Validate that all the shard locations are in fact missing from the LSM.
        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            assertEquals("The count of differences does not match the expected.", 5, kvps.keySet().size());

            for (Map.Entry<ShardRange, MappingLocation> kvp : kvps.entrySet()) {
                assertEquals(
                        "An unexpected difference between global and local shard maps was detected."
                                + " This is likely a false positive and implies a bug in the detection code.",
                        MappingLocation.MappingInShardMapOnly, kvp.getValue());
            }

            // Rebuild the range, leaving 2 inconsistencies (the last 2)
            List<ShardRange> ranges = kvps.entrySet().stream().map(Map.Entry::getKey).sorted(ShardRange::compareTo).limit(3)
                    .collect(Collectors.toList());
            rm.rebuildMappingsOnShard(g, ranges);
        }

        gs = rm.detectMappingDifferences(sl);

        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = new TreeMap<>(rm.getMappingDifferences(g));

            assertEquals("The count of differences does not match the expected.", 2,
                    kvps.values().stream().filter(loc -> loc != MappingLocation.MappingInShardMapAndShard).count());

            // We expect that the last two ranges only are missing from the shards.
            MappingLocation[] expectedLocations = new MappingLocation[] {MappingLocation.MappingInShardMapAndShard,
                    MappingLocation.MappingInShardMapAndShard, MappingLocation.MappingInShardMapAndShard, MappingLocation.MappingInShardMapOnly,
                    MappingLocation.MappingInShardMapOnly};

            Assert.assertArrayEquals("RebuildRangeShardMap rebuilt the shards out of order with respect" + " to its keep list.", expectedLocations,
                    kvps.values().toArray(new MappingLocation[kvps.size()]));

            // Rebuild the range, leaving 1 inconsistency
            List<ShardRange> ranges = kvps.entrySet().stream().map(Map.Entry::getKey).skip(1).collect(Collectors.toList());
            rm.rebuildMappingsOnShard(g, ranges);
        }

        gs = rm.detectMappingDifferences(sl);

        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            assertEquals("The count of differences does not match the expected.", 1,
                    kvps.values().stream().filter(loc -> loc != MappingLocation.MappingInShardMapAndShard).count());

            // Rebuild the range, leaving no inconsistencies
            List<ShardRange> ranges = kvps.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList());
            rm.rebuildMappingsOnShard(g, ranges);
        }

        gs = rm.detectMappingDifferences(sl);

        // Everything should be semantically consistent now.
        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            assertEquals("The count of differences does not match the expected.", 0,
                    kvps.values().stream().filter(loc -> loc != MappingLocation.MappingInShardMapAndShard).count());
            rm.resolveMappingDifferences(g, MappingDifferenceResolution.KeepShardMapping);
        }

        // As a sanity check, make sure the root is restorable from this LSM.
        gs = rm.detectMappingDifferences(sl);
        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            assertEquals("The GSM is not restorable from a rebuilt local shard.", 0, kvps.keySet().size());
        }
    }
    // Make sure that rebuild shard does not silently delete non conflicting ranges.

    @Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public void testRebuildShardFromGsmRangeKeepNonConflicts() throws SQLException {
        ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

        RangeShardMap<Integer> rsm = smm.getRangeShardMap(RecoveryManagerTests.rangeShardMapName, ShardKeyType.Int32);

        assert rsm != null;

        ShardLocation sl1 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, RecoveryManagerTests.shardDBs[0]);

        Shard s1 = rsm.createShard(sl1);

        // set initial ranges as non-intersecting.
        RangeMapping r1 = rsm.createRangeMapping(new Range(1, 6), s1);
        RangeMapping r2 = rsm.createRangeMapping(new Range(6, 10), s1);

        // Only mess up the range on the right.
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

            try (Statement stmt = conn.createStatement()) {
                String query = "UPDATE shard1.__ShardManagement.ShardMappingsLocal"
                        + " SET MaxValue = 0x8000000B, MappingId = newid() WHERE MaxValue = 0x8000000A";
                stmt.executeUpdate(query);
            }
            try (Statement stmt = conn.createStatement()) {
                String query = "UPDATE shard1.__ShardManagement.ShardMappingsLocal" + " SET MinValue = 0x8000000B WHERE MinValue = 0x8000000A";
                stmt.executeUpdate(query);
            }
        }
        catch (Exception e) {
            System.out.printf("Failed to connect to SQL database with connection string: " + e.getMessage());
        }
        finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
        RecoveryManager rm = new RecoveryManager(smm);

        List<RecoveryToken> gs = rm.detectMappingDifferences(sl1);
        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);

            assertEquals("The count of differences does not match the expected.", 2, kvps.keySet().size());

            // Let's make sure that rebuild does not un-intuitively delete ranges 1-6.
            rm.rebuildMappingsOnShard(g, new ArrayList<>());
        }

        gs = rm.detectMappingDifferences(sl1);
        for (RecoveryToken g : gs) {
            // Take local.
            rm.resolveMappingDifferences(g, MappingDifferenceResolution.KeepShardMapping);
        }

        List<RangeMapping> resultingMappings = rsm.getMappings(new Range(1, 11), s1);

        // Make sure the mapping [1-6) is still around.
        assertEquals("RebuildShard unexpectedly removed a non-conflicting range.", 1, resultingMappings.size());
    }

    /**
     * Test that the RebuildShard method produces usable LSMs for subsequent recovery action (list).
     */
    @Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public void testRebuildShardFromGsmList() throws SQLException {
        ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

        ListShardMap<Integer> lsm = smm.getListShardMap(RecoveryManagerTests.listShardMapName, ShardKeyType.Int32);

        assert lsm != null;

        ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, RecoveryManagerTests.shardDBs[0]);

        Shard s = lsm.createShard(sl);

        assert s != null;

        for (int i = 0; i < 5; i++) {
            PointMapping r = lsm.createPointMapping(i, s);
            assert r != null;
        }

        // Delete all the ranges and shard maps from the shard location.
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

            try (Statement stmt = conn.createStatement()) {
                String query = "DELETE FROM shard1.__ShardManagement.ShardMappingsLocal";
                stmt.executeUpdate(query);
            }
        }
        catch (Exception e) {
            System.out.printf("Failed to connect to SQL database with connection string: " + e.getMessage());
        }
        finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }

        RecoveryManager rm = new RecoveryManager(smm);

        List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

        // Validate that all the shard locations are in fact missing from the LSM.
        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            assertEquals("The count of differences does not match the expected.", 5, kvps.keySet().size());

            for (Map.Entry<ShardRange, MappingLocation> kvp : kvps.entrySet()) {
                assertEquals(
                        "An unexpected difference between global and local shard maps was detected."
                                + " This is likely a false positive and implies a bug in the detection code.",
                        MappingLocation.MappingInShardMapOnly, kvp.getValue());
            }

            // Rebuild the range, leaving 2 inconsistencies (the last 2)
            List<ShardRange> ranges = kvps.entrySet().stream().map(Map.Entry::getKey).sorted(ShardRange::compareTo).limit(3)
                    .collect(Collectors.toList());
            rm.rebuildMappingsOnShard(g, ranges);
        }

        gs = rm.detectMappingDifferences(sl);

        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = new TreeMap<>(rm.getMappingDifferences(g));

            assertEquals("The count of differences does not match the expected.", 2,
                    kvps.values().stream().filter(loc -> loc != MappingLocation.MappingInShardMapAndShard).count());

            // We expect that the last two ranges only are missing from the shards.
            MappingLocation[] expectedLocations = new MappingLocation[] {MappingLocation.MappingInShardMapAndShard,
                    MappingLocation.MappingInShardMapAndShard, MappingLocation.MappingInShardMapAndShard, MappingLocation.MappingInShardMapOnly,
                    MappingLocation.MappingInShardMapOnly};

            Assert.assertArrayEquals("RebuildRangeShardMap rebuilt the shards out of order with respect" + " to its keep list.", expectedLocations,
                    kvps.values().toArray(new MappingLocation[kvps.size()]));

            // Rebuild the range, leaving 1 inconsistency
            List<ShardRange> ranges = kvps.entrySet().stream().map(Map.Entry::getKey).skip(1).collect(Collectors.toList());
            rm.rebuildMappingsOnShard(g, ranges);
        }

        gs = rm.detectMappingDifferences(sl);

        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);

            assertEquals("The count of differences does not match the expected.", 1,
                    kvps.values().stream().filter(loc -> loc != MappingLocation.MappingInShardMapAndShard).count());

            // Rebuild the range, leaving no inconsistencies
            List<ShardRange> ranges = kvps.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList());
            rm.rebuildMappingsOnShard(g, ranges);
        }

        gs = rm.detectMappingDifferences(sl);

        // Everything should be semantically consistent now.
        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            assertEquals("The count of differences does not match the expected.", 0,
                    kvps.values().stream().filter(loc -> loc != MappingLocation.MappingInShardMapAndShard).count());
            rm.resolveMappingDifferences(g, MappingDifferenceResolution.KeepShardMapping);
        }

        // As a sanity check, make sure the root is restorable from this LSM.
        gs = rm.detectMappingDifferences(sl);
        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            assertEquals("The GSM is not restorable from a rebuilt local shard.", 0, kvps.keySet().size());
        }
    }

    /**
     * Basic sanity checks confirming that point mappings work the same way range mappings do in a recover-from-gsm scenario.
     */
    @Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public void testPointMappingRecoverFromGsm() throws SQLException {
        ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

        ListShardMap<Integer> listShardMap = smm.getListShardMap(RecoveryManagerTests.listShardMapName, ShardKeyType.Int32);

        assert listShardMap != null;

        ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, RecoveryManagerTests.shardDBs[0]);

        Shard s = listShardMap.createShard(sl);

        assert s != null;

        for (int i = 0; i < 5; i++) {
            PointMapping r = listShardMap.createPointMapping(new PointMappingCreationInfo(i, s, MappingStatus.Online));
            assert r != null;
        }

        // Delete all the ranges and shard maps from the shard location.
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

            try (Statement stmt = conn.createStatement()) {
                String query = "DELETE FROM shard1.__ShardManagement.ShardMappingsLocal";
                stmt.executeUpdate(query);
            }
        }
        catch (Exception e) {
            System.out.printf("Failed to connect to SQL database with connection string: " + e.getMessage());
        }
        finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }

        RecoveryManager rm = new RecoveryManager(smm);

        List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

        // Validate that all the shard locations are in fact missing from the LSM.
        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            assertEquals("The count of differences does not match the expected.", 5, kvps.keySet().size());

            for (Map.Entry<ShardRange, MappingLocation> kvp : kvps.entrySet()) {
                assertEquals(
                        "An unexpected difference between global and local shard maps was detected."
                                + " This is likely a false positive and implies a bug in the detection code.",
                        MappingLocation.MappingInShardMapOnly, kvp.getValue());
            }

            // Recover the LSM from the GSM
            rm.resolveMappingDifferences(g, MappingDifferenceResolution.KeepShardMapMapping);
        }

        gs = rm.detectMappingDifferences(sl);

        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            assertEquals("The count of differences does not match the expected.", 0, kvps.values().size());
        }
    }

    /**
     * Basic sanity checks confirming that pointmappings work the same way rangemappings do in a recover from rebuilt shard scenario.
     */
    @Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public final void testPointMappingRecoverFromLsm() throws SQLException {
        ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

        ListShardMap<Integer> lsm = smm.getListShardMap(RecoveryManagerTests.listShardMapName, ShardKeyType.Int32);

        assert lsm != null;

        ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, RecoveryManagerTests.shardDBs[0]);

        Shard s = lsm.createShard(sl);

        assert s != null;

        for (int i = 0; i < 5; i++) {
            PointMapping r = lsm.createPointMapping(new PointMappingCreationInfo(i, s, MappingStatus.Online));
            assert r != null;
        }

        // Delete all the ranges and shard maps from the shard location.
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

            try (Statement stmt = conn.createStatement()) {
                String query = "DELETE FROM shard1.__ShardManagement.ShardMappingsLocal";
                stmt.executeUpdate(query);
            }
        }
        catch (Exception e) {
            System.out.printf("Failed to connect to SQL database with connection string: " + e.getMessage());
        }
        finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }

        RecoveryManager rm = new RecoveryManager(smm);
        List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

        // Validate that all the shard locations are in fact missing from the LSM.
        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            Assert.assertEquals("The count of differences does not match the expected.", 5, kvps.keySet().size());

            for (Entry<ShardRange, MappingLocation> kvp : kvps.entrySet()) {
                Assert.assertEquals(
                        "An unexpected difference between global and local shard maps was"
                                + "detected. This is likely a false positive and implies a bug in detection code.",
                        MappingLocation.MappingInShardMapOnly, kvp.getValue());
            }

            List<ShardRange> keys = kvps.keySet().stream().sorted().limit(3).collect(Collectors.toList());
            rm.rebuildMappingsOnShard(g, keys);
        }

        gs = rm.detectMappingDifferences(sl);

        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            Assert.assertEquals("The count of differences does not match the expected.", 2,
                    kvps.values().stream().filter(loc -> loc != MappingLocation.MappingInShardMapAndShard).count());

            // We expect that the last two ranges only are missing from the shards.
            ArrayList<MappingLocation> expectedLocations = new ArrayList<>(
                    Arrays.asList(MappingLocation.MappingInShardMapAndShard, MappingLocation.MappingInShardMapAndShard,
                            MappingLocation.MappingInShardMapAndShard, MappingLocation.MappingInShardMapOnly, MappingLocation.MappingInShardMapOnly));

            Assert.assertTrue("RebuildRangeShardMap rebuilt the shards out of order with respect to its" + " keeplist.",
                    CollectionUtils.isEqualCollection(expectedLocations, kvps.values()));

            // Rebuild the range, leaving 1 inconsistency
            rm.rebuildMappingsOnShard(g, kvps.keySet().stream().skip(1).collect(Collectors.toList()));
        }

        gs = rm.detectMappingDifferences(sl);

        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            Assert.assertEquals("The count of differences does not match the expected.", 1,
                    kvps.values().stream().filter(loc -> loc != MappingLocation.MappingInShardMapAndShard).count());

            // Rebuild the range, leaving no inconsistencies
            rm.rebuildMappingsOnShard(g, new ArrayList<>(kvps.keySet()));
        }

        gs = rm.detectMappingDifferences(sl);

        // Everything should be semantically consistent now.
        for (RecoveryToken g : gs) {

            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            Assert.assertEquals("The count of differences does not match the expected.", 0,
                    kvps.values().stream().filter(loc -> loc != MappingLocation.MappingInShardMapAndShard).count());

            // As a sanity check, make sure the root is restorable from this LSM.
            rm.resolveMappingDifferences(g, MappingDifferenceResolution.KeepShardMapping);
        }

        gs = rm.detectMappingDifferences(sl);
        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            Assert.assertEquals("The GSM is not restorable from a rebuilt local shard.", 0, kvps.keySet().size());
        }
    }

    /**
     * Test geo failover scenario: rename one of the shards and then test detach/attach & consistency.
     */
    @Test
    @Category(value = ExcludeFromGatedCheckin.class)
    public void testGeoFailoverAttach() throws SQLException {
        ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

        ListShardMap<Integer> listsm = smm.getListShardMap(RecoveryManagerTests.listShardMapName, ShardKeyType.Int32);

        assert listsm != null;

        ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, RecoveryManagerTests.shardDBs[0]);

        // deploy LSM version 1.1 at location 'sl' before calling CreateShard() so that createShard will
        // not deploy latest LSM version
        smm.upgradeLocalStore(sl, new Version(1, 1));

        Shard s = listsm.createShard(sl);

        assert s != null;

        for (int i = 0; i < 5; i++) {
            PointMapping r = listsm.createPointMapping(new PointMappingCreationInfo(i, s, MappingStatus.Online));
            assert r != null;
        }

        // rename shard1 as shard1_new
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

            try (Statement stmt = conn.createStatement()) {
                String query = "ALTER DATABASE shard1 SET SINGLE_USER WITH ROLLBACK IMMEDIATE";
                stmt.executeUpdate(query);
            }

            try (Statement stmt = conn.createStatement()) {
                String query = "ALTER DATABASE shard1 MODIFY NAME = shard1_new";
                stmt.executeUpdate(query);
            }

            try (Statement stmt = conn.createStatement()) {
                String query = "ALTER DATABASE shard1_new SET MULTI_USER WITH ROLLBACK IMMEDIATE";
                stmt.executeUpdate(query);
            }
        }
        catch (Exception e) {
            System.out.printf("Failed to connect to SQL database with connection string: " + e.getMessage());
        }
        finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }

        RecoveryManager rm = new RecoveryManager(smm);

        rm.detachShard(sl);

        ShardLocation slNew = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, RecoveryManagerTests.shardDBs[0] + "_new");

        rm.attachShard(slNew);

        // Verify that shard location in LSM is updated to show database name as 'shard1_new'

        StoreResults result;

        try (IStoreOperationLocal op = smm.getStoreOperationFactory().createGetShardsLocalOperation(smm, slNew, "RecoveryTest")) {
            result = op.doLocal();
        }
        catch (Exception e) {
            e.printStackTrace();
            throw (ShardManagementException) e.getCause();
        }

        assert Objects.equals("shard1_new", result.getStoreShards().get(0).getLocation().getDatabase());

        // detect mapping differences and add local mappings to GSM
        List<RecoveryToken> gs = rm.detectMappingDifferences(slNew);

        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            assertEquals("Count of Mapping differences for shard1_new does not match expected value.", 5, kvps.keySet().size());
            rm.resolveMappingDifferences(g, MappingDifferenceResolution.KeepShardMapping);
        }

        gs = rm.detectMappingDifferences(slNew);

        for (RecoveryToken g : gs) {
            Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
            assertEquals("GSM and LSM at shard1_new do not have consistent mappings", 0, kvps.keySet().size());
        }

        // rename shard1_new back to shard1 so that test cleanup operations will succeed
        try {
            conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

            try (Statement stmt = conn.createStatement()) {
                String query = "ALTER DATABASE shard1_new SET SINGLE_USER WITH ROLLBACK IMMEDIATE";
                stmt.executeUpdate(query);
            }

            try (Statement stmt = conn.createStatement()) {
                String query = "ALTER DATABASE shard1_new MODIFY NAME = shard1";
                stmt.executeUpdate(query);
            }

            try (Statement stmt = conn.createStatement()) {
                String query = "ALTER DATABASE shard1 SET MULTI_USER WITH ROLLBACK IMMEDIATE";
                stmt.executeUpdate(query);
            }
        }
        catch (Exception e) {
            System.out.printf("Failed to connect to SQL database with connection string: " + e.getMessage());
        }
        finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
    }
}
