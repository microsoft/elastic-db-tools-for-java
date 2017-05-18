package com.microsoft.azure.elasticdb.shard.unittests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.microsoft.azure.elasticdb.shard.base.PointMapping;
import com.microsoft.azure.elasticdb.shard.base.Range;
import com.microsoft.azure.elasticdb.shard.base.RangeMapping;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.category.ExcludeFromGatedCheckin;
import com.microsoft.azure.elasticdb.shard.map.ListShardMap;
import com.microsoft.azure.elasticdb.shard.map.RangeShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerCreateMode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerFactory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerLoadPolicy;
import com.microsoft.azure.elasticdb.shard.recovery.MappingDifferenceResolution;
import com.microsoft.azure.elasticdb.shard.recovery.MappingLocation;
import com.microsoft.azure.elasticdb.shard.recovery.RecoveryManager;
import com.microsoft.azure.elasticdb.shard.recovery.RecoveryToken;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class RecoveryManagerTests {

  /**
   * Sharded databases to create for the test.
   */
  private static String[] s_shardedDBs = new String[]{"shard1", "shard2"};

  /**
   * GSM table names used in cleanup function.
   */
  private static String[] s_gsmTables = new String[]{
      "__ShardManagement.ShardMappingsGlobal", "__ShardManagement.ShardsGlobal",
      "__ShardManagement.ShardMapsGlobal", "__ShardManagement.OperationsLogGlobal"
  };

  /**
   * LSM table names used in cleanup function.
   */
  private static String[] s_lsmTables = new String[]{
      "__ShardManagement.ShardMappingsLocal", "__ShardManagement.ShardsLocal",
      "__ShardManagement.ShardMapsLocal"
  };

  /**
   * List shard map name.
   */
  private static String s_listShardMapName = "CustomersList";

  /**
   * Range shard map name.
   */
  private static String s_rangeShardMapName = "CustomersRange";

  /**
   * Helper function to create list and range shard maps.
   */
  private static void createShardMapsHelper() {
    // Create list shard map.
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ListShardMap<Integer> lsm = smm.createListShardMap(RecoveryManagerTests.s_listShardMapName,
        ShardKeyType.Int32);

    assert lsm != null;

    assert Objects.equals(RecoveryManagerTests.s_listShardMapName, lsm.getName());

    // Create range shard map.
    RangeShardMap<Integer> rsm = smm.createRangeShardMap(RecoveryManagerTests.s_rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    assert Objects.equals(RecoveryManagerTests.s_rangeShardMapName, rsm.getName());
  }

  /**
   * Helper function to clean SMM tables from all shards and GSM.
   */
  private static void cleanTablesHelper() throws SQLException {
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);
      // Clean LSM tables.
      for (String dbName : RecoveryManagerTests.s_shardedDBs) {
        for (String tableName : s_lsmTables) {
          try (Statement stmt = conn.createStatement()) {
            String query = String.format(Globals.CLEAN_DATABASE_QUERY, dbName, tableName);
            stmt.executeUpdate(query);
          } catch (SQLException ex) {
            ex.printStackTrace();
          }
        }
      }

      // Clean GSM tables
      for (String tableName : s_gsmTables) {
        try (Statement stmt = conn.createStatement()) {
          String query = String.format(Globals.CLEAN_DATABASE_QUERY,
              Globals.SHARD_MAP_MANAGER_DATABASE_NAME, tableName);
          stmt.executeUpdate(query);
        } catch (SQLException ex) {
          ex.printStackTrace();
        }
      }
    } catch (Exception e) {
      System.out.printf("Failed to connect to SQL database with connection string: "
          + e.getMessage());
    } finally {
      if (conn != null && !conn.isClosed()) {
        conn.close();
      }
    }
  }

  /**
   * Initializes common state for tests in this class.
   *
   * //@param testContext The TestContext we are running in.
   */
  @BeforeClass
  public static void recoveryManagerTestsInitialize() throws SQLException {
    // Clear all connection pools.
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);
      // Create ShardMapManager database
      try (Statement stmt = conn.createStatement()) {
        String query = String.format(Globals.CREATE_DATABASE_QUERY,
            Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
        stmt.executeUpdate(query);
      }
      // Create shard databases
      for (int i = 0; i < RecoveryManagerTests.s_shardedDBs.length; i++) {
        try (Statement stmt = conn.createStatement()) {
          String query = String.format(Globals.DROP_DATABASE_QUERY,
              RecoveryManagerTests.s_shardedDBs[i]);
          stmt.executeUpdate(query);
        }

        try (Statement stmt = conn.createStatement()) {
          String query = String.format(Globals.CREATE_DATABASE_QUERY,
              RecoveryManagerTests.s_shardedDBs[i]);
          stmt.executeUpdate(query);
        }
      }

      // Create shard map manager.
      ShardMapManagerFactory.createSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING,
          ShardMapManagerCreateMode.ReplaceExisting);
    } catch (Exception e) {
      System.out.printf("Failed to connect to SQL database with connection string: "
          + e.getMessage());
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
  public static void recoveryManagerTestsCleanup() throws SQLException {
    // Clear all connection pools.

    Connection conn = null;

    try {
      conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);
      // Drop shard databases
      for (int i = 0; i < RecoveryManagerTests.s_shardedDBs.length; i++) {
        try (Statement stmt = conn.createStatement()) {
          String query = String.format(Globals.DROP_DATABASE_QUERY,
              RecoveryManagerTests.s_shardedDBs[i]);
          stmt.executeUpdate(query);
        }
      }

      // Drop shard map manager database
      try (Statement stmt = conn.createStatement()) {
        String query = String.format(Globals.DROP_DATABASE_QUERY,
            Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
        stmt.executeUpdate(query);
      }
    } catch (Exception e) {
      System.out.printf("Failed to connect to SQL database with connection string: "
          + e.getMessage());
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
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(RecoveryManagerTests.s_rangeShardMapName);

    assert rsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        RecoveryManagerTests.s_shardedDBs[0]);

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
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(RecoveryManagerTests.s_rangeShardMapName);

    assert rsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        RecoveryManagerTests.s_shardedDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);

    assert r1 != null;

    RecoveryManager rm = new RecoveryManager(smm);

    List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

    assertEquals("The test environment was not expecting more than one local shardmap.", 1,
        gs.size());

    for (RecoveryToken g : gs) {
      Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
      assertEquals("An unexpected conflict was detected", 0, kvps.keySet().size());

      for (Map.Entry<ShardRange, MappingLocation> kvp : kvps.entrySet()) {
        ShardRange range = kvp.getKey();
        MappingLocation mappingLocation = kvp.getValue();

        assertEquals("An unexpected difference between global and local shard maps was"
                + "detected. This is likely a false positive and implies a bug in the detection code.",
            MappingLocation.MappingInShardMapAndShard, mappingLocation);
      }
    }
  }

  /**
   * Test that consistency detection works when there are only version conflicts.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void testConsistencyDetectionAndViewingWithVersionOnlyConflict() throws SQLException {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(RecoveryManagerTests.s_rangeShardMapName);

    assert rsm != null;

    // Make sure no other rangemappings are floating around here.
    List<RangeMapping> rangeMappings = rsm.getMappings();
    for (RangeMapping rangeMapping : rangeMappings) {
      rsm.deleteMapping(rangeMapping);
    }

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        RecoveryManagerTests.s_shardedDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);

    assert r1 != null;

    // Corrupt the mapping id number on the global shardmap.

    Connection conn = null;
    try {
      conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

      try (Statement stmt = conn.createStatement()) {
        String query = String.format("update %1$s.__ShardManagement.ShardMappingsGlobal"
            + " set MappingId = newid()", Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
        stmt.executeUpdate(query);
      }
    } catch (Exception e) {
      System.out.printf("Failed to connect to SQL database with connection string: "
          + e.getMessage());
    } finally {
      if (conn != null && !conn.isClosed()) {
        conn.close();
      }
    }

    RecoveryManager rm = new RecoveryManager(smm);

    List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

    assertEquals("The test environment was not expecting more than one local shardmap.", 1,
        gs.size());

    for (RecoveryToken g : gs) {
      Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
      assertEquals("An unexpected conflict was detected", 1, kvps.keySet().size());

      for (Map.Entry<ShardRange, MappingLocation> kvp : kvps.entrySet()) {
        ShardRange range = kvp.getKey();
        MappingLocation mappingLocation = kvp.getValue();

        assertEquals("An unexpected difference between global and local shard maps was"
                + "detected. This is likely a false positive and implies a bug in the detection code.",
            MappingLocation.MappingInShardMapAndShard, mappingLocation);
      }
    }
  }

  /**
   * Test that consistency detection works when the range in GSM is expanded while the LSM is left
   * untouched.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void testConsistencyDetectionAndViewingWithWiderRangeInLSM() throws SQLException {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(RecoveryManagerTests.s_rangeShardMapName);

    assert rsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        RecoveryManagerTests.s_shardedDBs[0]);

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
        String query = "update shard1.__ShardManagement.ShardMappingsLocal"
            + " set MinValue = MinValue - 1, MaxValue = MaxValue + 1";
        stmt.executeUpdate(query);
      }
    } catch (Exception e) {
      System.out.printf("Failed to connect to SQL database with connection string: "
          + e.getMessage());
    } finally {
      if (conn != null && !conn.isClosed()) {
        conn.close();
      }
    }

    RecoveryManager rm = new RecoveryManager(smm);

    List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

    assertEquals("The test environment was not expecting more than one local shardmap.", 1,
        gs.size());

    for (RecoveryToken g : gs) {
      Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
      assertEquals("The count of differences does not match the expected.", 2,
          kvps.keySet().size());

      for (Map.Entry<ShardRange, MappingLocation> kvp : kvps.entrySet()) {
        ShardRange range = kvp.getKey();
        MappingLocation mappingLocation = kvp.getValue();
        assertEquals("The ranges reported differed from those expected.", 1,
            (int) range.getHigh().getValue() - (int) range.getLow().getValue());
        assertEquals("An unexpected difference between global and local shard maps was"
                + "detected. This is likely a false positive and implies a bug in the detection code.",
            MappingLocation.MappingInShardOnly, mappingLocation);
      }
    }
  }

  /**
   * Test that consistency detection works when the range in GSM is expanded while the LSM is left
   * untouched.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void testConsistencyDetectionAndViewingWithWiderRangeInGSM() throws SQLException {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(RecoveryManagerTests.s_rangeShardMapName);

    assert rsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        RecoveryManagerTests.s_shardedDBs[0]);

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
        String query = String.format("update %1$s.__ShardManagement.ShardMappingsGlobal"
                + " set MinValue = MinValue - 1, MaxValue = MaxValue + 1",
            Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
        stmt.executeUpdate(query);
      }
    } catch (Exception e) {
      System.out.printf("Failed to connect to SQL database with connection string: "
          + e.getMessage());
    } finally {
      if (conn != null && !conn.isClosed()) {
        conn.close();
      }
    }

    RecoveryManager rm = new RecoveryManager(smm);

    List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

    assertEquals("The test environment was not expecting more than one local shardmap.", 1,
        gs.size());

    for (RecoveryToken g : gs) {
      Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
      assertEquals("The count of differences does not match the expected.", 2,
          kvps.keySet().size());
      for (Map.Entry<ShardRange, MappingLocation> kvp : kvps.entrySet()) {
        ShardRange range = kvp.getKey();
        MappingLocation mappingLocation = kvp.getValue();
        assertEquals("The ranges reported differed from those expected.", 1,
            (int) range.getHigh().getValue() - (int) range.getLow().getValue());
        assertEquals("An unexpected difference between global and local shard maps was"
                + "detected. This is likely a false positive and implies a bug in the detection code.",
            MappingLocation.MappingInShardMapOnly, mappingLocation);
      }
    }
  }

  /**
   * Test that consistency detection works the GSM is missing a range added to the LSM.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void testConsistencyDetectionAndViewingWithAdditionalRangeInLSM() throws SQLException {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(RecoveryManagerTests.s_rangeShardMapName);

    assert rsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        RecoveryManagerTests.s_shardedDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);

    // Add a range to the gsm
    RangeMapping r2 = rsm.createRangeMapping(new Range(11, 20), s);

    assert r1 != null;

    // Now, delete the new range from the GSM
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

      try (Statement stmt = conn.createStatement()) {
        String query = String.format("delete from %1$s.__ShardManagement.ShardMappingsGlobal"
            + " where MinValue = 0x8000000B", Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
        stmt.executeUpdate(query);
      }
    } catch (Exception e) {
      System.out.printf("Failed to connect to SQL database with connection string: "
          + e.getMessage());
    } finally {
      if (conn != null && !conn.isClosed()) {
        conn.close();
      }
    }

    RecoveryManager rm = new RecoveryManager(smm);

    List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

    assertEquals("The test environment was not expecting more than one local shardmap.", 1,
        gs.size());

    for (RecoveryToken g : gs) {
      Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
      assertEquals("The count of differences does not match the expected.", 1,
          kvps.keySet().size());
      for (Map.Entry<ShardRange, MappingLocation> kvp : kvps.entrySet()) {
        ShardRange range = kvp.getKey();
        MappingLocation mappingLocation = kvp.getValue();
        assertEquals("The range reported differed from that expected.", 20,
            (int) range.getHigh().getValue());
        assertEquals("An unexpected difference between global and local shard maps was"
                + "detected. This is likely a false positive and implies a bug in the detection code.",
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
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ListShardMap<Integer> rsm = smm.getListShardMap(RecoveryManagerTests.s_listShardMapName);

    assert rsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        RecoveryManagerTests.s_shardedDBs[0]);
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
        String query = String.format("delete from %1$s.__ShardManagement.ShardMappingsGlobal"
                + " where MinValue IN (0x80000000, 0x80000002)",
            Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
        stmt.executeUpdate(query);
      } catch (SQLException ex) {
        ex.printStackTrace();
      }

      try (Statement stmt = conn.createStatement()) {
        String query = "delete from shard1.__ShardManagement.ShardMappingsLocal"
            + " where MinValue = 0x80000008";
        stmt.executeUpdate(query);
      } catch (SQLException ex) {
        ex.printStackTrace();
      }

      try (Statement stmt = conn.createStatement()) {
        String query = "update shard1.__ShardManagement.ShardMappingsLocal set MappingId = newid()"
            + " where MinValue = 0x80000006";
        stmt.executeUpdate(query);
      } catch (SQLException ex) {
        ex.printStackTrace();
      }
    } catch (Exception e) {
      System.out.printf("Failed to connect to SQL database with connection string: "
          + e.getMessage());
    } finally {
      if (conn != null && !conn.isClosed()) {
        conn.close();
      }
    }

    RecoveryManager rm = new RecoveryManager(smm);

    List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

    assertEquals("The test environment was not expecting more than one local shardmap.", 1,
        gs.size());

    for (RecoveryToken g : gs) {
      Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
      assertEquals("The count of differences does not match the expected.", 4,
          kvps.keySet().size());
      assertEquals("The count of shardmap only differences does not match the expected.", 1,
          kvps.values().stream().filter(l -> l == MappingLocation.MappingInShardMapOnly).count());
      assertEquals("The count of shard only differences does not match the expected.", 2,
          kvps.values().stream().filter(l -> l == MappingLocation.MappingInShardOnly).count());
      assertEquals("The count of shard only differences does not match the expected.", 1, kvps
          .values().stream().filter(l -> l == MappingLocation.MappingInShardMapAndShard).count());
    }
  }

  /**
   * Test that consistency detection works when the ranges on the LSM and GSM are disjoint.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void testConsistencyDetectionAndViewingWithDisjointRanges() throws SQLException {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(RecoveryManagerTests.s_rangeShardMapName);

    assert rsm != null;

    // Make sure no other rangemappings are floating around here.
    List<RangeMapping> rangeMappings = rsm.getMappings();
    for (RangeMapping rangeMapping : rangeMappings) {
      rsm.deleteMapping(rangeMapping);
    }

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        RecoveryManagerTests.s_shardedDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);

    // Add a range to the gsm
    RangeMapping r2 = rsm.createRangeMapping(new Range(11, 20), s);

    assert r1 != null;

    // Delete the original range from the GSM.
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

      try (Statement stmt = conn.createStatement()) {
        String query = String.format("delete from %1$s.__ShardManagement.ShardMappingsGlobal"
            + " where MinValue = 0x80000001", Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
        stmt.executeUpdate(query);
      }
    } catch (Exception e) {
      System.out.printf("Failed to connect to SQL database with connection string: "
          + e.getMessage());
    } finally {
      if (conn != null && !conn.isClosed()) {
        conn.close();
      }
    }

    // Delete the new range from the LSM, so the LSM and GSM now have non-intersecting ranges.
    try {
      conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

      try (Statement stmt = conn.createStatement()) {
        String query = String.format("delete from shard1.__ShardManagement.ShardMappingsLocal"
            + " where MinValue = 0x8000000B", Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
        stmt.executeUpdate(query);
      }
    } catch (Exception e) {
      System.out.printf("Failed to connect to SQL database with connection string: "
          + e.getMessage());
    } finally {
      if (conn != null && !conn.isClosed()) {
        conn.close();
      }
    }

    RecoveryManager rm = new RecoveryManager(smm);

    List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

    assertEquals("The test environment was not expecting more than one local shardmap.", 1,
        gs.size());

    for (RecoveryToken g : gs) {
      Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
      assertEquals("The count of differences does not match the expected.", 2,
          kvps.keySet().size());

      for (Map.Entry<ShardRange, MappingLocation> kvp : kvps.entrySet()) {
        ShardRange range = kvp.getKey();
        MappingLocation mappingLocation = kvp.getValue();
        if ((int) range.getHigh().getValue() == 10) {
          assertEquals("An unexpected difference between global and local shard maps was"
                  + "detected. This is likely a false positive and implies a bug in the detection code.",
              MappingLocation.MappingInShardOnly, mappingLocation);
          continue;
        } else if ((int) range.getHigh().getValue() == 20) {
          assertEquals("An unexpected difference between global and local shard maps was"
                  + "detected. This is likely a false positive and implies a bug in the detection code.",
              MappingLocation.MappingInShardMapOnly, mappingLocation);
          continue;
        }
        fail("Unexpected range detected.");
      }
    }
  }

  /**
   * Test that consistency detection method produces usable LSMs when shards themselves disagree. In
   * particular, make sure it reports on subintervals not tagged to the current LSM.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void testConsistencyDetectionWithDivergence() throws SQLException {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(RecoveryManagerTests.s_rangeShardMapName);

    assert rsm != null;

    ShardLocation sl1 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        RecoveryManagerTests.s_shardedDBs[0]);
    ShardLocation sl2 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        RecoveryManagerTests.s_shardedDBs[1]);

    Shard s1 = rsm.createShard(sl1);
    Shard s2 = rsm.createShard(sl2);

    // set initial ranges as non-intersecting.
    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 6), s1);
    RangeMapping r2 = rsm.createRangeMapping(new Range(6, 10), s2);

    // Perturb the first LSM so that it has a
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

      try (Statement stmt = conn.createStatement()) {
        String query = "update shard1.__ShardManagement.ShardMappingsLocal"
            + " set MaxValue = 0x8000000B";
        stmt.executeUpdate(query);
      }
    } catch (Exception e) {
      System.out.printf("Failed to connect to SQL database with connection string: "
          + e.getMessage());
    } finally {
      if (conn != null && !conn.isClosed()) {
        conn.close();
      }
    }
    RecoveryManager rm = new RecoveryManager(smm);
    List<RecoveryToken> gs = rm.detectMappingDifferences(sl1);
    for (RecoveryToken g : gs) {
      Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);

      assertEquals("The count of differences does not match the expected.", 2,
          kvps.keySet().size());

      // We expect 6-10, and 10-11. If we did not detect intersected ranges, and only used tagged
      // ranges, we would have only 6-11 as a single range, which would be insufficient for rebuild.
      for (Map.Entry<ShardRange, MappingLocation> kvp : kvps.entrySet()) {
        ShardRange range = kvp.getKey();
        MappingLocation mappingLocation = kvp.getValue();
        if ((int) range.getHigh().getValue() == 10) {
          assertEquals("An unexpected difference between global and local shard maps was"
                  + "detected. This is likely a false positive and implies a bug in the detection code.",
              MappingLocation.MappingInShardMapAndShard, mappingLocation);
          continue;
        } else if ((int) range.getHigh().getValue() == 11) {
          assertEquals("An unexpected difference between global and local shard maps was"
                  + "detected. This is likely a false positive and implies a bug in the detection code.",
              MappingLocation.MappingInShardOnly, mappingLocation);
          continue;
        }
        fail("Unexpected range detected.");
      }
    }
  }

  /**
   * Test the "resolve using GSM" scenario.
   * 
   * @throws SQLException
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void testCopyGSMToLSM() throws SQLException {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm =
        smm.<Integer>getRangeShardMap(RecoveryManagerTests.s_rangeShardMapName);

    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, RecoveryManagerTests.s_shardedDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    // Remove any garbage that might be floating around.
    List<RangeMapping> rangeMappings = rsm.getMappings();
    for (RangeMapping rangeMapping : rangeMappings) {
      rsm.deleteMapping(rangeMapping);
    }

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);

    // Add a range to the gsm
    RangeMapping r2 = rsm.createRangeMapping(new Range(11, 20), s);


    assert r1 != null;

    // Delete the new range from the LSM, so the LSM is missing all mappings from the GSM.
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

      try (Statement stmt = conn.createStatement()) {
        String query = "delete from shard1.__ShardManagement.ShardMappingsLocal";
        stmt.executeUpdate(query);
      }
    } catch (Exception e) {
      System.out
          .printf("Failed to connect to SQL database with connection string: " + e.getMessage());
    } finally {
      if (conn != null && !conn.isClosed()) {
        conn.close();
      }
    }

    RecoveryManager rm = new RecoveryManager(smm);

    // Briefly validate that there are, in fact, the two ranges of inconsistency we are expecting.
    List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

    assertEquals("The test environment was not expecting more than one local shardmap.", 1,
        gs.size());

    // Briefly validate that
    for (RecoveryToken g : gs) {
      Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
      assertEquals("The count of differences does not match the expected.", 2,
          kvps.keySet().size());
      for (Map.Entry<ShardRange, MappingLocation> kvp : kvps.entrySet()) {
        ShardRange range = kvp.getKey();
        MappingLocation mappingLocation = kvp.getValue();
        assertEquals(
            "An unexpected difference between global and local shardmaps was detected. This is likely a false positive and implies a bug in the detection code.",
            MappingLocation.MappingInShardMapOnly, mappingLocation);
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
   * 
   * @throws SQLException
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void testCopyLSMToGSM() throws SQLException {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm =
        smm.<Integer>getRangeShardMap(RecoveryManagerTests.s_rangeShardMapName);

    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, RecoveryManagerTests.s_shardedDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);

    // Add a range to the gsm
    RangeMapping r2 = rsm.createRangeMapping(new Range(11, 20), s);


    assert r1 != null;

    // Delete everything from GSM (yes, this is overkill.)
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

      try (Statement stmt = conn.createStatement()) {
        String query = String.format("delete from %1$s.__ShardManagement.ShardMappingsGlobal",
            Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
        stmt.executeUpdate(query);
      }
    } catch (Exception e) {
      System.out
          .printf("Failed to connect to SQL database with connection string: " + e.getMessage());
    } finally {
      if (conn != null && !conn.isClosed()) {
        conn.close();
      }
    }

    RecoveryManager rm = new RecoveryManager(smm);

    // Briefly validate that there are, in fact, the two ranges of inconsistency we are expecting.
    List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

    assertEquals("The test environment was not expecting more than one local shardmap.", 1,
        gs.size());

    // Briefly validate that
    for (RecoveryToken g : gs) {
      Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
      assertEquals("The count of differences does not match the expected.", 2,
          kvps.keySet().size());
      for (Map.Entry<ShardRange, MappingLocation> kvp : kvps.entrySet()) {
        ShardRange range = kvp.getKey();
        MappingLocation mappingLocation = kvp.getValue();
        assertEquals(
            "An unexpected difference between global and local shardmaps was detected. This is likely a false positive and implies a bug in the detection code.",
            MappingLocation.MappingInShardOnly, mappingLocation);
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
  Test a restore of GSM from multiple different LSMs. (range)
   * @throws SQLException 
*/
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
 public void testRestoreGSMFromLSMsRange() throws SQLException
 {
     ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

     RangeShardMap<Integer> rsm = smm.<Integer>getRangeShardMap(RecoveryManagerTests.s_rangeShardMapName);

     assert rsm != null;
     List<ShardLocation> sls = new ArrayList<ShardLocation>();
     int i = 0;
     ArrayList<RangeMapping> ranges = new ArrayList<RangeMapping>();
     for (String dbName : RecoveryManagerTests.s_shardedDBs)
     {
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
     Connection conn = null;
     try 
     {
         conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

         try (Statement stmt = conn.createStatement())
         {
           String query = String.format("delete from %1$s.__ShardManagement.ShardMappingsGlobal", Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
           stmt.executeUpdate(query);
         }
     }catch (Exception e) {
       System.out.printf("Failed to connect to SQL database with connection string: "
           + e.getMessage());
     } finally {
       if (conn != null && !conn.isClosed()) {
         conn.close();
       }
     }

     RecoveryManager rm = new RecoveryManager(smm);

     // Validate that we detect the inconsistencies in all the LSMs.
     for (ShardLocation sl : sls)
     {
         List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

         assertEquals("The test environment was not expecting more than one local shardmap.", 1, gs.size());

         // Briefly validate that 
         for (RecoveryToken g : gs)
         {
           Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
           assertEquals("The count of differences does not match the expected.", 1, kvps.keySet().size());
           
             for (Map.Entry<ShardRange, MappingLocation> kvp : kvps.entrySet())
             {
                 ShardRange range = kvp.getKey();
                 MappingLocation mappingLocation = kvp.getValue();
                 assertEquals("An unexpected difference between global and local shardmaps was detected. This is likely a false positive and implies a bug in the detection code.", MappingLocation.MappingInShardOnly, mappingLocation);
             }
         }
     }

     // Recover the LSM from the GSM
     rm.rebuildMappingsOnShardMapManagerFromShards(sls);

     // Validate that we fixed all the inconsistencies.
     for (ShardLocation sl : sls)
     {
         List<RecoveryToken> gs = rm.detectMappingDifferences(sl);
         // Briefly validate that 
         for (RecoveryToken g : gs)
         {
             Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
             assertEquals("There were still differences after resolution.", 0, kvps.keySet().size());
         }
     }
 }


}

