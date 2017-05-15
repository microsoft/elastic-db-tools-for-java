package com.microsoft.azure.elasticdb.shard.unittests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

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
import com.microsoft.azure.elasticdb.shard.recovery.MappingLocation;
import com.microsoft.azure.elasticdb.shard.recovery.RecoveryManager;
import com.microsoft.azure.elasticdb.shard.recovery.RecoveryToken;

public class RecoveryManagerTests {
  /**
   * Sharded databases to create for the test.
   */
  private static String[] s_shardedDBs = new String[] {"shard1", "shard2"};

  /**
   * GSM table names used in cleanup function.
   */
  private static String[] s_gsmTables =
      new String[] {"__ShardManagement.ShardMappingsGlobal", "__ShardManagement.ShardsGlobal",
          "__ShardManagement.ShardMapsGlobal", "__ShardManagement.OperationsLogGlobal"};

  /**
   * LSM table names used in cleanup function.
   */
  private static String[] s_lsmTables = new String[] {"__ShardManagement.ShardMappingsLocal",
      "__ShardManagement.ShardsLocal", "__ShardManagement.ShardMapsLocal"};


  /**
   * List shard map name.
   */
  private static String s_listShardMapName = "Customerslist";

  /**
   * Range shard map name.
   */
  private static String s_rangeShardMapName = "Customersrange";

  /// #region Common Methods

  /**
   * Helper function to create list and range shard maps.
   */
  private static void createShardMapsHelper() {
    // Create list shard map.
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ListShardMap<Integer> lsm = smm
        .<Integer>createListShardMap(RecoveryManagerTests.s_listShardMapName, ShardKeyType.Int32);

    assert lsm != null;

    assert RecoveryManagerTests.s_listShardMapName == lsm.getName();

    // Create range shard map.
    RangeShardMap<Integer> rsm = smm
        .<Integer>createRangeShardMap(RecoveryManagerTests.s_rangeShardMapName, ShardKeyType.Int32);

    assert rsm != null;

    assert RecoveryManagerTests.s_rangeShardMapName == rsm.getName();
  }

  /**
   * Helper function to clean SMM tables from all shards and GSM.
   * 
   * @throws SQLException
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
      System.out.printf("Failed to connect to SQL database with connection string:",
          e.getMessage());
    } finally {
      if (conn != null && !conn.isClosed()) {
        conn.close();
      }
    }
  }

  /**
   * Initializes common state for tests in this class.
   * 
   * @param testContext The TestContext we are running in.
   * @throws SQLException
   */
  @BeforeClass
  public static void recoveryManagerTestsInitialize() throws SQLException {
    // Clear all connection pools.
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);
      // Create ShardMapManager database
      try (Statement stmt = conn.createStatement()) {
        String query =
            String.format(Globals.CREATE_DATABASE_QUERY, Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
      }
      // Create shard databases
      for (int i = 0; i < RecoveryManagerTests.s_shardedDBs.length; i++) {
        try (Statement stmt = conn.createStatement()) {
          String query =
              String.format(Globals.DROP_DATABASE_QUERY, RecoveryManagerTests.s_shardedDBs[i]);
          stmt.executeUpdate(query);
        }

        try (Statement stmt = conn.createStatement()) {
          String query =
              String.format(Globals.CREATE_DATABASE_QUERY, RecoveryManagerTests.s_shardedDBs[i]);
          stmt.executeUpdate(query);
        }
      }

      // Create shard map manager.
      ShardMapManagerFactory.createSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING,
          ShardMapManagerCreateMode.ReplaceExisting);
    } catch (Exception e) {
      System.out.printf("Failed to connect to SQL database with connection string:",
          e.getMessage());
    } finally {
      if (conn != null && !conn.isClosed()) {
        conn.close();
      }
    }
  }

  /**
   * Cleans up common state for the all tests in this class.
   * 
   * @throws SQLException
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
          String query =
              String.format(Globals.DROP_DATABASE_QUERY, RecoveryManagerTests.s_shardedDBs[i]);
          stmt.executeUpdate(query);
        }
      }

      // Drop shard map manager database
      try (Statement stmt = conn.createStatement()) {
        String query =
            String.format(Globals.DROP_DATABASE_QUERY, Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
      }
    } catch (Exception e) {
      System.out.printf("Failed to connect to SQL database with connection string:",
          e.getMessage());
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
   * 
   * @throws SQLException
   */
  @After
  public void shardMapperTestCleanup() throws SQLException {
    cleanTablesHelper();
  }

  /// #endregion Common Methods

  /**
   * Test Detach and Attach Shard Scenario. (This is just a stub for now.)
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void testDetachAttachShard() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm =
        smm.<Integer>getRangeShardMap(RecoveryManagerTests.s_rangeShardMapName);

    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, RecoveryManagerTests.s_shardedDBs[0]);

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

    RangeShardMap<Integer> rsm =
        smm.<Integer>getRangeShardMap(RecoveryManagerTests.s_rangeShardMapName);

    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, RecoveryManagerTests.s_shardedDBs[0]);

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

        assertEquals(
            "An unexpected difference between global and local shardmaps was detected. This is likely a false positive and implies a bug in the detection code.",
            MappingLocation.MappingInShardMapAndShard, mappingLocation);
      }
    }
  }

  /**
   * Test that consistency detection works when there are only version conflicts.
   * 
   * @throws SQLException
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void testConsistencyDetectionAndViewingWithVersionOnlyConflict() throws SQLException {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm =
        smm.<Integer>getRangeShardMap(RecoveryManagerTests.s_rangeShardMapName);

    assert rsm != null;

    // Make sure no other rangemappings are floating around here.
    List<RangeMapping> rangeMappings = rsm.getMappings();
    for (RangeMapping rangeMapping : rangeMappings) {
      rsm.deleteMapping(rangeMapping);
    }

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, RecoveryManagerTests.s_shardedDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);

    assert r1 != null;

    // Corrupt the mapping id number on the global shardmap.

    Connection conn = null;
    try {
      conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_CONN_STRING);

      try (Statement stmt = conn.createStatement()) {
        String query = String.format(
            "update %1$s.__ShardManagement.ShardMappingsGlobal set MappingId = newid()",
            Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
        stmt.executeUpdate(query);
      }
    } catch (Exception e) {
      System.out.printf("Failed to connect to SQL database with connection string:",
          e.getMessage());
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

        assertEquals(
            "An unexpected difference between global and local shardmaps was detected. This is likely a false positive and implies a bug in the detection code.",
            MappingLocation.MappingInShardMapAndShard, mappingLocation);
      }
    }
  }

  /**
   * Test that consistency detection works when the range in GSM is expanded while the LSM is left
   * untouched.
   * 
   * @throws SQLException
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void testConsistencyDetectionAndViewingWithWiderRangeInLSM() throws SQLException {
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

    assert r1 != null;

    // Corrupt the lsm by increasing the max range and decreasing min range. We should see two
    // ranges show up in the list of differences. The shared range
    // in the middle artificially has the same version number, so it should not register as a
    // conflicting range.

    Connection conn = null;
    try {
      conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_CONN_STRING);

      try (Statement stmt = conn.createStatement()) {
        String query =
            "update shard1.__ShardManagement.ShardMappingsLocal set MinValue = MinValue - 1, MaxValue = MaxValue + 1";
        stmt.executeUpdate(query);
      }
    } catch (Exception e) {
      System.out.printf("Failed to connect to SQL database with connection string:",
          e.getMessage());
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
        assertEquals(
            "An unexpected difference between global and local shardmaps was detected. This is likely a false positive and implies a bug in the detection code.",
            MappingLocation.MappingInShardOnly, mappingLocation);
      }
    }
  }
}
