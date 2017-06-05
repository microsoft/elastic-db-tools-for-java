package com.microsoft.azure.elasticdb.shard.unittests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryBehavior;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.shard.base.MappingLockToken;
import com.microsoft.azure.elasticdb.shard.base.MappingStatus;
import com.microsoft.azure.elasticdb.shard.base.PointMapping;
import com.microsoft.azure.elasticdb.shard.base.PointMappingUpdate;
import com.microsoft.azure.elasticdb.shard.base.Range;
import com.microsoft.azure.elasticdb.shard.base.RangeMapping;
import com.microsoft.azure.elasticdb.shard.base.RangeMappingUpdate;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.cache.CacheStore;
import com.microsoft.azure.elasticdb.shard.category.ExcludeFromGatedCheckin;
import com.microsoft.azure.elasticdb.shard.decorators.CountingCacheStore;
import com.microsoft.azure.elasticdb.shard.map.ListShardMap;
import com.microsoft.azure.elasticdb.shard.map.RangeShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerCreateMode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerFactory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerLoadPolicy;
import com.microsoft.azure.elasticdb.shard.mapper.ConnectionOptions;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlShardMapManagerCredentials;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlStoreConnectionFactory;
import com.microsoft.azure.elasticdb.shard.store.IStoreConnection;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreConnectionKind;
import com.microsoft.azure.elasticdb.shard.store.StoreLogEntry;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreTransactionScopeKind;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class ShardMapperTest {

  /**
   * Sharded databases to create for the test.
   */
  private static String[] shardedDBs = new String[]{"shard1", "shard2"};

  /**
   * List shard map name.
   */
  private static String listShardMapName = "CustomersList";

  /**
   * Range shard map name.
   */
  private static String rangeShardMapName = "CustomersRange";

  /**
   * Helper function to clean list and range shard maps.
   */
  private static void cleanShardMapsHelper() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    // Remove all existing mappings from the list shard map.
    ListShardMap<Integer> lsm = null;
    ReferenceObjectHelper<ListShardMap<Integer>> refLsm =
        new ReferenceObjectHelper<>(lsm);
    if (smm.tryGetListShardMap(ShardMapperTest.listShardMapName, refLsm)) {
      lsm = refLsm.argValue;
      assert lsm != null;

      for (PointMapping pm : lsm.getMappings()) {
        PointMapping pmOffline = lsm.markMappingOffline(pm);
        assert pmOffline != null;
        lsm.deleteMapping(pmOffline);
      }

      // Remove all shards from list shard map
      for (Shard s : lsm.getShards()) {
        lsm.deleteShard(s);
      }
    } else {
      lsm = refLsm.argValue;
    }

    // Remove all existing mappings from the range shard map.
    RangeShardMap<Integer> rsm = null;
    ReferenceObjectHelper<RangeShardMap<Integer>> refRsm =
        new ReferenceObjectHelper<>(rsm);
    if (smm.tryGetRangeShardMap(ShardMapperTest.rangeShardMapName, refRsm)) {
      rsm = refRsm.argValue;
      assert rsm != null;

      for (RangeMapping rm : rsm.getMappings()) {
        MappingLockToken mappingLockToken = rsm.getMappingLockOwner(rm);
        rsm.unlockMapping(rm, mappingLockToken);
        RangeMapping rmOffline = rsm.markMappingOffline(rm);
        assert rmOffline != null;
        rsm.deleteMapping(rmOffline);
      }

      // Remove all shards from range shard map
      for (Shard s : rsm.getShards()) {
        rsm.deleteShard(s);
      }
    } else {
      rsm = refRsm.argValue;
    }
  }

  /**
   * Initializes common state for tests in this class.
   */
  @BeforeClass
  public static void shardMapperTestsInitialize() throws SQLException {
    // Clear all connection pools.
    // TODO:SqlConnection.ClearAllPools();
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);
      try (Statement stmt = conn.createStatement()) {
        // Create ShardMapManager database
        String query =
            String.format(Globals.CREATE_DATABASE_QUERY, Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
        stmt.executeUpdate(query);
      } catch (SQLException ex) {
        ex.printStackTrace();
      }

      // Create shard databases
      for (int i = 0; i < ShardMapperTest.shardedDBs.length; i++) {
        try (Statement stmt = conn.createStatement()) {
          String query =
              String.format(Globals.DROP_DATABASE_QUERY, ShardMapperTest.shardedDBs[i]);
          stmt.executeUpdate(query);
        } catch (SQLException ex) {
          ex.printStackTrace();
        }
        try (Statement stmt = conn.createStatement()) {
          String query =
              String.format(Globals.CREATE_DATABASE_QUERY, ShardMapperTest.shardedDBs[i]);
          stmt.executeUpdate(query);
        } catch (SQLException ex) {
          ex.printStackTrace();
        }
      }

      // Create shard map manager.
      ShardMapManagerFactory.createSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING,
          ShardMapManagerCreateMode.ReplaceExisting);

      // Create list shard map.
      ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

      ListShardMap<Integer> lsm =
          smm.createListShardMap(ShardMapperTest.listShardMapName, ShardKeyType.Int32);

      assert lsm != null;

      assert Objects.equals(ShardMapperTest.listShardMapName, lsm.getName());

      // Create range shard map.
      RangeShardMap<Integer> rsm =
          smm.createRangeShardMap(ShardMapperTest.rangeShardMapName, ShardKeyType.Int32);

      assert rsm != null;

      assert Objects.equals(ShardMapperTest.rangeShardMapName, rsm.getName());
    } catch (Exception e) {
      System.out.printf("Failed to connect to SQL database: " + e.getMessage());
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
  public static void shardMapperTestsCleanup() throws SQLException {
    // Clear all connection pools.
    // TODO:SqlConnection.ClearAllPools();
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

      // Drop shard databases
      for (int i = 0; i < ShardMapperTest.shardedDBs.length; i++) {
        try (Statement stmt = conn.createStatement()) {
          String query =
              String.format(Globals.DROP_DATABASE_QUERY, ShardMapperTest.shardedDBs[i]);
          stmt.executeUpdate(query);
        } catch (SQLException ex) {
          ex.printStackTrace();
        }
      }

      // Drop shard map manager database
      try (Statement stmt = conn.createStatement()) {
        String query =
            String.format(Globals.DROP_DATABASE_QUERY, Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
        stmt.executeUpdate(query);
      } catch (SQLException ex) {
        ex.printStackTrace();
      }

    } catch (Exception e) {
      System.out.printf("Failed to connect to SQL database: " + e.getMessage());
    } finally {
      if (conn != null && !conn.isClosed()) {
        conn.close();
      }
    }
  }

  static List<StoreLogEntry> getPendingStoreOperations() {
    StoreResults result = new StoreResults();
    try (IStoreConnection conn = (new SqlStoreConnectionFactory()).getConnection(
        StoreConnectionKind.Global, Globals.SHARD_MAP_MANAGER_CONN_STRING)) {

      try (IStoreTransactionScope ts = conn.getTransactionScope(
          StoreTransactionScopeKind.ReadOnly)) {
        result = ts.executeCommandSingle(new StringBuilder("SELECT 6, OperationId, OperationCode,"
            + " Data, UndoStartState, ShardVersionRemoves, ShardVersionAdds FROM"
            + " __ShardManagement.OperationsLogGlobal"));
      } catch (Exception e) {
        e.printStackTrace();
        //TODO Handle Exception
      }
    }

    return result.getStoreOperations();
  }

  /**
   * Initializes common state per-test.
   */
  @Before
  public void shardMapperTestInitialize() {
    ShardMapperTest.cleanShardMapsHelper();
  }

  /**
   * Cleans up common state per-test.
   */
  @After
  public void shardMapperTestCleanup() {
    ShardMapperTest.cleanShardMapsHelper();
  }

  /**
   * Shard map type conversion between list and range.
   */
  /*@Test
  @Category(value = ExcludeFromGatedCheckin.class)*/
  public void shardMapTypeFailures() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    // Try to get list<int> shard map as range<int>
    try {
      RangeShardMap<Integer> rsm =
          smm.getRangeShardMap(ShardMapperTest.listShardMapName);
      fail("GetRangeshardMap did not throw as expected");
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ShardMapManager == sme.getErrorCategory();
      assert ShardManagementErrorCode.ShardMapTypeConversionError == sme.getErrorCode();
    }

    // Try to get range<int> shard map as list<int>
    try {
      ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTest.rangeShardMapName);
      fail("GetListShardMap did not throw as expected");
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ShardMapManager == sme.getErrorCategory();
      assert ShardManagementErrorCode.ShardMapTypeConversionError == sme.getErrorCode();
    }

    // Try to get list<int> shard map as list<guid>
    try {
      ListShardMap<UUID> lsm = smm.getListShardMap(ShardMapperTest.listShardMapName);
      fail("GetListShardMap did not throw as expected");
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ShardMapManager == sme.getErrorCategory();
      assert ShardManagementErrorCode.ShardMapTypeConversionError == sme.getErrorCode();
    }

    // Try to get range<int> shard map as range<long>
    try {
      RangeShardMap<Long> rsm = smm.getRangeShardMap(ShardMapperTest.rangeShardMapName);
      fail("GetRangeshardMap did not throw as expected");
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ShardMapManager == sme.getErrorCategory();
      assert ShardManagementErrorCode.ShardMapTypeConversionError == sme.getErrorCode();
    }

    // Try to get range<int> shard map as list<guid>
    try {
      ListShardMap<UUID> lsm = smm.getListShardMap(ShardMapperTest.rangeShardMapName);
      fail("GetListShardMap did not throw as expected");
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ShardMapManager == sme.getErrorCategory();
      assert ShardManagementErrorCode.ShardMapTypeConversionError == sme.getErrorCode();
    }
  }

  /**
   * Add a point mapping to list shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void addPointMappingDefault() throws SQLException {
    addPointMappingDefault(ShardKeyInfo.allTestShardKeyValues.subList(0, 7));
    addPointMappingDefault(ShardKeyInfo.allTestShardKeyValues.subList(8, 15));
    addPointMappingDefault(ShardKeyInfo.allTestShardKeyValues.subList(16, 18));
    // TODO:
    // AddPointMappingDefault(ShardKeyInfo.allTestShardKeyValues.<byte[]>OfType());
    // AddPointMappingDefault(ShardKeyInfo.allTestShardKeyValues.<java.time.LocalDateTime>OfType());
    // AddPointMappingDefault(ShardKeyInfo.allTestShardKeyValues.<DateTimeOffset>OfType());
    // AddPointMappingDefault(ShardKeyInfo.allTestShardKeyValues.<TimeSpan>OfType());
  }

  /**
   * All combinations of getting point mappings from a list shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void getPointMappingsForRange() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTest.listShardMapName);

    assert lsm != null;

    Shard s1 = lsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]));
    assert s1 != null;

    Shard s2 = lsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[1]));
    assert s2 != null;

    PointMapping p1 = lsm.createPointMapping(1, s1);
    assert p1 != null;

    PointMapping p2 = lsm.createPointMapping(10, s1);
    assert p2 != null;

    PointMapping p3 = lsm.createPointMapping(5, s2);
    assert p3 != null;

    // Get all mappings in shard map.
    List<PointMapping> allMappings = lsm.getMappings();
    assert 3 == allMappings.size();

    // Get all mappings in specified range.
    List<PointMapping> mappingsInRange = lsm.getMappings(new Range(5, 15));
    assert 2 == mappingsInRange.size();

    // Get all mappings for a shard.
    List<PointMapping> mappingsForShard = lsm.getMappings(s1);
    assert 2 == mappingsForShard.size();

    // Get all mappings in specified range for a particular shard.
    List<PointMapping> mappingsInRangeForShard = lsm.getMappings(new Range(5, 15), s1);
    assert 1 == mappingsInRangeForShard.size();
  }

  /**
   * Add a duplicate point mapping to list shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void addPointMappingDuplicate() {
    CountingCacheStore countingCache = new CountingCacheStore(new CacheStore());

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), new StoreOperationFactory(), countingCache,
        ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO, Duration.ZERO,
        Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTest.listShardMapName);

    assert lsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]);

    Shard s = lsm.createShard(sl);

    assert s != null;

    PointMapping p1 = lsm.createPointMapping(1, s);

    assert p1 != null;

    boolean addFailed = false;
    try {
      // add same point mapping again.
      PointMapping pmNew = lsm.createPointMapping(1, s);
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ListShardMap == sme.getErrorCategory();
      assert ShardManagementErrorCode.MappingPointAlreadyMapped == sme.getErrorCode();
      addFailed = true;
    }

    assert addFailed;

    PointMapping p2 = lsm.getMappingForKey(1);

    assert p2 != null;
    assert 0 == countingCache.getLookupMappingHitCount();
  }

  /**
   * Delete existing point mapping from list shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void deletePointMappingDefault() {
    CountingCacheStore countingCache = new CountingCacheStore(new CacheStore());

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), new StoreOperationFactory(), countingCache,
        ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO, Duration.ZERO,
        Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTest.listShardMapName);

    assert lsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]);

    Shard s = lsm.createShard(sl);

    assert s != null;

    PointMapping pm2 = lsm.createPointMapping(1, s);

    assert pm2 != null;

    PointMapping pm1 = lsm.getMappingForKey(1);

    assert pm1 != null;
    assert 0 == countingCache.getLookupMappingHitCount();

    // The mapping must be made offline first before it can be deleted.
    PointMappingUpdate ru = new PointMappingUpdate();
    ru.setStatus(MappingStatus.Offline);

    PointMapping mappingToDelete = lsm.updateMapping(pm2, ru);

    lsm.deleteMapping(mappingToDelete);

    // Verify that the mapping is removed from cache.
    boolean lookupFailed = false;
    try {
      lsm.getMappingForKey(1);
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ListShardMap == sme.getErrorCategory();
      assert ShardManagementErrorCode.MappingNotFoundForKey == sme.getErrorCode();
      lookupFailed = true;
    }

    assert lookupFailed;
    assert 0 == countingCache.getLookupMappingMissCount();
  }

  /**
   * Delete non-existing point mapping from list shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void deletePointMappingNonExisting() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTest.listShardMapName);

    assert lsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]);

    Shard s = lsm.createShard(sl);

    assert s != null;

    PointMapping p1 = lsm.createPointMapping(1, s);

    assert p1 != null;

    PointMappingUpdate ru = new PointMappingUpdate();
    ru.setStatus(MappingStatus.Offline);

    // The mapping must be made offline before it can be deleted.
    p1 = lsm.updateMapping(p1, ru);

    lsm.deleteMapping(p1);

    boolean removeFailed = tryDeletePointMapping(lsm, p1);
    assert removeFailed;
  }

  /**
   * Delete point mapping with version mismatch from list shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void deletePointMappingVersionMismatch() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTest.listShardMapName);

    assert lsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]);

    Shard s = lsm.createShard(sl);

    assert s != null;

    PointMapping p1 = lsm.createPointMapping(1, s);

    assert p1 != null;

    PointMappingUpdate pu = new PointMappingUpdate();
    pu.setStatus(MappingStatus.Offline);

    PointMapping pmNew = lsm.updateMapping(p1, pu);
    assert pmNew != null;

    boolean removeFailed = tryDeletePointMapping(lsm, p1);
    assert removeFailed;
  }

  /**
   * Update existing point mapping in list shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void updatePointMappingDefault() {
    CountingCacheStore countingCache = new CountingCacheStore(new CacheStore());

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), new StoreOperationFactory(), countingCache,
        ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO, Duration.ZERO,
        Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTest.listShardMapName);

    assert lsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]);

    Shard s = lsm.createShard(sl);

    assert s != null;

    PointMapping p1 = lsm.createPointMapping(1, s);

    PointMappingUpdate pu = new PointMappingUpdate();
    pu.setStatus(MappingStatus.Offline);

    PointMapping pmNew = lsm.updateMapping(p1, pu);
    assert pmNew != null;

    PointMapping p2 = lsm.getMappingForKey(1);

    assert p2 != null;
    assert 0 == countingCache.getLookupMappingHitCount();

    // Mark the mapping online again so that it will be cleaned up
    pu.setStatus(MappingStatus.Online);
    PointMapping pmUpdated = lsm.updateMapping(pmNew, pu);
    assert pmUpdated != null;
  }

  /**
   * Take a mapping offline, verify that the existing connection is killed.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void killConnectionOnOfflinePointMapping() {
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), new StoreOperationFactory(), new CacheStore(),
        ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO, Duration.ZERO,
        Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTest.listShardMapName);

    assert lsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]);

    Shard s = lsm.createShard(sl);

    assert s != null;

    PointMapping p1 = lsm.createPointMapping(1, s);

    // TODO:OpenConnectionForKeyAsync with int and string as parameters
    /*
     * try (Connection conn = lsm.openConnectionAsync(1, Globals.SHARD_USER_CONN_STRING).Result) {
     * assert ConnectionState.Open == conn.gets;
     *
     * PointMappingUpdate pu = new PointMappingUpdate(); pu.setStatus(MappingStatus.Offline);
     *
     * PointMapping pmNew = lsm.updateMapping(p1, pu); assert pmNew != null;
     *
     * boolean failed = false;
     *
     * try { try (SqlCommand cmd = conn.CreateCommand()) { cmd.CommandText = "select 1";
     * cmd.CommandType = CommandType.Text;
     *
     * try (SqlDataReader rdr = cmd.ExecuteReader()) { } } } catch (SqlException e) { failed = true;
     * }
     *
     * assert true == failed; assert ConnectionState.Closed == conn.State;
     *
     * failed = false;
     *
     * // Open 2nd connection. try { try (SqlConnection conn2 = lsm.OpenConnectionForKeyAsync(1,
     * Globals.ShardUserConnectionString).Result) { } } catch (AggregateException ex) {
     * RuntimeException tempVar = ex.getCause(); ShardManagementException sme =
     * (ShardManagementException)((tempVar instanceof ShardManagementException) ? tempVar : null);
     * if (sme != null) { failed = true; assert ShardManagementErrorCode.MappingIsOffline ==
     * sme.ErrorCode; } }
     *
     * assert true == failed;
     *
     * // Mark the mapping online again so that it will be cleaned up pu.Status =
     * MappingStatus.Online; PointMapping<Integer> pUpdated = lsm.updateMapping(pmNew, pu); assert
     * pUpdated != null;
     *
     * failed = false;
     *
     * // Open 3rd connection. This should succeed. try { try (SqlConnection conn3 =
     * lsm.OpenConnectionForKey(1, Globals.ShardUserConnectionString)) { } } catch
     * (ShardManagementException e2) { failed = true; }
     *
     * assert false == failed; }
     */
  }

  /**
   * Update location of existing point mapping in list shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void updatePointMappingLocation() {
    CountingCacheStore countingCache = new CountingCacheStore(new CacheStore());

    // TODO:RetryPolicy
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), new StoreOperationFactory(), countingCache,
        ShardMapManagerLoadPolicy.Lazy, RetryPolicy.getDefaultRetryPolicy(),
        RetryBehavior.getDefaultRetryBehavior());

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTest.listShardMapName);

    assert lsm != null;

    Shard s1 = lsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]));
    assert s1 != null;

    Shard s2 = lsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[1]));
    assert s2 != null;

    PointMapping p1 = lsm.createPointMapping(1, s1);

    PointMappingUpdate pu = new PointMappingUpdate();
    pu.setStatus(MappingStatus.Offline); // Shard location in a mapping cannot be changed unless it
    // is offline.

    PointMapping pmOffline = lsm.updateMapping(p1, pu);

    assert pmOffline != null;
    assert pu.getStatus() == pmOffline.getStatus();
    pu.setShard(s2);

    PointMapping pmNew = lsm.updateMapping(pmOffline, pu);
    assert pmNew != null;

    PointMapping p2 = lsm.getMappingForKey(1);

    assert p2 != null;
    assert 0 == countingCache.getLookupMappingHitCount();
    assert s2.getId().equals(p2.getShard().getId());
  }

  /**
   * Update location of existing point mapping in list shard map with idemptency checks.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void updatePointMappingIdempotency() {
    // TODO:
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), new StoreOperationFactory(), new CacheStore(),
        ShardMapManagerLoadPolicy.Lazy, RetryPolicy.getDefaultRetryPolicy(),
        RetryBehavior.getDefaultRetryBehavior());

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTest.listShardMapName);

    assert lsm != null;

    Shard s1 = lsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]));
    assert s1 != null;

    Shard s2 = lsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[1]));
    assert s2 != null;

    PointMapping p1 = lsm.createPointMapping(1, s1);

    // Online -> Offline - No Location Change
    PointMappingUpdate pu = new PointMappingUpdate();
    pu.setStatus(MappingStatus.Offline);

    PointMapping presult = lsm.updateMapping(p1, pu);
    assert presult != null;
    assert presult.getStatus() == MappingStatus.Offline;

    // Offline -> Offline - No Location Change
    pu = new PointMappingUpdate();
    pu.setStatus(MappingStatus.Offline);

    presult = lsm.updateMapping(presult, pu);
    assert presult != null;
    assert presult.getStatus() == MappingStatus.Offline;

    // Offline -> Offline - Location Change
    pu = new PointMappingUpdate();
    pu.setShard(s2);

    presult = lsm.updateMapping(presult, pu);
    assert presult != null;
    assert presult.getStatus() == MappingStatus.Offline;
    assert s2.getLocation() == presult.getShard().getLocation();

    // Offline -> Online - No Location Change
    pu = new PointMappingUpdate();
    pu.setStatus(MappingStatus.Online);

    presult = lsm.updateMapping(presult, pu);
    assert presult != null;
    assert presult.getStatus() == MappingStatus.Online;

    // Online -> Offline - Location Change
    pu = new PointMappingUpdate();
    pu.setStatus(MappingStatus.Offline);
    pu.setShard(s1);

    presult = lsm.updateMapping(presult, pu);
    assert presult != null;
    assert presult.getStatus() == MappingStatus.Offline;
    assert s1.getLocation() == presult.getShard().getLocation();

    // Offline -> Online - Location Change
    pu = new PointMappingUpdate();
    pu.setStatus(MappingStatus.Online);
    pu.setShard(s2);

    presult = lsm.updateMapping(presult, pu);
    assert presult != null;
    assert presult.getStatus() == MappingStatus.Online;
    assert s2.getLocation() == presult.getShard().getLocation();

    // Online -> Online - No Location Change
    pu = new PointMappingUpdate();
    pu.setStatus(MappingStatus.Online);

    presult = lsm.updateMapping(presult, pu);
    assert presult != null;
    assert presult.getStatus() == MappingStatus.Online;

    // Online -> Online - Location Change
    pu = new PointMappingUpdate();
    pu.setShard(s1);

    boolean failed = false;

    try {
      presult = lsm.updateMapping(presult, pu);
    } catch (ShardManagementException sme) {
      failed = true;
      assert ShardManagementErrorCategory.ListShardMap == sme.getErrorCategory();
      assert ShardManagementErrorCode.MappingIsNotOffline == sme.getErrorCode();
    }

    assert failed;
  }

  /**
   * Add a range mapping to range shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void addRangeMappingDefault() throws SQLException {
    addRangeMappingDefault(ShardKeyInfo.allTestShardKeyValues.subList(0, 7));
    addRangeMappingDefault(ShardKeyInfo.allTestShardKeyValues.subList(8, 15));
    addRangeMappingDefault(ShardKeyInfo.allTestShardKeyValues.subList(16, 18));
    // TODO:
    // AddRangeMappingDefault(ShardKeyInfo.allTestShardKeyValues.<byte[]>OfType().ToArray());
    // AddRangeMappingDefault(ShardKeyInfo.allTestShardKeyValues.<java.time.LocalDateTime>OfType()
    // .ToArray());
    // AddRangeMappingDefault(ShardKeyInfo.allTestShardKeyValues.<DateTimeOffset>OfType()
    // .ToArray());
    // AddRangeMappingDefault(ShardKeyInfo.allTestShardKeyValues.<TimeSpan>OfType().ToArray());
  }

  /**
   * Add multiple range mapping to range shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void addRangeMappingMultiple() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTest.rangeShardMapName);

    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(0, 10), s);
    assert r1 != null;

    RangeMapping r2 = rsm.createRangeMapping(new Range(20, 30), s);
    assert r2 != null;

    boolean addFailed = false;

    int[][] ranges = new int[][]{{5, 15}, {5, 7}, {-5, 5}, {-5, 15}, {15, 25},
        {Integer.MIN_VALUE, Integer.MAX_VALUE}};

    for (int i = 0; i < 6; i++) {
      try {
        addFailed = false;
        RangeMapping r3 = rsm.createRangeMapping(new Range(ranges[i][0], ranges[i][1]), s);
      } catch (ShardManagementException sme) {
        assert ShardManagementErrorCategory.RangeShardMap == sme.getErrorCategory();
        assert ShardManagementErrorCode.MappingRangeAlreadyMapped == sme.getErrorCode();
        addFailed = true;
      }
      assert addFailed;
    }

    RangeMapping r4 = rsm.createRangeMapping(new Range(10, 20), s);
    assert r4 != null;
  }

  /**
   * Exercise IEquatable for shard objects.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testEquatableForShards() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTest.rangeShardMapName);

    assert rsm != null;

    ShardLocation sl1 =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]);

    ShardLocation sl2 =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[1]);

    Shard s1 = rsm.createShard(sl1);
    assert s1 != null;

    Shard s2 = rsm.createShard(sl2);
    assert s2 != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(0, 10), s1);
    assert r1 != null;

    RangeMapping r2 = rsm.createRangeMapping(new Range(10, 20), s2);
    assert r2 != null;

    RangeMapping r3 = rsm.createRangeMapping(new Range(20, 30), s1);
    assert r3 != null;

    Shard shardLookup = (rsm.getMappingForKey(5)).getShard();

    assert !shardLookup.equals(s1);

    assert !shardLookup.equals(s2);

    List<Shard> myShardSelection = rsm.getMappings(new Range(0, 300)).stream()
        .map(RangeMapping::getShard).distinct().collect(Collectors.toList());

    assert myShardSelection.size() == 2;
  }

  /**
   * Add a range mapping to cover entire range in range shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void addRangeMappingEntireRange() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTest.rangeShardMapName);

    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(Integer.MIN_VALUE), s);

    assert r1 != null;
  }

  /**
   * Add a range mapping to cover entire range in range shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void addRangeMappingTestBoundaries() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTest.rangeShardMapName);

    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    int[][] ranges = new int[][]{{Integer.MIN_VALUE, Integer.MIN_VALUE + 1},
        {Integer.MIN_VALUE + 1, Integer.MAX_VALUE - 1}, {Integer.MAX_VALUE - 1, Integer.MAX_VALUE}};

    for (int i = 0; i < 3; i++) {
      RangeMapping r = rsm.createRangeMapping(new Range(ranges[i][0], ranges[i][1]), s);
      assert r != null;
    }

    // Add range [2147483647, +inf). This range is actually representing a single point int.MaxValue
    RangeMapping r1 = rsm.createRangeMapping(new Range(Integer.MAX_VALUE), s);
    assert r1 != null;
  }

  /**
   * Add a duplicate range mapping to range shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void addRangeMappingDuplicate() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTest.rangeShardMapName);

    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);

    assert r1 != null;

    ShardManagementException exception = AssertExtensions
        .assertThrows(() -> rsm.createRangeMapping(new Range(1, 10), s));

    assertTrue(exception.getErrorCode() == ShardManagementErrorCode.MappingRangeAlreadyMapped
        && exception.getErrorCategory() == ShardManagementErrorCategory.RangeShardMap);
  }

  /**
   * Delete existing range mapping from range shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void deleteRangeMappingDefault() {
    CountingCacheStore countingCache = new CountingCacheStore(new CacheStore());

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), new StoreOperationFactory(), countingCache,
        ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO, Duration.ZERO,
        Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTest.rangeShardMapName);

    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);

    assert r1 != null;

    MappingLockToken mappingLockToken = MappingLockToken.create();
    rsm.lockMapping(r1, mappingLockToken);

    RangeMapping rmLookup = rsm.getMappingForKey(1);

    assert rmLookup != null;
    assert 0 == countingCache.getLookupMappingHitCount();

    // The mapping must be made offline first before it can be deleted.
    RangeMappingUpdate ru = new RangeMappingUpdate();
    ru.setStatus(MappingStatus.Offline);

    // Should throw if the correct lock owner id isn't passed
    // TODO:updateMapping with RangeMapping and Range
    ShardManagementException exception =
        AssertExtensions.assertThrows(() -> rsm.updateMapping(r1, ru));

    assert exception.getErrorCode() == ShardManagementErrorCode.MappingLockOwnerIdDoesNotMatch
        && exception.getErrorCategory() == ShardManagementErrorCategory.RangeShardMap;

    RangeMapping mappingToDelete = rsm.updateMapping(r1, ru, mappingLockToken);

    exception = AssertExtensions
        .assertThrows(() -> rsm.deleteMapping(mappingToDelete));

    assert exception.getErrorCode() == ShardManagementErrorCode.MappingLockOwnerIdDoesNotMatch
        && exception.getErrorCategory() == ShardManagementErrorCategory.RangeShardMap;

    rsm.deleteMapping(mappingToDelete, mappingLockToken);

    exception =
        AssertExtensions.assertThrows(() -> rsm.getMappingForKey(1));

    assert exception.getErrorCode() == ShardManagementErrorCode.MappingNotFoundForKey
        && exception.getErrorCategory() == ShardManagementErrorCategory.RangeShardMap;

    assert 0 == countingCache.getLookupMappingMissCount();
  }

  /**
   * Delete non-existing range mapping from range shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void deleteRangeMappingNonExisting() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTest.rangeShardMapName);

    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);

    assert r1 != null;

    RangeMappingUpdate ru = new RangeMappingUpdate();
    ru.setStatus(MappingStatus.Offline);

    // The mapping must be made offline before it can be deleted.
    r1 = rsm.updateMapping(r1, ru);

    rsm.deleteMapping(r1);

    boolean removeFailed = tryDeleteRangeMapping(rsm, r1);
    assert removeFailed;
  }

  /**
   * Delete range mapping with version mismatch from range shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void deleteRangeMappingVersionMismatch() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTest.rangeShardMapName);

    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);

    assert r1 != null;

    RangeMappingUpdate ru = new RangeMappingUpdate();
    ru.setStatus(MappingStatus.Offline);

    // upate range mapping to change version
    RangeMapping rmNew = rsm.updateMapping(r1, ru);
    assert rmNew != null;

    boolean removeFailed = tryDeleteRangeMapping(rsm, r1);
    assert removeFailed;
  }

  // TODO: Take a mapping offline, verify that the existing connection is killed.

  /**
   * Update range mapping in range shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void updateRangeMappingDefault() {
    CountingCacheStore countingCache = new CountingCacheStore(new CacheStore());

    // TODO:RetryPolicy
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), new StoreOperationFactory(), countingCache,
        ShardMapManagerLoadPolicy.Lazy, RetryPolicy.getDefaultRetryPolicy(),
        RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTest.rangeShardMapName);

    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 20), s);

    // Lock the mapping
    MappingLockToken mappingLockToken = MappingLockToken.create();
    rsm.lockMapping(r1, mappingLockToken);

    RangeMappingUpdate ru = new RangeMappingUpdate();
    ru.setStatus(MappingStatus.Offline);

    RangeMapping rmNew = rsm.updateMapping(r1, ru, mappingLockToken);

    assert rmNew != null;

    MappingLockToken storeMappingLockToken = rsm.getMappingLockOwner(rmNew);
    assertEquals(storeMappingLockToken, mappingLockToken);

    rsm.unlockMapping(rmNew, mappingLockToken);
    RangeMapping r2 = rsm.getMappingForKey(1);
    assert 0 == countingCache.getLookupMappingHitCount();
    assert r1.getId() != r2.getId();
  }

  /**
   * Update range mapping in range shard map to change location.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void updateRangeMappingLocation() {
    CountingCacheStore countingCache = new CountingCacheStore(new CacheStore());

    // TODO:RetryPolicy
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), new StoreOperationFactory(), countingCache,
        ShardMapManagerLoadPolicy.Lazy, RetryPolicy.getDefaultRetryPolicy(),
        RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTest.rangeShardMapName);

    assert rsm != null;

    ShardLocation sl1 =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]);
    Shard s1 = rsm.createShard(sl1);
    assert s1 != null;

    ShardLocation sl2 =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[1]);
    Shard s2 = rsm.createShard(sl2);
    assert s2 != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 20), s1);

    RangeMappingUpdate ru = new RangeMappingUpdate();

    // Shard location in a mapping cannot be updated when online.
    ru.setStatus(MappingStatus.Offline);
    RangeMapping rmOffline = rsm.updateMapping(r1, ru);

    assert rmOffline != null;
    assert ru.getStatus() == rmOffline.getStatus();
    ru.setShard(s2);

    RangeMapping rmNew = rsm.updateMapping(rmOffline, ru);
    assert rmNew != null;

    // Bring the mapping back online.
    ru.setStatus(MappingStatus.Online);

    rmNew = rsm.updateMapping(rmNew, ru);
    assert rmNew != null;

    RangeMapping r2 = rsm.getMappingForKey(1);

    assert r2 != null;
    assert 0 == countingCache.getLookupMappingHitCount();
    assertEquals(s2.getId(), r2.getShard().getId());
  }

  /**
   * Update location of existing point mapping in list shard map with idemptency checks.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void updateRangeMappingIdempotency() {
    // TODO:RetryPolicy
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), new StoreOperationFactory(), new CacheStore(),
        ShardMapManagerLoadPolicy.Lazy, RetryPolicy.getDefaultRetryPolicy(),
        RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTest.rangeShardMapName);

    assert rsm != null;

    Shard s1 = rsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]));
    assert s1 != null;

    Shard s2 = rsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[1]));
    assert s2 != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 20), s1);

    // Online -> Offline - No Location Change
    RangeMappingUpdate pu = new RangeMappingUpdate();
    pu.setStatus(MappingStatus.Offline);

    RangeMapping presult = rsm.updateMapping(r1, pu);
    assert presult != null;
    assert presult.getStatus() == MappingStatus.Offline;

    // Offline -> Offline - No Location Change
    pu = new RangeMappingUpdate();
    pu.setStatus(MappingStatus.Offline);

    presult = rsm.updateMapping(presult, pu);
    assert presult != null;
    assert presult.getStatus() == MappingStatus.Offline;

    // Offline -> Offline - Location Change
    pu = new RangeMappingUpdate();
    pu.setShard(s2);

    presult = rsm.updateMapping(presult, pu);
    assert presult != null;
    assert presult.getStatus() == MappingStatus.Offline;
    assert s2.getLocation() == presult.getShard().getLocation();

    // Offline -> Online - No Location Change
    pu = new RangeMappingUpdate();
    pu.setStatus(MappingStatus.Online);

    presult = rsm.updateMapping(presult, pu);
    assert presult != null;
    assert presult.getStatus() == MappingStatus.Online;

    // Online -> Offline - Location Change
    pu = new RangeMappingUpdate();
    pu.setStatus(MappingStatus.Offline);
    pu.setShard(s1);

    presult = rsm.updateMapping(presult, pu);
    assert presult != null;
    assert presult.getStatus() == MappingStatus.Offline;
    assert s1.getLocation() == presult.getShard().getLocation();

    // Offline -> Online - Location Change
    pu = new RangeMappingUpdate();
    pu.setStatus(MappingStatus.Online);
    pu.setShard(s2);

    presult = rsm.updateMapping(presult, pu);
    assert presult != null;
    assert presult.getStatus() == MappingStatus.Online;
    assert s2.getLocation() == presult.getShard().getLocation();

    // Online -> Online - No Location Change
    pu = new RangeMappingUpdate();
    pu.setStatus(MappingStatus.Online);

    presult = rsm.updateMapping(presult, pu);
    assert presult != null;
    assert presult.getStatus() == MappingStatus.Online;

    // Online -> Online - Location Change
    pu = new RangeMappingUpdate();
    pu.setShard(s1);

    boolean failed = false;

    try {
      presult = rsm.updateMapping(presult, pu);
    } catch (ShardManagementException sme) {
      failed = true;
      assert ShardManagementErrorCategory.RangeShardMap == sme.getErrorCategory();
      assert ShardManagementErrorCode.MappingIsNotOffline == sme.getErrorCode();
    }

    assert failed;
  }

  /**
   * All combinations of getting range mappings from a range shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void getRangeMappingsForRange() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTest.rangeShardMapName);

    assert rsm != null;

    Shard s1 = rsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]));
    assert s1 != null;

    Shard s2 = rsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[1]));
    assert s2 != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s1);
    assert r1 != null;

    RangeMapping r2 = rsm.createRangeMapping(new Range(10, 20), s2);
    assert r2 != null;

    RangeMapping r3 = rsm.createRangeMapping(new Range(20, 30), s1);
    assert r3 != null;

    // Get all mappings in shard map.
    List<RangeMapping> allMappings = rsm.getMappings();
    assert 3 == allMappings.size();

    // Get all mappings in specified range.
    List<RangeMapping> mappingsInRange = rsm.getMappings(new Range(1, 15));
    assert 2 == mappingsInRange.size();

    // Get all mappings for a shard.
    List<RangeMapping> mappingsForShard = rsm.getMappings(s1);
    assert 2 == mappingsForShard.size();

    // Get all mappings in specified range for a particular shard.
    List<RangeMapping> mappingsInRangeForShard = rsm.getMappings(new Range(1, 15), s1);
    assert 1 == mappingsInRangeForShard.size();
  }

  /**
   * Split existing range mapping in range shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void splitRangeDefault() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTest.rangeShardMapName);

    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 20), s);

    // Lock the mapping
    MappingLockToken mappingLockToken = MappingLockToken.create();
    rsm.lockMapping(r1, mappingLockToken);

    // Should throw if the correct lock owner id isn't passed
    ShardManagementException exception =
        AssertExtensions.assertThrows(() -> rsm.splitMapping(r1, 5));
    assertTrue(exception.getErrorCode() == ShardManagementErrorCode.MappingLockOwnerIdDoesNotMatch
        && exception.getErrorCategory() == ShardManagementErrorCategory.RangeShardMap);

    List<RangeMapping> rmList = rsm.splitMapping(r1, 5, mappingLockToken);

    assert 2 == rmList.size();

    for (RangeMapping r : rmList) {
      assert r != null;
      assertEquals(mappingLockToken, rsm.getMappingLockOwner(r));

      // Unlock each mapping and verify
      rsm.unlockMapping(r, mappingLockToken);
      assert MappingLockToken.opEquality(MappingLockToken.NoLock, rsm.getMappingLockOwner(r));
    }
  }

  /**
   * Split existing range mapping at boundary in range shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void splitRangeBoundary() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTest.rangeShardMapName);

    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 20), s);

    // Lock the mapping
    MappingLockToken mappingLockToken = MappingLockToken.create();
    rsm.lockMapping(r1, mappingLockToken);

    IllegalArgumentException exception = AssertExtensions
        .assertThrows(() -> rsm.splitMapping(r1, 1, mappingLockToken));

    // Unlock mapping
    rsm.unlockMapping(r1, mappingLockToken);
  }

  /**
   * Split a range at point outside range in range shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void splitRangeOutside() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTest.rangeShardMapName);

    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 20), s);

    IllegalArgumentException exception =
        AssertExtensions.assertThrows(() -> rsm.splitMapping(r1, 31));
  }

  /**
   * Merge adjacent range mappings in range shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void mergeRangeMappingsDefault() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTest.rangeShardMapName);

    assert rsm != null;

    Shard s1 = rsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]));
    assert s1 != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s1);
    MappingLockToken mappingLockTokenLeft = MappingLockToken.create();
    rsm.lockMapping(r1, mappingLockTokenLeft);

    RangeMapping r2 = rsm.createRangeMapping(new Range(10, 20), s1);
    MappingLockToken mappingLockTokenRight = MappingLockToken.create();
    rsm.lockMapping(r2, mappingLockTokenRight);

    // Should throw if the correct lock owner id isn't passed
    ShardManagementException exception =
        AssertExtensions.assertThrows(() -> rsm.mergeMappings(r1, r2));
    assertTrue("Expected MappingLockOwnerIdDoesNotMatch error when Updating mapping!",
        exception.getErrorCode() == ShardManagementErrorCode.MappingLockOwnerIdDoesNotMatch
            && exception.getErrorCategory() == ShardManagementErrorCategory.RangeShardMap);

    // Pass in an incorrect right lockowner id
    exception = AssertExtensions.assertThrows(
        () -> rsm.mergeMappings(r1, r2, MappingLockToken.NoLock, mappingLockTokenRight));
    assertTrue("Expected MappingLockOwnerIdDoesNotMatch error when Updating mapping!",
        exception.getErrorCode() == ShardManagementErrorCode.MappingLockOwnerIdDoesNotMatch
            && exception.getErrorCategory() == ShardManagementErrorCategory.RangeShardMap);

    RangeMapping rmMerged = rsm.mergeMappings(r1, r2, mappingLockTokenLeft, mappingLockTokenRight);

    assert rmMerged != null;

    MappingLockToken storeMappingLockToken = rsm.getMappingLockOwner(rmMerged);

    assertEquals("Expected merged mapping lock id to equal left mapping id!", storeMappingLockToken,
        mappingLockTokenLeft);
    rsm.unlockMapping(rmMerged, storeMappingLockToken);
    storeMappingLockToken = rsm.getMappingLockOwner(rmMerged);
    assertEquals("Expected merged mapping lock id to equal default mapping id after unlock!",
        storeMappingLockToken.getLockOwnerId(), MappingLockToken.NoLock.getLockOwnerId());
  }

  /**
   * Merge adjacent range mappings with different location in range shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void mergeRangeMappingsDifferentLocation() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTest.rangeShardMapName);

    assert rsm != null;

    Shard s1 = rsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]));
    assert s1 != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s1);

    Shard s2 = rsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[1]));
    assert s2 != null;

    RangeMapping r2 = rsm.createRangeMapping(new Range(10, 20), s2);

    IllegalArgumentException exception =
        AssertExtensions.assertThrows(() -> rsm.mergeMappings(r1, r2));
  }

  /**
   * Merge non-adjacent range mappings in range shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void mergeRangeMappingsNonAdjacent() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTest.rangeShardMapName);

    assert rsm != null;

    Shard s1 = rsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]));
    assert s1 != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s1);

    RangeMapping r2 = rsm.createRangeMapping(new Range(15, 20), s1);

    IllegalArgumentException exception =
        AssertExtensions.assertThrows(() -> rsm.mergeMappings(r1, r2));
  }

  /**
   * Basic test to lock range mappings that - Creates a mapping and locks it - Verifies look-up APIs
   * work as expected - Unlock works as expected.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void lockOrUnlockRangeMappingBasic() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTest.rangeShardMapName);

    assert rsm != null;

    Shard s1 = rsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]));
    assert s1 != null;

    // Create a range mapping
    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 20), s1);

    // Lock the mapping
    // Try to lock with an invalid owner id first
    IllegalArgumentException argException =
        AssertExtensions.assertThrows(() -> rsm.lockMapping(r1,
            new MappingLockToken(MappingLockToken.ForceUnlock.getLockOwnerId())));

    MappingLockToken mappingLockToken = MappingLockToken.create();
    rsm.lockMapping(r1, mappingLockToken);

    // Trying to lock it again should result in an exception
    ShardManagementException exception = AssertExtensions
        .assertThrows(() -> rsm.lockMapping(r1, mappingLockToken));
    assertTrue("Expected MappingIsAlreadyLocked error!",
        exception.getErrorCode() == ShardManagementErrorCode.MappingIsAlreadyLocked
            && exception.getErrorCategory() == ShardManagementErrorCategory.RangeShardMap);

    // Lookup should work without a lockownerId
    RangeMapping r1LookUp = rsm.getMappingForKey(5);
    assertEquals("Expected range mappings to be equal!", r1, r1LookUp);

    // Try to unlock the mapping with the wrong lock owner id
    exception = AssertExtensions.assertThrows(
        () -> rsm.unlockMapping(r1, MappingLockToken.NoLock));

    assertTrue(String.format("Expected MappingLockOwnerIdDoesNotMatch error. Found: ErrorCode: %1$s"
            + " ErrorCategory: %2$s!", exception.getErrorCode(),
        ShardManagementErrorCategory.RangeShardMap), exception.getErrorCode() ==
        ShardManagementErrorCode.MappingLockOwnerIdDoesNotMatch && exception.getErrorCategory() ==
        ShardManagementErrorCategory.RangeShardMap);

    rsm.unlockMapping(r1, mappingLockToken);
  }

  /**
   * Basic test to lock range mappings that - Creates a mapping and locks it - Verifies look-up APIs
   * work as expected - Unlock works as expected.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void lockOrUnlockListMappingBasic() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ListShardMap<Integer> rsm = smm.getListShardMap(ShardMapperTest.listShardMapName);
    assert rsm != null;

    Shard s1 = rsm.createShard(new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTest.shardedDBs[0]));
    assert s1 != null;

    // Create a range mapping
    PointMapping r1 = rsm.createPointMapping(1, s1);

    // Lock the mapping. Try to lock with an invalid owner id first.
    AssertExtensions.assertThrows(() -> rsm.lockMapping(r1,
        new MappingLockToken(MappingLockToken.ForceUnlock.getLockOwnerId())));

    MappingLockToken mappingLockToken = MappingLockToken.create();
    rsm.lockMapping(r1, mappingLockToken);

    // Trying to lock it again should result in an exception
    ShardManagementException exception = AssertExtensions
        .assertThrows(() -> rsm.lockMapping(r1, mappingLockToken));
    assertTrue("Expected MappingIsAlreadyLocked error!",
        exception.getErrorCode() == ShardManagementErrorCode.MappingIsAlreadyLocked
            && exception.getErrorCategory() == ShardManagementErrorCategory.ListShardMap);

    // Lookup should work without a lockOwnerId
    PointMapping r1LookUp = rsm.getMappingForKey(1);
    assertEquals("Expected range mappings to be equal!", r1, r1LookUp);

    // Try to unlock the mapping with the wrong lock owner id
    exception = AssertExtensions.assertThrows(
        () -> rsm.unlockMapping(r1, MappingLockToken.NoLock));
    assertTrue(exception.getErrorCode() == ShardManagementErrorCode.MappingLockOwnerIdDoesNotMatch
        && exception.getErrorCategory() == ShardManagementErrorCategory.ListShardMap);
    // TODO:assertTrue(string,condition,Object[])

    rsm.unlockMapping(r1, mappingLockToken);
  }

  /**
   * Test the Unlock API that unlocks all mappings that belong to a given lock owner id.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void unlockAllMappingsWithLockOwnerId() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTest.rangeShardMapName);

    assert rsm != null;

    Shard s1 = rsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]));
    assert s1 != null;

    // Create a few mappings and lock some of them
    List<RangeMapping> mappings = new ArrayList<>();
    MappingLockToken mappingLockToken = MappingLockToken.create();

    for (int i = 0; i < 100; i += 10) {
      RangeMapping mapping = rsm.createRangeMapping(new Range(i, i + 10), s1);
      mappings.add(mapping);

      if (mappings.size() < 5) {
        rsm.lockMapping(mapping, mappingLockToken);
      }
    }

    // Unlock all of them
    rsm.unlockMapping(mappingLockToken);

    for (RangeMapping mapping : mappings) {
      assertEquals("Expected all mappings to be unlocked!", MappingLockToken.NoLock,
          rsm.getMappingLockOwner(mapping));
    }
  }

  /**
   * Test the Unlock API that unlocks all mappings that belong to a given lock owner id.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void unlockAllMappingsListMapWithLockOwnerId() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ListShardMap<Integer> rsm = smm.getListShardMap(ShardMapperTest.listShardMapName);

    assert rsm != null;

    Shard s1 = rsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]));
    assert s1 != null;

    // Create a few mappings and lock some of them
    ArrayList<PointMapping> mappings = new ArrayList<>();
    MappingLockToken mappingLockToken = MappingLockToken.create();

    for (int i = 0; i < 100; i += 10) {
      PointMapping mapping = rsm.createPointMapping(i, s1);
      mappings.add(mapping);

      if (mappings.size() < 5) {
        rsm.lockMapping(mapping, mappingLockToken);
      }
    }

    // Unlock all of them
    rsm.unlockMapping(mappingLockToken);

    for (PointMapping mapping : mappings) {
      assertEquals("Expected all mappings to be unlocked!", MappingLockToken.NoLock,
          rsm.getMappingLockOwner(mapping));
    }
  }

  /**
   * Mark a point mapping offline or online.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void markMappingOfflineOnline() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTest.listShardMapName);

    assert lsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]);

    Shard s = lsm.createShard(sl);

    assert s != null;

    PointMapping p1 = lsm.createPointMapping(1, s);

    PointMapping pmNew = lsm.markMappingOffline(p1);

    assert pmNew != null;
    assertEquals("The point mapping was not successfully marked offline.", MappingStatus.Offline,
        pmNew.getStatus());

    pmNew = lsm.markMappingOnline(pmNew);

    assert pmNew != null;
    assertEquals("The point mapping was not successfully marked online.", MappingStatus.Online,
        pmNew.getStatus());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTest.rangeShardMapName);

    assert rsm != null;

    s = rsm.createShard(sl);
    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 5), s);

    RangeMapping rmNew = rsm.markMappingOffline(r1);

    assert rmNew != null;
    assertEquals("The range mapping was not successfully marked offline.", MappingStatus.Offline,
        rmNew.getStatus());

    rmNew = rsm.markMappingOnline(rmNew);

    assert rmNew != null;
    assertEquals("The range mapping was not successfully marked online.", MappingStatus.Online,
        rmNew.getStatus());
  }

  private <T> void addPointMappingDefault(List<T> keysToTest) throws SQLException {
    CountingCacheStore countingCache = new CountingCacheStore(new CacheStore());

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), new StoreOperationFactory(), countingCache,
        ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO, Duration.ZERO,
        Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    ShardKeyType type = ShardKey.shardKeyTypeFromType(keysToTest.get(0).getClass());
    ListShardMap<T> lsm =
        smm.createListShardMap(String.format("AddPointMappingDefault%1$s", type.name()), type);
    assert lsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]);
    Shard s = lsm.createShard(sl);
    assert s != null;

    for (T key : keysToTest) {
      System.out.printf("Key: %1$s" + "\r\n", key);

      PointMapping p1 = lsm.createPointMapping(key, s);
      assert p1 != null;
      assertEquals(key, p1.getValue());

      PointMapping p2 = lsm.getMappingForKey(key);
      assert p2 != null;
      assertEquals(key, p2.getValue());

      assert 0 == countingCache.getLookupMappingCount();
      assert 0 == countingCache.getLookupMappingHitCount();

      // Validate mapping by trying to connect
      try (Connection conn =
          lsm.openConnection(p1, Globals.SHARD_USER_CONN_STRING, ConnectionOptions.Validate)) {
        conn.close();
      }
    }
  }

  private <T> void addRangeMappingDefault(List<T> keysToTest) throws SQLException {
    CountingCacheStore countingCache = new CountingCacheStore(new CacheStore());

    // TODO:RetryPolicy
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), new StoreOperationFactory(), countingCache,
        ShardMapManagerLoadPolicy.Lazy, RetryPolicy.getDefaultRetryPolicy(),
        RetryBehavior.getDefaultRetryBehavior());

    assert 0 < keysToTest.size();

    ShardKeyType type = ShardKey.shardKeyTypeFromType(keysToTest.get(0).getClass());
    RangeShardMap<T> rsm =
        smm.createRangeShardMap(String.format("AddRangeMappingDefault%1$s", type.name()), type);
    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTest.shardedDBs[0]);
    Shard s = rsm.createShard(sl);
    assert s != null;

    for (int i = 0; i < keysToTest.size() - 1; i++) {
      // https://github.com/Azure/elastic-db-tools/issues/117
      // Bug? DateTimeOffsets with the same universal time but different offset are equal as
      // ShardKeys.
      // According to SQL (and our normalization format), they should be unequal, although according
      // to .NET they should be equal.
      // We need to skip empty ranges because if we use them in this test then we end up with
      // duplicate mappings
      // TODO if (T.class == DateTimeOffset.class && (DateTimeOffset)(Object)keysToTest.get(i) ==
      // (DateTimeOffset)(Object)keysToTest.get(i + 1))
      // {
      // System.out.printf("Skipping %1$s == %2$s" + "\r\n", keysToTest.get(i), keysToTest.get(i +
      // 1));
      // continue;
      // }

      Range range = new Range(keysToTest.get(i), keysToTest.get(i + 1));
      System.out.printf("Range: %1$s" + "\r\n", range);

      RangeMapping p1 = rsm.createRangeMapping(range, s);
      assert p1 != null;
      assertEquals(range, p1.getValue());

      RangeMapping p2 = rsm.getMappingForKey((T) range.getLow());
      assert p2 != null;
      assertEquals(range, p2.getValue());

      assert 0 == countingCache.getLookupMappingCount();
      assert 0 == countingCache.getLookupMappingHitCount();

      // Validate mapping by trying to connect
      try (Connection conn =
          rsm.openConnection(p1, Globals.SHARD_USER_CONN_STRING, ConnectionOptions.Validate)) {
        conn.close();
      }
    }
  }

  private boolean tryDeletePointMapping(ListShardMap<Integer> lsm, PointMapping p1) {
    try {
      lsm.deleteMapping(p1);
      return false;
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ListShardMap == sme.getErrorCategory();
      assert ShardManagementErrorCode.MappingDoesNotExist == sme.getErrorCode();
      return true;
    }
  }

  private boolean tryDeleteRangeMapping(RangeShardMap<Integer> rsm, RangeMapping r1) {
    try {
      rsm.deleteMapping(r1);
      return false;
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.RangeShardMap == sme.getErrorCategory();
      assert ShardManagementErrorCode.MappingDoesNotExist == sme.getErrorCode();
      return true;
    }
  }
}
