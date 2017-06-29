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
import com.microsoft.azure.elasticdb.shard.cache.CacheStoreMappingUpdatePolicy;
import com.microsoft.azure.elasticdb.shard.cache.ICacheStoreMapping;
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
import com.microsoft.azure.elasticdb.shard.store.IUserStoreConnection;
import com.microsoft.azure.elasticdb.shard.store.StoreConnectionKind;
import com.microsoft.azure.elasticdb.shard.store.StoreException;
import com.microsoft.azure.elasticdb.shard.store.StoreLogEntry;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.store.StoreTransactionScopeKind;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperation;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationCode;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationFactory;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func1Param;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func2Param;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func4Param;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func5Param;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func7Param;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func8Param;
import com.microsoft.azure.elasticdb.shard.stubs.StubAddMappingOperation;
import com.microsoft.azure.elasticdb.shard.stubs.StubCacheStore;
import com.microsoft.azure.elasticdb.shard.stubs.StubFindMappingByKeyGlobalOperation;
import com.microsoft.azure.elasticdb.shard.stubs.StubICacheStoreMapping;
import com.microsoft.azure.elasticdb.shard.stubs.StubRemoveMappingOperation;
import com.microsoft.azure.elasticdb.shard.stubs.StubReplaceMappingsOperation;
import com.microsoft.azure.elasticdb.shard.stubs.StubSqlStoreConnectionFactory;
import com.microsoft.azure.elasticdb.shard.stubs.StubStoreOperationFactory;
import com.microsoft.azure.elasticdb.shard.stubs.StubUpdateMappingOperation;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class ShardMapperTests {

  /**
   * Sharded databases to create for the test.
   */
  private static String[] shardDBs = new String[]{"shard1", "shard2"};

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
    ListShardMap<Integer> lsm;
    ReferenceObjectHelper<ListShardMap<Integer>> refLsm =
        new ReferenceObjectHelper<>(null);
    if (smm.tryGetListShardMap(ShardMapperTests.listShardMapName, ShardKeyType.Int32, refLsm)) {
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
    }

    // Remove all existing mappings from the range shard map.
    RangeShardMap<Integer> rsm;
    ReferenceObjectHelper<RangeShardMap<Integer>> refRsm =
        new ReferenceObjectHelper<>(null);
    if (smm.tryGetRangeShardMap(ShardMapperTests.rangeShardMapName, ShardKeyType.Int32, refRsm)) {
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
    }
  }

  /**
   * Initializes common state for tests in this class.
   */
  @BeforeClass
  public static void shardMapperTestsInitialize() throws SQLException {
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);
      try (Statement stmt = conn.createStatement()) {
        // Create ShardMapManager database
        String query =
            String.format(Globals.CREATE_DATABASE_QUERY, Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
        stmt.execute(query);
      } catch (SQLException ex) {
        ex.printStackTrace();
      }

      // Create shard databases
      for (int i = 0; i < ShardMapperTests.shardDBs.length; i++) {
        try (Statement stmt = conn.createStatement()) {
          String query =
              String.format(Globals.DROP_DATABASE_QUERY, ShardMapperTests.shardDBs[i]);
          stmt.execute(query);
        } catch (SQLException ex) {
          ex.printStackTrace();
        }
        try (Statement stmt = conn.createStatement()) {
          String query =
              String.format(Globals.CREATE_DATABASE_QUERY, ShardMapperTests.shardDBs[i]);
          stmt.execute(query);
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
          smm.createListShardMap(ShardMapperTests.listShardMapName, ShardKeyType.Int32);

      assert Objects.equals(ShardMapperTests.listShardMapName, lsm.getName());

      // Create range shard map.
      RangeShardMap<Integer> rsm =
          smm.createRangeShardMap(ShardMapperTests.rangeShardMapName, ShardKeyType.Int32);

      assert Objects.equals(ShardMapperTests.rangeShardMapName, rsm.getName());
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
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

      // Drop shard databases
      for (int i = 0; i < ShardMapperTests.shardDBs.length; i++) {
        try (Statement stmt = conn.createStatement()) {
          String query =
              String.format(Globals.DROP_DATABASE_QUERY, ShardMapperTests.shardDBs[i]);
          stmt.execute(query);
        } catch (SQLException ex) {
          ex.printStackTrace();
        }
      }

      // Drop shard map manager database
      try (Statement stmt = conn.createStatement()) {
        String query =
            String.format(Globals.DROP_DATABASE_QUERY, Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
        stmt.execute(query);
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
    ShardMapperTests.cleanShardMapsHelper();
  }

  /**
   * Cleans up common state per-test.
   */
  @After
  public void shardMapperTestCleanup() {
    ShardMapperTests.cleanShardMapsHelper();
  }

  /**
   * Shard map type conversion between list and range.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void shardMapTypeFailures() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    // Try to get list<int> shard map as range<int>
    try {
      smm.getRangeShardMap(ShardMapperTests.listShardMapName, ShardKeyType.Int32);
      fail("GetRangeShardMap did not throw as expected");
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ShardMapManager == sme.getErrorCategory();
      assert ShardManagementErrorCode.ShardMapTypeConversionError == sme.getErrorCode();
    }

    // Try to get range<int> shard map as list<int>
    try {
      smm.getListShardMap(ShardMapperTests.rangeShardMapName, ShardKeyType.Int32);
      fail("GetListShardMap did not throw as expected");
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ShardMapManager == sme.getErrorCategory();
      assert ShardManagementErrorCode.ShardMapTypeConversionError == sme.getErrorCode();
    }

    // Try to get list<int> shard map as list<guid>
    try {
      smm.getListShardMap(ShardMapperTests.listShardMapName, ShardKeyType.Guid);
      fail("GetListShardMap did not throw as expected");
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ShardMapManager == sme.getErrorCategory();
      assert ShardManagementErrorCode.ShardMapTypeConversionError == sme.getErrorCode();
    }

    // Try to get range<int> shard map as range<long>
    try {
      smm.getRangeShardMap(ShardMapperTests.rangeShardMapName, ShardKeyType.Int64);
      fail("GetRangeShardMap did not throw as expected");
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ShardMapManager == sme.getErrorCategory();
      assert ShardManagementErrorCode.ShardMapTypeConversionError == sme.getErrorCode();
    }

    // Try to get range<int> shard map as list<guid>
    try {
      smm.getListShardMap(ShardMapperTests.rangeShardMapName, ShardKeyType.Guid);
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
    addPointMapping(ShardKeyInfo.allTestShardKeyValues.subList(0, 7));
    addPointMapping(ShardKeyInfo.allTestShardKeyValues.subList(8, 15));
    addPointMapping(ShardKeyInfo.allTestShardKeyValues.subList(16, 18));
    // TODO:
    // AddPointMappingDefault(ShardKeyInfo.allTestShardKeyValues.<byte[]>OfType());
    // AddPointMappingDefault(ShardKeyInfo.allTestShardKeyValues.<LocalDateTime>OfType());
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

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTests.listShardMapName,
        ShardKeyType.Int32);

    assert lsm != null;

    Shard s1 = lsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]));
    assert s1 != null;

    Shard s2 = lsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[1]));
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

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTests.listShardMapName,
        ShardKeyType.Int32);

    assert lsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]);

    Shard s = lsm.createShard(sl);

    assert s != null;

    PointMapping p1 = lsm.createPointMapping(1, s);

    assert p1 != null;

    boolean addFailed = false;
    try {
      // add same point mapping again.
      lsm.createPointMapping(1, s);
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

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTests.listShardMapName,
        ShardKeyType.Int32);

    assert lsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]);

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

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTests.listShardMapName,
        ShardKeyType.Int32);

    assert lsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]);

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

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTests.listShardMapName,
        ShardKeyType.Int32);

    assert lsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]);

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

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTests.listShardMapName,
        ShardKeyType.Int32);

    assert lsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]);

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
  @Ignore
  public void killConnectionOnOfflinePointMapping() {
    ShardMapManager smm = new ShardMapManager(new SqlShardMapManagerCredentials(
        Globals.SHARD_MAP_MANAGER_CONN_STRING), new SqlStoreConnectionFactory(),
        new StoreOperationFactory(), new CacheStore(), ShardMapManagerLoadPolicy.Lazy,
        new RetryPolicy(1, Duration.ZERO, Duration.ZERO, Duration.ZERO),
        RetryBehavior.getDefaultRetryBehavior());

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTests.listShardMapName,
        ShardKeyType.Int32);

    assert lsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);

    Shard s = lsm.createShard(sl);

    assert s != null;

    PointMapping p1 = lsm.createPointMapping(1, s);

    try (Connection conn = lsm.openConnectionForKey(1, Globals.SHARD_USER_CONN_STRING)) {
      assert !conn.isClosed();

      PointMappingUpdate pu = new PointMappingUpdate();
      pu.setStatus(MappingStatus.Offline);

      PointMapping pmNew = lsm.updateMapping(p1, pu);
      assert pmNew != null;

      boolean failed = false;
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("select 1");
      } catch (SQLException e) {
        failed = true;
      }

      assert failed;
      assert conn.isClosed();

      failed = false;
      // Open 2nd connection.
      try (Connection conn2 = lsm.openConnectionForKey(1, Globals.SHARD_USER_CONN_STRING)) {
        conn2.close();
      } catch (Exception ex) {
        RuntimeException tempVar = (RuntimeException) ex.getCause();
        ShardManagementException sme = (ShardManagementException)
            ((tempVar instanceof ShardManagementException) ? tempVar : null);
        if (sme != null) {
          assert sme.getErrorCode().equals(ShardManagementErrorCode.MappingIsOffline);
          failed = true;
        }
      }

      assert failed;

      // Mark the mapping online again so that it will be cleaned up
      pu.setStatus(MappingStatus.Online);
      PointMapping pUpdated = lsm.updateMapping(pmNew, pu);

      assert pUpdated != null;

      failed = false;
      // Open 3rd connection. This should succeed.
      try (Connection conn3 = lsm.openConnectionForKey(1, Globals.SHARD_USER_CONN_STRING)) {
        conn3.close();
      } catch (ShardManagementException e2) {
        failed = true;
      }

      assert !failed;
    } catch (SQLException e) {
      e.printStackTrace();
    }
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
        ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO, Duration.ZERO,
        Duration.ZERO),
        RetryBehavior.getDefaultRetryBehavior());

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTests.listShardMapName,
        ShardKeyType.Int32);

    assert lsm != null;

    Shard s1 = lsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]));
    assert s1 != null;

    Shard s2 = lsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[1]));
    assert s2 != null;

    PointMapping p1 = lsm.createPointMapping(1, s1);

    PointMappingUpdate pu = new PointMappingUpdate();
    // Shard location in a mapping cannot be changed unless it is offline.
    pu.setStatus(MappingStatus.Offline);

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
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), new StoreOperationFactory(), new CacheStore(),
        ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO, Duration.ZERO,
        Duration.ZERO),
        RetryBehavior.getDefaultRetryBehavior());

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTests.listShardMapName,
        ShardKeyType.Int32);

    assert lsm != null;

    Shard s1 = lsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]));
    assert s1 != null;

    Shard s2 = lsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[1]));
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
      lsm.updateMapping(presult, pu);
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ListShardMap == sme.getErrorCategory();
      assert ShardManagementErrorCode.MappingIsNotOffline == sme.getErrorCode();
      failed = true;
    }

    assert failed;
  }

  /**
   * Add a range mapping to range shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void addRangeMappingDefault() throws SQLException {
    addRangeMapping(ShardKeyInfo.allTestShardKeyValues.subList(0, 7));
    addRangeMapping(ShardKeyInfo.allTestShardKeyValues.subList(8, 15));
    addRangeMapping(ShardKeyInfo.allTestShardKeyValues.subList(16, 18));
    // TODO:
    // AddRangeMappingDefault(ShardKeyInfo.allTestShardKeyValues.<byte[]>OfType().ToArray());
    // AddRangeMappingDefault(ShardKeyInfo.allTestShardKeyValues.<LocalDateTime>OfType()
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

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]);

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
        rsm.createRangeMapping(new Range(ranges[i][0], ranges[i][1]), s);
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

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl1 =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]);

    ShardLocation sl2 =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[1]);

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

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]);

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

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]);

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

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);

    assert r1 != null;

    ShardManagementException exception = AssertExtensions
        .assertThrows(() -> rsm.createRangeMapping(new Range(1, 10), s));

    assertTrue(exception.getErrorCode().equals(ShardManagementErrorCode.MappingRangeAlreadyMapped)
        && exception.getErrorCategory().equals(ShardManagementErrorCategory.RangeShardMap));
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

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]);

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

    assert exception.getErrorCode().equals(ShardManagementErrorCode.MappingLockOwnerIdDoesNotMatch)
        && exception.getErrorCategory().equals(ShardManagementErrorCategory.RangeShardMap);

    RangeMapping mappingToDelete = rsm.updateMapping(r1, ru, mappingLockToken);

    exception = AssertExtensions
        .assertThrows(() -> rsm.deleteMapping(mappingToDelete));

    assert exception.getErrorCode().equals(ShardManagementErrorCode.MappingLockOwnerIdDoesNotMatch)
        && exception.getErrorCategory().equals(ShardManagementErrorCategory.RangeShardMap);

    rsm.deleteMapping(mappingToDelete, mappingLockToken);

    exception =
        AssertExtensions.assertThrows(() -> rsm.getMappingForKey(1));

    assert exception.getErrorCode().equals(ShardManagementErrorCode.MappingNotFoundForKey)
        && exception.getErrorCategory().equals(ShardManagementErrorCategory.RangeShardMap);

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

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]);

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

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]);

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

  /**
   * Update range mapping in range shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void updateRangeMappingDefault() {
    CountingCacheStore countingCache = new CountingCacheStore(new CacheStore());

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), new StoreOperationFactory(), countingCache,
        ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO, Duration.ZERO,
        Duration.ZERO),
        RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]);

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
        ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO, Duration.ZERO,
        Duration.ZERO),
        RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl1 =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]);
    Shard s1 = rsm.createShard(sl1);
    assert s1 != null;

    ShardLocation sl2 =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[1]);
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
        ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO, Duration.ZERO,
        Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    Shard s1 = rsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]));
    assert s1 != null;

    Shard s2 = rsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[1]));
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
      rsm.updateMapping(presult, pu);
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

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    Shard s1 = rsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]));
    assert s1 != null;

    Shard s2 = rsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[1]));
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

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 20), s);

    // Lock the mapping
    MappingLockToken mappingLockToken = MappingLockToken.create();
    rsm.lockMapping(r1, mappingLockToken);

    // Should throw if the correct lock owner id isn't passed
    ShardManagementException exception =
        AssertExtensions.assertThrows(() -> rsm.splitMapping(r1, 5));
    assertTrue(exception.getErrorCode().equals(
        ShardManagementErrorCode.MappingLockOwnerIdDoesNotMatch)
        && exception.getErrorCategory().equals(ShardManagementErrorCategory.RangeShardMap));

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

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]);

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

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]);

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

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    Shard s1 = rsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]));
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
        exception.getErrorCode().equals(ShardManagementErrorCode.MappingLockOwnerIdDoesNotMatch)
            && exception.getErrorCategory().equals(ShardManagementErrorCategory.RangeShardMap));

    // Pass in an incorrect right lockowner id
    exception = AssertExtensions.assertThrows(
        () -> rsm.mergeMappings(r1, r2, MappingLockToken.NoLock, mappingLockTokenRight));
    assertTrue("Expected MappingLockOwnerIdDoesNotMatch error when Updating mapping!",
        exception.getErrorCode().equals(ShardManagementErrorCode.MappingLockOwnerIdDoesNotMatch)
            && exception.getErrorCategory().equals(ShardManagementErrorCategory.RangeShardMap));

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

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    Shard s1 = rsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]));
    assert s1 != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s1);

    Shard s2 = rsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[1]));
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

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    Shard s1 = rsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]));
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

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    Shard s1 = rsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]));
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
        exception.getErrorCode().equals(ShardManagementErrorCode.MappingIsAlreadyLocked)
            && exception.getErrorCategory().equals(ShardManagementErrorCategory.RangeShardMap));

    // Lookup should work without a lockownerId
    RangeMapping r1LookUp = rsm.getMappingForKey(5);
    assertEquals("Expected range mappings to be equal!", r1, r1LookUp);

    // Try to unlock the mapping with the wrong lock owner id
    exception = AssertExtensions.assertThrows(
        () -> rsm.unlockMapping(r1, MappingLockToken.NoLock));

    assertTrue(String.format("Expected MappingLockOwnerIdDoesNotMatch error. Found: ErrorCode: %1$s"
            + " ErrorCategory: %2$s!", exception.getErrorCode(),
        ShardManagementErrorCategory.RangeShardMap),
        exception.getErrorCode().equals(ShardManagementErrorCode.MappingLockOwnerIdDoesNotMatch)
            && exception.getErrorCategory().equals(ShardManagementErrorCategory.RangeShardMap));

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

    ListShardMap<Integer> rsm = smm.getListShardMap(ShardMapperTests.listShardMapName,
        ShardKeyType.Int32);
    assert rsm != null;

    Shard s1 = rsm.createShard(new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]));
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
        exception.getErrorCode().equals(ShardManagementErrorCode.MappingIsAlreadyLocked)
            && exception.getErrorCategory().equals(ShardManagementErrorCategory.ListShardMap));

    // Lookup should work without a lockOwnerId
    PointMapping r1LookUp = rsm.getMappingForKey(1);
    assertEquals("Expected range mappings to be equal!", r1, r1LookUp);

    // Try to unlock the mapping with the wrong lock owner id
    exception = AssertExtensions.assertThrows(
        () -> rsm.unlockMapping(r1, MappingLockToken.NoLock));
    assertTrue(exception.getErrorCode().equals(
        ShardManagementErrorCode.MappingLockOwnerIdDoesNotMatch)
        && exception.getErrorCategory().equals(ShardManagementErrorCategory.ListShardMap));
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

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    Shard s1 = rsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]));
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

    ListShardMap<Integer> rsm = smm.getListShardMap(ShardMapperTests.listShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    Shard s1 = rsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]));
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

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTests.listShardMapName,
        ShardKeyType.Int32);

    assert lsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]);

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

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

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

  /**
   * Take a mapping offline, verify that the existing connection is killed.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void killConnectionOnOfflineRangeMapping() {
    ShardMapManager smm = new ShardMapManager(new SqlShardMapManagerCredentials(
        Globals.SHARD_MAP_MANAGER_CONN_STRING), new SqlStoreConnectionFactory(),
        new StoreOperationFactory(), new CacheStore(), ShardMapManagerLoadPolicy.Lazy,
        new RetryPolicy(1, Duration.ZERO, Duration.ZERO, Duration.ZERO),
        RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 20), s);

    try (Connection conn = rsm.openConnectionForKey(1, Globals.SHARD_MAP_MANAGER_CONN_STRING)) {
      assert !conn.isClosed();

      RangeMappingUpdate ru = new RangeMappingUpdate();
      ru.setStatus(MappingStatus.Offline);

      RangeMapping rNew = rsm.updateMapping(r1, ru);
      assert rNew != null;

      boolean failed = false;
      try (Statement stmt = conn.createStatement()) {
        stmt.executeQuery("select 1");
      } catch (SQLException e) {
        failed = true;
      }

      assert failed;
      assert conn.isClosed();

      failed = false;
      // Open 2nd connection.
      try (Connection conn2 = rsm.openConnectionForKey(1, Globals.SHARD_MAP_MANAGER_CONN_STRING)) {
        conn2.close();
      } catch (Exception ex) {
        RuntimeException tempVar = (RuntimeException) ex.getCause();
        ShardManagementException sme = (ShardManagementException)
            ((tempVar instanceof ShardManagementException) ? tempVar : null);
        if (sme != null) {
          assert sme.getErrorCode().equals(ShardManagementErrorCode.MappingIsOffline);
          failed = true;
        }
      }

      assert failed;

      // Mark the mapping online again so that it will be cleaned up
      ru.setStatus(MappingStatus.Online);
      RangeMapping rUpdated = rsm.updateMapping(rNew, ru);

      assert rUpdated != null;

      failed = false;
      // Open 3rd connection. This should succeed.
      try (Connection conn3 = rsm.openConnectionForKey(1, Globals.SHARD_MAP_MANAGER_CONN_STRING)) {
        conn3.close();
      } catch (ShardManagementException ex) {
        failed = true;
      }

      assert !failed;
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * OpenConnectionForKey for unavailable server using ListShardMap.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void unavailableServerOpenConnectionForKeyListShardMap() {
    unavailableServerOpenConnectionForKeyListShardMapInternal(false);
  }

  /**
   * OpenConnectionForKeyAsync for unavailable server using ListShardMap.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void unavailableServerOpenConnectionForKeyAsyncListShardMap() {
    unavailableServerOpenConnectionForKeyListShardMapInternal(true);
  }

  /**
   * OpenConnectionForKey for unavailable server using RangeShardMap.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void unavailableServerOpenConnectionForKeyRangeShardMap() {
    unavailableServerOpenConnectionForKeyRangeShardMapInternal(false);
  }

  /**
   * OpenConnectionForKeyAsync for unavailable server using RangeShardMap.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void unavailableServerOpenConnectionForKeyAsyncRangeShardMap() {
    unavailableServerOpenConnectionForKeyRangeShardMapInternal(true);
  }

  /**
   * Add point mapping in list shard map, do not update local cache.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void addPointMappingNoCacheUpdate() {
    StubCacheStore scs = new StubCacheStore();
    scs.setCallBase(true);
    scs.addOrUpdateMappingIStoreMappingCacheStoreMappingUpdatePolicy = (ssm, p) -> {
    };

    // Create a cache store that never inserts.
    CountingCacheStore cacheStore = new CountingCacheStore(scs);

    ShardMapManager smm = new ShardMapManager(new SqlShardMapManagerCredentials(
        Globals.SHARD_MAP_MANAGER_CONN_STRING), new SqlStoreConnectionFactory(),
        new StoreOperationFactory(), cacheStore, ShardMapManagerLoadPolicy.Lazy,
        new RetryPolicy(1, Duration.ZERO, Duration.ZERO, Duration.ZERO),
        RetryBehavior.getDefaultRetryBehavior());

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTests.listShardMapName,
        ShardKeyType.Int32);
    assert lsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);

    Shard s = lsm.createShard(sl);
    assert s != null;

    PointMapping p1 = lsm.createPointMapping(2, s);
    assert p1 != null;

    PointMapping p2 = lsm.getMappingForKey(2);
    assert p2 != null;

    assert 0 == cacheStore.getLookupMappingMissCount();
  }

  /**
   * Add range mapping in range shard map, do not update local cache.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void addRangeMappingNoCacheUpdate() {
    StubCacheStore scs = new StubCacheStore();
    scs.setCallBase(true);
    scs.addOrUpdateMappingIStoreMappingCacheStoreMappingUpdatePolicy = (ssm, p) -> {
    };

    // Create a cache store that never inserts.
    CountingCacheStore cacheStore = new CountingCacheStore(scs);

    ShardMapManager smm = new ShardMapManager(new SqlShardMapManagerCredentials(
        Globals.SHARD_MAP_MANAGER_CONN_STRING), new SqlStoreConnectionFactory(),
        new StoreOperationFactory(), cacheStore, ShardMapManagerLoadPolicy.Lazy,
        new RetryPolicy(1, Duration.ZERO, Duration.ZERO, Duration.ZERO),
        RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);
    assert rsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);

    Shard s = rsm.createShard(sl);
    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);
    assert r1 != null;

    RangeMapping r2 = rsm.getMappingForKey(2);

    assert r2 != null;
    Assert.assertEquals(0, cacheStore.getLookupMappingMissCount());
  }

  /**
   * Add a point mapping to list shard map, do not commit GSM transaction.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void addPointMappingAbortGSM() {
    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createAddMappingOperation4Param = setAddMappingOperationGsmDo(true);

    ShardMapManager smm = new ShardMapManager(new SqlShardMapManagerCredentials(
        Globals.SHARD_MAP_MANAGER_CONN_STRING), new SqlStoreConnectionFactory(), sof,
        new CacheStore(), ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO,
        Duration.ZERO, Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTests.listShardMapName,
        ShardKeyType.Int32);

    assert lsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);

    Shard s = lsm.createShard(sl);

    assert s != null;

    boolean storeOperationFailed = false;
    try {
      PointMapping p1 = lsm.createPointMapping(2, s);
      assert p1 != null;
    } catch (ShardManagementException sme) {
      Assert.assertEquals(ShardManagementErrorCategory.ListShardMap, sme.getErrorCategory());
      Assert.assertEquals(ShardManagementErrorCode.StorageOperationFailure, sme.getErrorCode());
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    sof.createAddMappingOperation4Param = setAddMappingOperationGsmDo(false);

    // Validation: Adding same mapping again will succeed.
    PointMapping p2 = lsm.createPointMapping(2, s);
    assert p2 != null;
  }

  /**
   * Delete existing point mapping from list shard map, do not commit GSM transaction.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void deletePointMappingAbortGSM() {
    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createRemoveMappingOperation5Param = (_smm, _opcode, _ssm, _sm, _loid) -> {
      StubRemoveMappingOperation op = new StubRemoveMappingOperation(_smm, _opcode, _ssm, _sm,
          _loid);
      op.setCallBase(true);
      op.doGlobalPostLocalExecuteIStoreTransactionScope = (ts) -> {
        throw new StoreException("RemoveMappingOperation",
            ShardMapFaultHandlingTests.sqlException);
      };
      return op;
    };

    ShardMapManager smm = new ShardMapManager(new SqlShardMapManagerCredentials(
        Globals.SHARD_MAP_MANAGER_CONN_STRING), new SqlStoreConnectionFactory(), sof,
        new CacheStore(), ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO,
        Duration.ZERO, Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTests.listShardMapName,
        ShardKeyType.Int32);

    assert lsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);

    Shard s = lsm.createShard(sl);
    assert s != null;

    PointMapping p1 = lsm.createPointMapping(1, s);
    assert p1 != null;

    PointMapping pmOffline = lsm.markMappingOffline(p1);
    assert pmOffline != null;

    boolean storeOperationFailed = false;
    try {
      lsm.deleteMapping(pmOffline);
    } catch (ShardManagementException sme) {
      Assert.assertEquals(ShardManagementErrorCategory.ListShardMap, sme.getErrorCategory());
      Assert.assertEquals(ShardManagementErrorCode.StorageOperationFailure, sme.getErrorCode());
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // validation: Lookup point will succeed.
    PointMapping pNew = lsm.getMappingForKey(1);
    assert pNew != null;
  }

  /**
   * Update existing point mapping in list shard map, do not commit GSM transaction.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void updatePointMappingAbortGSM() {
    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createUpdateMappingOperation7Param = (_smm, _opcode, _ssm, _sms, _smt, _p, _loid) -> {
      StubUpdateMappingOperation op = new StubUpdateMappingOperation(_smm, _opcode, _ssm, _sms,
          _smt, _p, _loid);
      op.setCallBase(true);
      op.doGlobalPostLocalExecuteIStoreTransactionScope = (ts) -> {
        throw new StoreException("UpdateMappingOperation",
            ShardMapFaultHandlingTests.sqlException);
      };
      return op;
    };

    ShardMapManager smm = new ShardMapManager(new SqlShardMapManagerCredentials(
        Globals.SHARD_MAP_MANAGER_CONN_STRING), new SqlStoreConnectionFactory(), sof,
        new CacheStore(), ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO,
        Duration.ZERO, Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTests.listShardMapName,
        ShardKeyType.Int32);

    assert lsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);

    Shard s = lsm.createShard(sl);
    assert s != null;

    PointMapping p1 = lsm.createPointMapping(1, s);
    assert p1 != null;

    // Take the mapping offline first before the shard location can be updated.
    PointMappingUpdate pu = new PointMappingUpdate();
    pu.setStatus(MappingStatus.Offline);

    boolean storeOperationFailed = false;
    try {
      PointMapping pNew = lsm.updateMapping(p1, pu);
      assert pNew != null;
    } catch (ShardManagementException sme) {
      Assert.assertEquals(ShardManagementErrorCategory.ListShardMap, sme.getErrorCategory());
      Assert.assertEquals(ShardManagementErrorCode.StorageOperationFailure, sme.getErrorCode());
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // validation: validate custom field of the mapping.
    PointMapping pValidate = lsm.getMappingForKey(1);
    Assert.assertEquals(p1.getStatus(), pValidate.getStatus());
  }

  /**
   * Update location of existing point mapping in list shard map, do not commit GSM transaction.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void updatePointMappingLocationAbortGSM() {
    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createUpdateMappingOperation7Param = setUpdateMappingOperationGsmDo(false);

    ShardMapManager smm = new ShardMapManager(new SqlShardMapManagerCredentials(
        Globals.SHARD_MAP_MANAGER_CONN_STRING), new SqlStoreConnectionFactory(), sof,
        new CacheStore(), ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO,
        Duration.ZERO, Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTests.listShardMapName,
        ShardKeyType.Int32);

    assert lsm != null;

    Shard s1 = lsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]));
    assert s1 != null;

    Shard s2 = lsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[1]));
    assert s2 != null;

    PointMapping p1 = lsm.createPointMapping(1, s1);

    PointMappingUpdate pu1 = new PointMappingUpdate();
    // Take the mapping offline first before the shard location can be updated.
    pu1.setStatus(MappingStatus.Offline);
    PointMapping pNew = lsm.updateMapping(p1, pu1);

    PointMappingUpdate pu2 = new PointMappingUpdate();
    pu2.setShard(s2);

    sof.createUpdateMappingOperation7Param = setUpdateMappingOperationGsmDo(true);

    boolean storeOperationFailed = false;
    try {
      pNew = lsm.updateMapping(pNew, pu2);
      assert pNew != null;
    } catch (ShardManagementException sme) {
      Assert.assertEquals(ShardManagementErrorCategory.ListShardMap, sme.getErrorCategory());
      Assert.assertEquals(ShardManagementErrorCode.StorageOperationFailure, sme.getErrorCode());
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // validation: validate location of the mapping.
    PointMapping pValidate = lsm.getMappingForKey(1);
    Assert.assertEquals(p1.getShard().getId(), pValidate.getShard().getId());
  }

  /**
   * Add a range mapping to range shard map, do not commit GSM transaction.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void addRangeMappingAbortGSM() {
    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createAddMappingOperation4Param = setAddMappingOperationGsmDo(true);

    ShardMapManager smm = new ShardMapManager(new SqlShardMapManagerCredentials(
        Globals.SHARD_MAP_MANAGER_CONN_STRING), new SqlStoreConnectionFactory(), sof,
        new CacheStore(), ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO,
        Duration.ZERO, Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);

    Shard s = rsm.createShard(sl);
    assert s != null;

    boolean storeOperationFailed = false;
    try {
      RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);
      assert r1 != null;
    } catch (ShardManagementException sme) {
      Assert.assertEquals(ShardManagementErrorCategory.RangeShardMap, sme.getErrorCategory());
      Assert.assertEquals(ShardManagementErrorCode.StorageOperationFailure, sme.getErrorCode());
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    sof.createAddMappingOperation4Param = setAddMappingOperationGsmDo(false);

    // validation: adding same range mapping again will succeed.
    RangeMapping rValidate = rsm.createRangeMapping(new Range(1, 10), s);
    assert rValidate != null;
  }

  /**
   * Delete existing range mapping from range shard map, abort transaction in GSM.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void deleteRangeMappingAbortGSM() {
    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createRemoveMappingOperation5Param = (_smm, _opcode, _ssm, _sm, _loid) -> {
      StubRemoveMappingOperation op = new StubRemoveMappingOperation(_smm, _opcode, _ssm, _sm,
          _loid);
      op.setCallBase(true);
      op.doGlobalPostLocalExecuteIStoreTransactionScope = (ts) -> {
        throw new StoreException("RemoveMappingOperation",
            ShardMapFaultHandlingTests.sqlException);
      };
      return op;
    };

    ShardMapManager smm = new ShardMapManager(new SqlShardMapManagerCredentials(
        Globals.SHARD_MAP_MANAGER_CONN_STRING), new SqlStoreConnectionFactory(), sof,
        new CacheStore(), ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO,
        Duration.ZERO, Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);

    assert r1 != null;

    RangeMappingUpdate ru = new RangeMappingUpdate();
    ru.setStatus(MappingStatus.Offline);

    // The mapping must be made offline before it can be deleted.
    r1 = rsm.updateMapping(r1, ru);
    Assert.assertEquals(MappingStatus.Offline, r1.getStatus());

    boolean storeOperationFailed = false;
    try {
      rsm.deleteMapping(r1);
    } catch (ShardManagementException sme) {
      Assert.assertEquals(ShardManagementErrorCategory.RangeShardMap, sme.getErrorCategory());
      Assert.assertEquals(ShardManagementErrorCode.StorageOperationFailure, sme.getErrorCode());
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // Validation: lookup for 5 returns a valid mapping.
    RangeMapping rValidate = rsm.getMappingForKey(5);
    assert rValidate != null;
    Assert.assertEquals(rValidate.getRange(), r1.getRange());
  }

  /**
   * Update range mapping in range shard map, do not commit transaction in GSM.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void updateRangeMappingAbortGSM() {
    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createUpdateMappingOperation7Param = (_smm, _opcode, _ssm, _sms, _smt, _p, _loid) -> {
      StubUpdateMappingOperation op = new StubUpdateMappingOperation(_smm, _opcode, _ssm, _sms,
          _smt, _p, _loid);
      op.setCallBase(true);
      op.doGlobalPostLocalExecuteIStoreTransactionScope = (ts) -> {
        throw new StoreException("UpdateMappingOperation",
            ShardMapFaultHandlingTests.sqlException);
      };
      return op;
    };

    ShardMapManager smm = new ShardMapManager(new SqlShardMapManagerCredentials(
        Globals.SHARD_MAP_MANAGER_CONN_STRING), new SqlStoreConnectionFactory(), sof,
        new CacheStore(), ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO,
        Duration.ZERO, Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);

    Shard s = rsm.createShard(sl);
    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 20), s);

    RangeMappingUpdate ru = new RangeMappingUpdate();
    ru.setStatus(MappingStatus.Offline);

    boolean storeOperationFailed = false;
    try {
      RangeMapping rNew = rsm.updateMapping(r1, ru);
      assert rNew != null;
    } catch (ShardManagementException sme) {
      Assert.assertEquals(ShardManagementErrorCategory.RangeShardMap, sme.getErrorCategory());
      Assert.assertEquals(ShardManagementErrorCode.StorageOperationFailure, sme.getErrorCode());
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // Validation: check that custom is unchanged.
    RangeMapping rValidate = rsm.getMappingForKey(1);
    Assert.assertEquals(r1.getStatus(), rValidate.getStatus());
  }

  /**
   * Update range mapping in range shard map to change location, do not commit transaction in GSM.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void updateRangeMappingLocationAbortGSM() {
    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createUpdateMappingOperation7Param = setUpdateMappingOperationGsmDo(false);

    ShardMapManager smm = new ShardMapManager(new SqlShardMapManagerCredentials(
        Globals.SHARD_MAP_MANAGER_CONN_STRING), new SqlStoreConnectionFactory(), sof,
        new CacheStore(), ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO,
        Duration.ZERO, Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl1 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);
    Shard s1 = rsm.createShard(sl1);
    assert s1 != null;

    ShardLocation sl2 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[1]);
    Shard s2 = rsm.createShard(sl2);
    assert s2 != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 20), s1);

    RangeMappingUpdate ru1 = new RangeMappingUpdate();
    // Take the mapping offline first.
    ru1.setStatus(MappingStatus.Offline);
    RangeMapping rNew = rsm.updateMapping(r1, ru1);
    assert rNew != null;

    RangeMappingUpdate ru2 = new RangeMappingUpdate();
    ru2.setShard(s2);

    sof.createUpdateMappingOperation7Param = setUpdateMappingOperationGsmDo(true);

    boolean storeOperationFailed = false;
    try {
      rNew = rsm.updateMapping(rNew, ru2);
      assert rNew != null;
    } catch (ShardManagementException sme) {
      Assert.assertEquals(ShardManagementErrorCategory.RangeShardMap, sme.getErrorCategory());
      Assert.assertEquals(ShardManagementErrorCode.StorageOperationFailure, sme.getErrorCode());
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // validation: validate location of the mapping.
    RangeMapping rValidate = rsm.getMappingForKey(1);
    assert s1.getId().equals(rValidate.getShard().getId());
  }

  /**
   * Split existing range mapping in range shard map, abort transaction in GSM.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void splitRangeAbortGSM() {
    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createReplaceMappingsOperation5Param = (_smm, _opcode, _ssm, _smo, _smn) -> {
      StubReplaceMappingsOperation op = new StubReplaceMappingsOperation(_smm, _opcode, _ssm, _smo,
          _smn);
      op.setCallBase(true);
      op.doGlobalPostLocalExecuteIStoreTransactionScope = (ts) -> {
        throw new StoreException("ReplaceMappingsOperation",
            ShardMapFaultHandlingTests.sqlException);
      };
      return op;
    };

    ShardMapManager smm = new ShardMapManager(new SqlShardMapManagerCredentials(
        Globals.SHARD_MAP_MANAGER_CONN_STRING), new SqlStoreConnectionFactory(), sof,
        new CacheStore(), ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO,
        Duration.ZERO, Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 20), s);

    boolean storeOperationFailed = false;
    try {
      List<RangeMapping> rList = rsm.splitMapping(r1, 5);
      assert 2 == rList.size();
    } catch (ShardManagementException sme) {
      Assert.assertEquals(ShardManagementErrorCategory.RangeShardMap, sme.getErrorCategory());
      Assert.assertEquals(ShardManagementErrorCode.StorageOperationFailure, sme.getErrorCode());
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // Validation: get all mappings for [1,20) should return 1 mapping.
    assert 1 == rsm.getMappings().size();
  }

  /**
   * Merge adjacent range mappings in range shard map, do not commit transaction in GSM.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void mergeRangeMappingsAbortGSM() {
    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createReplaceMappingsOperation5Param = (_smm, _opcode, _ssm, _smo, _smn) -> {
      StubReplaceMappingsOperation op = new StubReplaceMappingsOperation(_smm, _opcode, _ssm, _smo,
          _smn);
      op.setCallBase(true);
      op.doGlobalPostLocalExecuteIStoreTransactionScope = (ts) -> {
        throw new StoreException("ReplaceMappingsOperation",
            ShardMapFaultHandlingTests.sqlException);
      };
      return op;
    };

    ShardMapManager smm = new ShardMapManager(new SqlShardMapManagerCredentials(
        Globals.SHARD_MAP_MANAGER_CONN_STRING), new SqlStoreConnectionFactory(), sof,
        new CacheStore(), ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO,
        Duration.ZERO, Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    Shard s1 = rsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]));

    assert s1 != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s1);

    RangeMapping r2 = rsm.createRangeMapping(new Range(10, 20), s1);

    boolean storeOperationFailed = false;
    try {
      RangeMapping rMerged = rsm.mergeMappings(r1, r2);
      assert rMerged != null;
    } catch (ShardManagementException sme) {
      Assert.assertEquals(ShardManagementErrorCategory.RangeShardMap, sme.getErrorCategory());
      Assert.assertEquals(ShardManagementErrorCode.StorageOperationFailure, sme.getErrorCode());
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // Validation: get all mappings for [1,20) should return 2 mappings.
    assert 2 == rsm.getMappings().size();
  }

  /**
   * Add a point mapping to list shard map, do not commit LSM transaction.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void addPointMappingAbortLSM() {
    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createAddMappingOperation4Param = setAddMappingOperationLsmDo(true);

    ShardMapManager smm = new ShardMapManager(new SqlShardMapManagerCredentials(
        Globals.SHARD_MAP_MANAGER_CONN_STRING), new SqlStoreConnectionFactory(), sof,
        new CacheStore(), ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO,
        Duration.ZERO, Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTests.listShardMapName,
        ShardKeyType.Int32);

    assert lsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);

    Shard s = lsm.createShard(sl);

    assert s != null;

    boolean storeOperationFailed = false;
    try {
      PointMapping p1 = lsm.createPointMapping(2, s);
      assert p1 != null;
    } catch (ShardManagementException sme) {
      Assert.assertEquals(ShardManagementErrorCategory.ListShardMap, sme.getErrorCategory());
      Assert.assertEquals(ShardManagementErrorCode.StorageOperationFailure, sme.getErrorCode());
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    sof.createAddMappingOperation4Param = setAddMappingOperationLsmDo(false);

    // Validation: Adding same mapping again will succeed.
    PointMapping p2 = lsm.createPointMapping(2, s);
    assert p2 != null;
  }

  /**
   * Delete existing point mapping from list shard map, do not commit LSM transaction.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void deletePointMappingAbortLSM() {
    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createRemoveMappingOperation5Param = (_smm, _opcode, _ssm, _sm, _loid) -> {
      StubRemoveMappingOperation op = new StubRemoveMappingOperation(_smm, _opcode, _ssm, _sm,
          _loid);
      op.setCallBase(true);
      op.doLocalSourceExecuteIStoreTransactionScope = (ts) -> {
        throw new StoreException("RemoveMappingOperation",
            ShardMapFaultHandlingTests.sqlException);
      };
      return op;
    };

    ShardMapManager smm = new ShardMapManager(new SqlShardMapManagerCredentials(
        Globals.SHARD_MAP_MANAGER_CONN_STRING), new SqlStoreConnectionFactory(), sof,
        new CacheStore(), ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO,
        Duration.ZERO, Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTests.listShardMapName,
        ShardKeyType.Int32);

    assert lsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);

    Shard s = lsm.createShard(sl);

    assert s != null;

    PointMapping p1 = lsm.createPointMapping(1, s);

    PointMappingUpdate ru = new PointMappingUpdate();
    ru.setStatus(MappingStatus.Offline);

    // The mapping must be made offline before it can be deleted.
    p1 = lsm.updateMapping(p1, ru);
    Assert.assertEquals(MappingStatus.Offline, p1.getStatus());

    boolean storeOperationFailed = false;
    try {
      lsm.deleteMapping(p1);
    } catch (ShardManagementException sme) {
      Assert.assertEquals(ShardManagementErrorCategory.ListShardMap, sme.getErrorCategory());
      Assert.assertEquals(ShardManagementErrorCode.StorageOperationFailure, sme.getErrorCode());
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // validation: Lookup point will succeed.
    PointMapping pNew = lsm.getMappingForKey(1);
    assert pNew != null;
  }

  /**
   * Delete existing range mapping from range shard map, abort transaction in LSM
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void deleteRangeMappingAbortLSM() {
    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createRemoveMappingOperation5Param = (_smm, _opcode, _ssm, _sm, _loid) -> {
      StubRemoveMappingOperation op = new StubRemoveMappingOperation(_smm, _opcode, _ssm, _sm,
          _loid);
      op.setCallBase(true);
      op.doLocalSourceExecuteIStoreTransactionScope = (ts) -> {
        throw new StoreException("RemoveMappingOperation",
            ShardMapFaultHandlingTests.sqlException);
      };
      return op;
    };

    ShardMapManager smm = new ShardMapManager(new SqlShardMapManagerCredentials(
        Globals.SHARD_MAP_MANAGER_CONN_STRING), new SqlStoreConnectionFactory(), sof,
        new CacheStore(), ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO,
        Duration.ZERO, Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);

    assert r1 != null;

    RangeMappingUpdate ru = new RangeMappingUpdate();
    ru.setStatus(MappingStatus.Offline);

    // The mapping must be made offline before it can be deleted.
    r1 = rsm.updateMapping(r1, ru);
    assert MappingStatus.Offline.equals(r1.getStatus());

    boolean storeOperationFailed = false;
    try {
      rsm.deleteMapping(r1);
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.RangeShardMap.equals(sme.getErrorCategory());
      assert ShardManagementErrorCode.StorageOperationFailure.equals(sme.getErrorCode());
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // Validation: lookup for 5 returns a valid mapping.
    RangeMapping rValidate = rsm.getMappingForKey(5);
    assert rValidate != null;
  }

  /**
   * Update existing point mapping in list shard map, do not commit LSM transaction.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void updatePointMappingAbortLSM() {
    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createUpdateMappingOperation7Param = (_smm, _opcode, _ssm, _sms, _smt, _p, _loid) -> {
      StubUpdateMappingOperation op = new StubUpdateMappingOperation(_smm, _opcode, _ssm, _sms,
          _smt, _p, _loid);
      op.setCallBase(true);
      op.doLocalSourceExecuteIStoreTransactionScope = (ts) -> {
        throw new StoreException("UpdateMappingOperation",
            ShardMapFaultHandlingTests.sqlException);
      };
      return op;
    };

    ShardMapManager smm = new ShardMapManager(new SqlShardMapManagerCredentials(
        Globals.SHARD_MAP_MANAGER_CONN_STRING), new SqlStoreConnectionFactory(), sof,
        new CacheStore(), ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO,
        Duration.ZERO, Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTests.listShardMapName,
        ShardKeyType.Int32);

    assert lsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);

    Shard s = lsm.createShard(sl);

    assert s != null;

    PointMapping p1 = lsm.createPointMapping(1, s);

    // Take the mapping offline first before the shard location can be updated.
    PointMappingUpdate pu = new PointMappingUpdate();
    pu.setStatus(MappingStatus.Offline);

    boolean storeOperationFailed = false;
    try {
      PointMapping pNew = lsm.updateMapping(p1, pu);
      assert pNew != null;
    } catch (ShardManagementException sme) {
      Assert.assertEquals(ShardManagementErrorCategory.ListShardMap, sme.getErrorCategory());
      Assert.assertEquals(ShardManagementErrorCode.StorageOperationFailure, sme.getErrorCode());
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // validation: validate custom field of the mapping.
    PointMapping pValidate = lsm.getMappingForKey(1);
    Assert.assertEquals(p1.getStatus(), pValidate.getStatus());
  }

  /**
   * Update location of existing point mapping in list shard map, do not commit LSM transaction.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void updatePointMappingLocationAbortLSM() {
    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createUpdateMappingOperation7Param = setUpdateMappingOperationLsmDo(false);

    ShardMapManager smm = new ShardMapManager(new SqlShardMapManagerCredentials(
        Globals.SHARD_MAP_MANAGER_CONN_STRING), new SqlStoreConnectionFactory(), sof,
        new CacheStore(), ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO,
        Duration.ZERO, Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTests.listShardMapName,
        ShardKeyType.Int32);

    assert lsm != null;

    Shard s1 = lsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]));
    assert s1 != null;

    Shard s2 = lsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[1]));
    assert s2 != null;

    PointMapping p1 = lsm.createPointMapping(1, s1);

    PointMappingUpdate pu1 = new PointMappingUpdate();
    // Take the mapping offline first before the shard location can be updated.
    pu1.setStatus(MappingStatus.Offline);
    PointMapping pNew = lsm.updateMapping(p1, pu1);

    PointMappingUpdate pu2 = new PointMappingUpdate();
    pu2.setShard(s2);

    sof.createUpdateMappingOperation7Param = setUpdateMappingOperationLsmDo(true);

    boolean storeOperationFailed = false;
    try {
      pNew = lsm.updateMapping(pNew, pu2);
      assert pNew != null;
    } catch (ShardManagementException sme) {
      Assert.assertEquals(ShardManagementErrorCategory.ListShardMap, sme.getErrorCategory());
      Assert.assertEquals(ShardManagementErrorCode.StorageOperationFailure, sme.getErrorCode());
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // validation: validate location of the mapping.
    PointMapping pValidate = lsm.getMappingForKey(1);
    Assert.assertEquals(p1.getShard().getId(), pValidate.getShard().getId());
  }

  /**
   * Update range mapping in range shard map to change location, do not commit transaction in LSM.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void updateRangeMappingLocationAbortLSM() {
    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createUpdateMappingOperation7Param = setUpdateMappingOperationLsmDo(false);

    ShardMapManager smm = new ShardMapManager(new SqlShardMapManagerCredentials(
        Globals.SHARD_MAP_MANAGER_CONN_STRING), new SqlStoreConnectionFactory(), sof,
        new CacheStore(), ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO,
        Duration.ZERO, Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl1 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);
    Shard s1 = rsm.createShard(sl1);
    assert s1 != null;

    ShardLocation sl2 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[1]);
    Shard s2 = rsm.createShard(sl2);
    assert s2 != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 20), s1);

    RangeMappingUpdate ru1 = new RangeMappingUpdate();
    // Take the mapping offline first.
    ru1.setStatus(MappingStatus.Offline);

    RangeMapping rNew = rsm.updateMapping(r1, ru1);
    assert rNew != null;

    RangeMappingUpdate ru2 = new RangeMappingUpdate();
    ru2.setShard(s2);

    sof.createUpdateMappingOperation7Param = setUpdateMappingOperationLsmDo(true);
    boolean storeOperationFailed = false;
    try {
      rNew = rsm.updateMapping(rNew, ru2);
      assert rNew != null;
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.RangeShardMap.equals(sme.getErrorCategory());
      assert ShardManagementErrorCode.StorageOperationFailure.equals(sme.getErrorCode());
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // validation: validate location of the mapping.
    RangeMapping rValidate = rsm.getMappingForKey(1);
    assert s1.getId().equals(rValidate.getShard().getId());
  }

  /**
   * Add a range mapping to range shard map, do not commit LSM transaction.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void addRangeMappingAbortLSM() {
    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createAddMappingOperation4Param = setAddMappingOperationLsmDo(true);

    ShardMapManager smm = new ShardMapManager(new SqlShardMapManagerCredentials(
        Globals.SHARD_MAP_MANAGER_CONN_STRING), new SqlStoreConnectionFactory(), sof,
        new CacheStore(), ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO,
        Duration.ZERO, Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    boolean storeOperationFailed = false;
    try {
      RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);
      assert r1 != null;
    } catch (ShardManagementException sme) {
      Assert.assertEquals(ShardManagementErrorCategory.RangeShardMap, sme.getErrorCategory());
      Assert.assertEquals(ShardManagementErrorCode.StorageOperationFailure, sme.getErrorCode());
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    sof.createAddMappingOperation4Param = setAddMappingOperationLsmDo(false);

    // validation: adding same range mapping again will succeed.
    RangeMapping rValidate = rsm.createRangeMapping(new Range(1, 10), s);
    assert rValidate != null;
  }

  /**
   * Update range mapping in range shard map, do not commit transaction in LSM
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void updateRangeMappingAbortLSM() {
    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createUpdateMappingOperation7Param = (_smm, _opcode, _ssm, _sms, _smt, _p, _loid) -> {
      StubUpdateMappingOperation op = new StubUpdateMappingOperation(_smm, _opcode, _ssm, _sms,
          _smt, _p, _loid);
      op.setCallBase(true);
      op.doLocalSourceExecuteIStoreTransactionScope = (ts) -> {
        throw new StoreException("UpdateMappingOperation",
            ShardMapFaultHandlingTests.sqlException);
      };
      return op;
    };

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), sof, new CacheStore(), ShardMapManagerLoadPolicy.Lazy,
        new RetryPolicy(1, Duration.ZERO, Duration.ZERO, Duration.ZERO),
        RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 20), s);

    RangeMappingUpdate ru = new RangeMappingUpdate();
    ru.setStatus(MappingStatus.Offline);

    boolean storeOperationFailed = false;
    try {
      RangeMapping rNew = rsm.updateMapping(r1, ru);
      assert rNew != null;
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.RangeShardMap.equals(sme.getErrorCategory());
      assert ShardManagementErrorCode.StorageOperationFailure.equals(sme.getErrorCode());
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // Validation: check that custom is unchanged.
    RangeMapping rValidate = rsm.getMappingForKey(1);
    assert r1.getStatus().equals(rValidate.getStatus());
  }

  /**
   * Split existing range mapping in range shard map, abort transaction in LSM.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void splitRangeAbortLSM() {
    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createReplaceMappingsOperation5Param = (_smm, _opcode, _ssm, _smo, _smn) -> {
      StubReplaceMappingsOperation op = new StubReplaceMappingsOperation(_smm, _opcode, _ssm, _smo,
          _smn);
      op.setCallBase(true);
      op.doLocalSourceExecuteIStoreTransactionScope = (ts) -> {
        throw new StoreException("ReplaceMappingsOperation",
            ShardMapFaultHandlingTests.sqlException);
      };
      return op;
    };

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), sof, new CacheStore(), ShardMapManagerLoadPolicy.Lazy,
        new RetryPolicy(1, Duration.ZERO, Duration.ZERO, Duration.ZERO),
        RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 20), s);

    boolean storeOperationFailed = false;
    try {
      List<RangeMapping> rList = rsm.splitMapping(r1, 5);
      assert 2 == rList.size();
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.RangeShardMap.equals(sme.getErrorCategory());
      assert ShardManagementErrorCode.StorageOperationFailure.equals(sme.getErrorCode());
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // Validation: get all mappings for [1,20) should return 1 mapping.
    assert 1 == rsm.getMappings().size();
  }

  /**
   * Merge adjacent range mappings in range shard map, do not commit transaction in LSM.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void mergeRangeMappingsAbortLSM() {
    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createReplaceMappingsOperation5Param = (_smm, _opcode, _ssm, _smo, _smn) -> {
      StubReplaceMappingsOperation op = new StubReplaceMappingsOperation(_smm, _opcode, _ssm, _smo,
          _smn);
      op.setCallBase(true);
      op.doLocalSourceExecuteIStoreTransactionScope = (ts) -> {
        throw new StoreException("ReplaceMappingsOperation",
            ShardMapFaultHandlingTests.sqlException);
      };
      return op;
    };

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), sof, new CacheStore(), ShardMapManagerLoadPolicy.Lazy,
        new RetryPolicy(1, Duration.ZERO, Duration.ZERO, Duration.ZERO),
        RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    Shard s1 = rsm.createShard(
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]));
    assert s1 != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s1);

    RangeMapping r2 = rsm.createRangeMapping(new Range(10, 20), s1);

    boolean storeOperationFailed = false;
    try {
      RangeMapping rMerged = rsm.mergeMappings(r1, r2);
      assert rMerged != null;
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.RangeShardMap.equals(sme.getErrorCategory());
      assert ShardManagementErrorCode.StorageOperationFailure.equals(sme.getErrorCode());
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // Validation: get all mappings for [1,20) should return 2 mappings.
    assert 2 == rsm.getMappings().size();
  }

  /**
   * Add a point mapping to list shard map, do not commit GSM Do or Undo transaction.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  @Ignore
  public final void addPointMappingAbortGSMDoAndGSMUndo() {
    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createAddMappingOperation4Param = setAddMappingOperationAbortGsmDoUndo(true);

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), sof, new CacheStore(), ShardMapManagerLoadPolicy.Lazy,
        new RetryPolicy(1, Duration.ZERO, Duration.ZERO, Duration.ZERO),
        RetryBehavior.getDefaultRetryBehavior());

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTests.listShardMapName,
        ShardKeyType.Int32);

    assert lsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);
    ShardLocation sl2 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[1]);

    Shard s = lsm.createShard(sl);
    assert s != null;
    Shard s2 = lsm.createShard(sl2);
    assert s2 != null;

    boolean storeOperationFailed = false;
    try {
      PointMapping p1 = lsm.createPointMapping(2, s);
      assert p1 != null;
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ListShardMap.equals(sme.getErrorCategory());
      assert ShardManagementErrorCode.StorageOperationFailure.equals(sme.getErrorCode());
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // Obtain the pending operations.
    List<StoreLogEntry> pendingOperations = ShardMapperTests.getPendingStoreOperations();
    assert pendingOperations.size() == 1;

    sof.createAddMappingOperation4Param = setAddMappingOperationAbortGsmDoUndo(false);

    // Validation: Adding same mapping again even at different location should succeed.
    PointMapping p2 = lsm.createPointMapping(2, s2);
    assert p2 != null;

    pendingOperations = ShardMapperTests.getPendingStoreOperations();
    assert pendingOperations.size() == 0;
  }

  /**
   * Delete existing range mapping from range shard map, abort transaction in LSM for Do and GSM for
   * Undo.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void deleteRangeMappingAbortLSMDoAndGSMUndo() {
    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createRemoveMappingOperation5Param = setRemoveMappingOperation(true);

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), sof, new CacheStore(), ShardMapManagerLoadPolicy.Lazy,
        new RetryPolicy(1, Duration.ZERO, Duration.ZERO, Duration.ZERO),
        RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);

    assert r1 != null;

    RangeMappingUpdate ru = new RangeMappingUpdate();
    ru.setStatus(MappingStatus.Offline);

    // The mapping must be made offline before it can be deleted.
    r1 = rsm.updateMapping(r1, ru);
    assert MappingStatus.Offline.equals(r1.getStatus());

    boolean storeOperationFailed = false;
    try {
      rsm.deleteMapping(r1);
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.RangeShardMap.equals(sme.getErrorCategory());
      assert ShardManagementErrorCode.StorageOperationFailure.equals(sme.getErrorCode());
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // Obtain the pending operations.
    List<StoreLogEntry> pendingOperations = ShardMapperTests.getPendingStoreOperations();
    assert pendingOperations.size() == 1;

    // Validation: lookup for 5 still returns a valid mapping since we never committed the remove.
    RangeMapping rValidate = rsm.getMappingForKey(5);
    assert rValidate != null;
    assert rValidate.getRange().equals(r1.getRange());

    ///#region OpenConnection with Validation

    // Validation should fail with mapping is offline error since local mapping was not deleted.
    boolean validationFailed = false;
    try (Connection conn = rsm
        .openConnection(rValidate, Globals.SHARD_USER_CONN_STRING, ConnectionOptions.Validate)) {
      conn.close();
    } catch (ShardManagementException smme) {
      validationFailed = true;
      assert smme.getErrorCode().equals(ShardManagementErrorCode.MappingIsOffline);
    } catch (Exception e) {
      e.printStackTrace();
    }

    assert true == validationFailed;

    ///#endregion OpenConnection with Validation

    sof.createRemoveMappingOperation5Param = setRemoveMappingOperation(false);

    // Now we try an AddOperation, which should fail since we still have the mapping.
    ShardManagementException exception = AssertExtensions.assertThrows(
        () -> rsm.createRangeMapping(new Range(1, 10), s));

    Assert.assertTrue("Expected MappingRangeAlreadyMapped error!", exception.getErrorCode().equals(
        ShardManagementErrorCode.MappingRangeAlreadyMapped)
        && exception.getErrorCategory().equals(ShardManagementErrorCategory.RangeShardMap));

    // No pending operation should be left now since the previous operation took care of it.
    pendingOperations = ShardMapperTests.getPendingStoreOperations();
    assert pendingOperations.size() == 0;

    // Removal should succeed now.
    storeOperationFailed = false;
    try {
      rsm.deleteMapping(r1);
    } catch (ShardManagementException e) {
      storeOperationFailed = true;
    }

    assert !storeOperationFailed;
  }

  /**
   * Update range mapping in range shard map, do not commit transaction in GSM Do and LSM Source
   * Undo.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void updateRangeMappingOfflineAbortGSMDoAndGSMUndoPostLocal() {
    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createUpdateMappingOperation7Param = setUpdateMappingOperationGsmDoUndo(true);

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), sof, new CacheStore(), ShardMapManagerLoadPolicy.Lazy,
        new RetryPolicy(1, Duration.ZERO, Duration.ZERO, Duration.ZERO),
        RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 20), s);

    RangeMappingUpdate ru = new RangeMappingUpdate();
    ru.setStatus(MappingStatus.Offline);

    RangeMapping rNew;

    boolean storeOperationFailed = false;
    try {
      rsm.updateMapping(r1, ru);
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.RangeShardMap.equals(sme.getErrorCategory());
      assert ShardManagementErrorCode.StorageOperationFailure.equals(sme.getErrorCode());
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // Validation: check that custom is unchanged.
    RangeMapping rValidate = rsm.getMappingForKey(1);
    assert r1.getStatus().equals(rValidate.getStatus());

    sof.createUpdateMappingOperation7Param = setUpdateMappingOperationGsmDoUndo(false);

    rNew = rsm.updateMapping(r1, ru);
    assert rNew != null;
    assert rNew.getStatus().equals(ru.getStatus());
  }

  /**
   * Update range mapping in range shard map to change location, abort GSM post Local in Do and LSM
   * target in Undo.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void updateRangeMappingLocationAbortGSMPostLocalDoAndLSMTargetUndo() {
    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createUpdateMappingOperation7Param = setUpdateMappingOperationGsmDoUndo(false);

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), sof, new CacheStore(), ShardMapManagerLoadPolicy.Lazy,
        new RetryPolicy(1, Duration.ZERO, Duration.ZERO, Duration.ZERO),
        RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl1 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);
    Shard s1 = rsm.createShard(sl1);
    assert s1 != null;

    ShardLocation sl2 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[1]);
    Shard s2 = rsm.createShard(sl2);
    assert s2 != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 20), s1);

    RangeMappingUpdate ru1 = new RangeMappingUpdate();
    // Take the mapping offline first.
    ru1.setStatus(MappingStatus.Offline);
    RangeMapping rNew = rsm.updateMapping(r1, ru1);
    assert rNew != null;

    RangeMappingUpdate ru2 = new RangeMappingUpdate();
    ru2.setShard(s2);

    sof.createUpdateMappingOperation7Param = setUpdateMappingOperationGsmDoUndo(true);

    boolean storeOperationFailed = false;
    try {
      rNew = rsm.updateMapping(rNew, ru2);
      assert rNew != null;
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.RangeShardMap.equals(sme.getErrorCategory());
      assert ShardManagementErrorCode.StorageOperationFailure.equals(sme.getErrorCode());
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // Obtain the pending operations.
    List<StoreLogEntry> pendingOperations = ShardMapperTests.getPendingStoreOperations();
    assert pendingOperations.size() == 1;

    // validation: validate location of the mapping.
    RangeMapping rValidate = rsm.getMappingForKey(1);
    assert s1.getId().equals(rValidate.getShard().getId());

    ///#region OpenConnection with Validation

    // Validation should fail with mapping does not exist since source mapping was deleted.
    boolean validationFailed = false;
    try (Connection conn = rsm.openConnection(rValidate, Globals.SHARD_USER_CONN_STRING,
        ConnectionOptions.Validate)) {
      conn.close();
    } catch (ShardManagementException smme) {
      assert smme.getErrorCode().equals(ShardManagementErrorCode.MappingDoesNotExist);
      validationFailed = true;
    } catch (Exception e) {
      e.printStackTrace();
    }

    assert validationFailed;

    ///#endregion OpenConnection with Validation

    sof.createUpdateMappingOperation7Param = setUpdateMappingOperationGsmDoUndo(false);

    // Removal should succeed now.
    storeOperationFailed = false;
    try {
      rsm.deleteMapping(rNew);
    } catch (ShardManagementException e) {
      storeOperationFailed = true;
    }

    assert !storeOperationFailed;
  }

  /**
   * Split range mapping in range shard map, abort GSM post Local in Do and GSM post local in Undo.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void splitRangeMappingAbortGSMPostLocalDoAndGSMPostLocalUndo() {
    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createReplaceMappingsOperation5Param = setSplitReplaceMappingOperation(true);

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), sof, new CacheStore(), ShardMapManagerLoadPolicy.Lazy,
        new RetryPolicy(1, Duration.ZERO, Duration.ZERO, Duration.ZERO),
        RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 20), s);

    boolean storeOperationFailed = false;
    try {
      List<RangeMapping> rList = rsm.splitMapping(r1, 10);
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.RangeShardMap.equals(sme.getErrorCategory());
      assert ShardManagementErrorCode.StorageOperationFailure.equals(sme.getErrorCode());
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // Obtain the pending operations.
    List<StoreLogEntry> pendingOperations = ShardMapperTests.getPendingStoreOperations();
    assert pendingOperations.size() == 1;

    // Validation: Mapping range is not updated - lookup for point 10 returns mapping with version 0.
    RangeMapping rValidateLeft = rsm.getMappingForKey(5);
    RangeMapping rValidateRight = rsm.getMappingForKey(15);
    assert r1.getRange().equals(rValidateLeft.getRange());
    assert r1.getRange().equals(rValidateRight.getRange());

    ///#region OpenConnection with Validation

    // Validation should succeed since source mapping was never deleted.
    boolean validationFailed = false;
    try (Connection conn = rsm.openConnection(rValidateLeft, Globals.SHARD_USER_CONN_STRING,
        ConnectionOptions.Validate)) {
      conn.close();
    } catch (ShardManagementException e) {
      validationFailed = true;
    } catch (Exception e) {
      e.printStackTrace();
    }

    assert !validationFailed;

    ///#endregion OpenConnection with Validation

    sof.createReplaceMappingsOperation5Param = setSplitReplaceMappingOperation(false);

    // Try splitting again.
    storeOperationFailed = false;

    try {
      rsm.splitMapping(r1, 10);
    } catch (ShardManagementException e2) {
      storeOperationFailed = true;
    }

    assert !storeOperationFailed;
  }

  /**
   * Merge range mappings in range shard map, abort LSM Source Local in Do and GSM post local in
   * Undo.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void mergeRangeMappingAbortSourceLocalDoAndGSMPostLocalUndo() {
    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createReplaceMappingsOperation5Param = setMergeReplaceMappingOperation(true);

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), sof, new CacheStore(), ShardMapManagerLoadPolicy.Lazy,
        new RetryPolicy(1, Duration.ZERO, Duration.ZERO, Duration.ZERO),
        RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);

    assert rsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);

    Shard s = rsm.createShard(sl);

    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 20), s);
    RangeMapping r2 = rsm.createRangeMapping(new Range(20, 40), s);

    boolean storeOperationFailed = false;
    try {
      RangeMapping rMerged = rsm.mergeMappings(r1, r2);
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.RangeShardMap.equals(sme.getErrorCategory());
      assert ShardManagementErrorCode.StorageOperationFailure.equals(sme.getErrorCode());
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // Obtain the pending operations.
    List<StoreLogEntry> pendingOperations = ShardMapperTests.getPendingStoreOperations();
    assert pendingOperations.size() == 1;

    // Validation: Mapping range is not updated - lookup for point 10 returns mapping with version 0.
    RangeMapping rValidateLeft = rsm.getMappingForKey(5);
    RangeMapping rValidateRight = rsm.getMappingForKey(25);
    assert r1.getRange().equals(rValidateLeft.getRange());
    assert r2.getRange().equals(rValidateRight.getRange());

    ///#region OpenConnection with Validation

    // Validation should succeed since source mapping was never deleted.
    boolean validationFailed = false;
    try {
      try (Connection conn = rsm.openConnection(rValidateLeft, Globals.SHARD_USER_CONN_STRING,
          ConnectionOptions.Validate)) {
        conn.close();
      }

      try (Connection conn = rsm.openConnection(rValidateRight, Globals.SHARD_USER_CONN_STRING,
          ConnectionOptions.Validate)) {
        conn.close();
      }
    } catch (ShardManagementException e) {
      validationFailed = true;
    } catch (Exception e) {
      e.printStackTrace();
    }

    assert !validationFailed;

    ///#endregion OpenConnection with Validation

    sof.createReplaceMappingsOperation5Param = setMergeReplaceMappingOperation(false);

    // Split the range mapping on the left.
    storeOperationFailed = false;
    try {
      rsm.splitMapping(r1, 10);
    } catch (ShardManagementException e2) {
      storeOperationFailed = true;
    }

    assert !storeOperationFailed;
  }

  /**
   * OpenConnectionForKey for unavailable server using ListShardMap.
   */
  private void unavailableServerOpenConnectionForKeyListShardMapInternal(boolean openAsync) {
    StubSqlStoreConnectionFactory scf = new StubSqlStoreConnectionFactory();
    scf.setCallBase(true);
    scf.getUserConnectionString = setGetUserConnectionString(scf, false);

    AtomicInteger callCount = new AtomicInteger(0);

    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createFindMappingByKeyGlobalOperation8Param = setFindMappingByKey(callCount);

    AtomicReference<ICacheStoreMapping> currentMapping = new AtomicReference<>();
    StubICacheStoreMapping sics = new StubICacheStoreMapping();

    StubCacheStore scs = new StubCacheStore();
    scs.setCallBase(true);
    scs.lookupMappingByKeyIStoreShardMapShardKey = setLookupMappingByKey(scs, currentMapping, sics);

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING), scf, sof, scs,
        ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO, Duration.ZERO,
        Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    ListShardMap<Integer> lsm = smm.getListShardMap(ShardMapperTests.listShardMapName,
        ShardKeyType.Int32);
    assert lsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);

    Shard s = lsm.createShard(sl);
    assert s != null;

    PointMapping p1 = lsm.createPointMapping(2, s);
    assert p1 != null;

    // Mapping is there, now let's try to abort the OpenConnectionForKey
    scf.getUserConnectionString = setGetUserConnectionString(scf, true);

    boolean failed;
    for (int i = 1; i <= 10; i++) {
      failed = tryOpenConnectionForKey(lsm, openAsync);
      assert failed;
    }

    //TODO: Implement TTL and check if connection is opened using cache.
    assert 10 == callCount.get();

    long currentTtl = sics.getTimeToLiveMilliseconds();
    assert currentTtl > 0;

    // Let's fake the TTL to be 0, to force another call to store.
    sics.timeToLiveMillisecondsGet = () -> 0L;
    sics.hasTimeToLiveExpired = () -> true;

    failed = tryOpenConnectionForKey(lsm, openAsync);
    assert failed;
    assert 11 == callCount.get();

    sics.timeToLiveMillisecondsGet = currentMapping.get()::getTimeToLiveMilliseconds;
    sics.hasTimeToLiveExpired = currentMapping.get()::hasTimeToLiveExpired;

    failed = tryOpenConnectionForKey(lsm, openAsync);
    assert failed;
    assert sics.getTimeToLiveMilliseconds() <= currentTtl;
    assert 12 == callCount.get();

    scf.getUserConnectionString = setGetUserConnectionString(scf, false);

    failed = tryOpenConnectionForKey(lsm, openAsync);
    assert !failed;
    assert 0 == sics.getTimeToLiveMilliseconds();
    assert 12 == callCount.get();
  }

  /**
   * OpenConnectionForKey for unavailable server using RangeShardMap.
   */
  private void unavailableServerOpenConnectionForKeyRangeShardMapInternal(boolean openAsync) {
    StubSqlStoreConnectionFactory scf = new StubSqlStoreConnectionFactory();
    scf.setCallBase(true);
    scf.getUserConnectionString = setGetUserConnectionString(scf, false);

    AtomicInteger callCount = new AtomicInteger(0);

    StubStoreOperationFactory sof = new StubStoreOperationFactory();
    sof.setCallBase(true);
    sof.createFindMappingByKeyGlobalOperation8Param = setFindMappingByKey(callCount);

    AtomicReference<ICacheStoreMapping> currentMapping = new AtomicReference<>();
    StubICacheStoreMapping sics = new StubICacheStoreMapping();
    StubCacheStore scs = new StubCacheStore();
    scs.setCallBase(true);
    scs.lookupMappingByKeyIStoreShardMapShardKey = setLookupMappingByKey(scs, currentMapping, sics);

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING), scf, sof, scs,
        ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO, Duration.ZERO,
        Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm = smm.getRangeShardMap(ShardMapperTests.rangeShardMapName,
        ShardKeyType.Int32);
    assert rsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapperTests.shardDBs[0]);

    Shard s = rsm.createShard(sl);
    assert s != null;

    RangeMapping r1 = rsm.createRangeMapping(new Range(5, 20), s);
    assert r1 != null;

    // Mapping is there, now let's try to abort the OpenConnectionForKey
    scf.getUserConnectionString = setGetUserConnectionString(scf, true);

    boolean failed;
    for (int i = 1; i <= 10; i++) {
      failed = tryOpenConnectionForKey(rsm, 10, openAsync);
      assert failed;
    }

    //TODO: Implement TTL and check if connection is opened using cache.
    assert 10 == callCount.get();

    long currentTtl = sics.getTimeToLiveMilliseconds();
    assert currentTtl >= 0;

    // Let's fake the TTL to be 0, to force another call to store.
    sics.timeToLiveMillisecondsGet = () -> 0L;
    sics.hasTimeToLiveExpired = () -> true;

    failed = tryOpenConnectionForKey(rsm, 12, openAsync);
    assert failed;
    assert 11 == callCount.get();

    sics.timeToLiveMillisecondsGet = currentMapping.get()::getTimeToLiveMilliseconds;
    sics.hasTimeToLiveExpired = currentMapping.get()::hasTimeToLiveExpired;

    failed = tryOpenConnectionForKey(rsm, 15, openAsync);
    assert failed;
    assert sics.getTimeToLiveMilliseconds() <= currentTtl;
    assert 12 == callCount.get();

    scf.getUserConnectionString = setGetUserConnectionString(scf, false);

    failed = tryOpenConnectionForKey(rsm, 7, openAsync);
    assert !failed;
    assert 12 == callCount.get();
    assert 0 == sics.getTimeToLiveMilliseconds();
  }

  private Func2Param<StoreShardMap, ShardKey, ICacheStoreMapping> setLookupMappingByKey(
      StubCacheStore scs, AtomicReference<ICacheStoreMapping> currentMapping,
      StubICacheStoreMapping sics) {
    return (_ssm, _sk) -> {
      Func2Param<StoreShardMap, ShardKey, ICacheStoreMapping> original
          = scs.lookupMappingByKeyIStoreShardMapShardKey;
      scs.lookupMappingByKeyIStoreShardMapShardKey = null;
      try {
        currentMapping.set(scs.lookupMappingByKey(_ssm, _sk));
        sics.mappingGet = currentMapping.get()::getMapping;
        sics.creationTimeGet = currentMapping.get()::getCreationTime;
        sics.timeToLiveMillisecondsGet = currentMapping.get()::getTimeToLiveMilliseconds;
        sics.resetTimeToLive = currentMapping.get()::resetTimeToLive;
        sics.hasTimeToLiveExpired = currentMapping.get()::hasTimeToLiveExpired;
        return sics;
      } finally {
        scs.lookupMappingByKeyIStoreShardMapShardKey = original;
      }
    };
  }

  private Func8Param<ShardMapManager, String, StoreShardMap, ShardKey, CacheStoreMappingUpdatePolicy, ShardManagementErrorCategory, Boolean, Boolean, IStoreOperationGlobal> setFindMappingByKey(
      AtomicInteger callCount) {
    return (_smm, _opname, _ssm, _sk, _pol, _ec, _cr, _if) -> {
      StubFindMappingByKeyGlobalOperation op = new StubFindMappingByKeyGlobalOperation(_smm,
          _opname, _ssm, _sk, _pol, _ec, _cr, _if);
      op.setCallBase(true);
      op.doGlobalExecuteIStoreTransactionScope = (ts) -> {
        callCount.getAndIncrement();

        // Call the base function, hack for this behavior is to save current operation,
        // set current to null, restore current operation.
        Func1Param<IStoreTransactionScope, StoreResults> original
            = op.doGlobalExecuteIStoreTransactionScope;

        op.doGlobalExecuteIStoreTransactionScope = null;
        try {
          return op.doGlobalExecute(ts);
        } finally {
          op.doGlobalExecuteIStoreTransactionScope = original;
        }
      };
      return op;
    };
  }

  private boolean tryOpenConnectionForKey(ListShardMap lsm, boolean openAsync) {
    boolean failed = false;
    Connection conn = null;
    try {
      if (openAsync) {
        conn = lsm.openConnectionForKeyAsync(2, Globals.SHARD_USER_CONN_STRING).call();
      } else {
        conn = lsm.openConnectionForKey(2, Globals.SHARD_USER_CONN_STRING);
      }
    } catch (Exception ex) {
      if (ex.getCause() != null && ex.getCause() instanceof SQLException) {
        failed = true;
      }
    } finally {
      try {
        if (conn != null && !conn.isClosed()) {
          conn.close();
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    return failed;
  }

  private <KeyT> boolean tryOpenConnectionForKey(RangeShardMap rsm, KeyT key, boolean openAsync) {
    boolean failed = false;
    Connection conn = null;
    try {
      if (openAsync) {
        conn = rsm.openConnectionForKeyAsync(key, Globals.SHARD_USER_CONN_STRING).call();
      } else {
        conn = rsm.openConnectionForKey(key, Globals.SHARD_USER_CONN_STRING);
      }
    } catch (Exception ex) {
      if (ex.getCause() != null && ex.getCause() instanceof SQLException) {
        failed = true;
      }
    } finally {
      try {
        if (conn != null && !conn.isClosed()) {
          conn.close();
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    return failed;
  }

  private Func4Param<ShardMapManager, StoreOperationCode, StoreShardMap, StoreMapping,
      IStoreOperation> setAddMappingOperationAbortGsmDoUndo(boolean shouldThrow) {
    return (_smm, _opcode, _ssm, _sm) -> {
      StubAddMappingOperation op = new StubAddMappingOperation(_smm, _opcode, _ssm, _sm);
      op.setCallBase(true);
      setDoPostLocalAddMappingOperation(op, shouldThrow);
      op.undoGlobalPostLocalExecuteIStoreTransactionScope = (ts) -> {
        if (shouldThrow) {
          throw new StoreException("AddMappingOperation",
              ShardMapFaultHandlingTests.sqlException);
        } else {
          Func1Param<IStoreTransactionScope, StoreResults> original
              = op.undoGlobalPostLocalExecuteIStoreTransactionScope;
          op.undoGlobalPostLocalExecuteIStoreTransactionScope = null;
          try {
            return op.undoGlobalPostLocalExecute(ts);
          } finally {
            op.undoGlobalPostLocalExecuteIStoreTransactionScope = original;
          }
        }
      };
      return op;
    };
  }

  private Func4Param<ShardMapManager, StoreOperationCode, StoreShardMap, StoreMapping,
      IStoreOperation> setAddMappingOperationGsmDo(boolean shouldThrow) {
    return (_smm, _opcode, _ssm, _sm) -> {
      StubAddMappingOperation op = new StubAddMappingOperation(_smm, _opcode, _ssm, _sm);
      op.setCallBase(true);
      setDoPostLocalAddMappingOperation(op, shouldThrow);
      return op;
    };
  }

  private Func4Param<ShardMapManager, StoreOperationCode, StoreShardMap, StoreMapping,
      IStoreOperation> setAddMappingOperationLsmDo(boolean shouldThrow) {
    return (_smm, _opcode, _ssm, _sm) -> {
      StubAddMappingOperation op = new StubAddMappingOperation(_smm, _opcode, _ssm, _sm);
      op.setCallBase(true);
      op.doLocalSourceExecuteIStoreTransactionScope = (ts) -> {
        if (shouldThrow) {
          throw new StoreException("AddMappingOperation",
              ShardMapFaultHandlingTests.sqlException);
        } else {
          Func1Param<IStoreTransactionScope, StoreResults> original
              = op.doLocalSourceExecuteIStoreTransactionScope;
          op.doLocalSourceExecuteIStoreTransactionScope = null;
          try {
            return op.doLocalSourceExecute(ts);
          } finally {
            op.doLocalSourceExecuteIStoreTransactionScope = original;
          }
        }
      };
      return op;
    };
  }

  private void setDoPostLocalAddMappingOperation(StubAddMappingOperation op, boolean shouldThrow) {
    op.doGlobalPostLocalExecuteIStoreTransactionScope = (ts) -> stubOperationAddMapping(op, ts,
        shouldThrow);
  }

  private StoreResults stubOperationAddMapping(StubAddMappingOperation op,
      IStoreTransactionScope ts,
      boolean shouldThrow) {
    if (shouldThrow) {
      throw new StoreException("AddMappingOperation",
          ShardMapFaultHandlingTests.sqlException);
    } else {
      Func1Param<IStoreTransactionScope, StoreResults> original
          = op.doGlobalPostLocalExecuteIStoreTransactionScope;
      op.doGlobalPostLocalExecuteIStoreTransactionScope = null;
      try {
        return op.doGlobalPostLocalExecute(ts);
      } finally {
        op.doGlobalPostLocalExecuteIStoreTransactionScope = original;
      }
    }
  }

  private Func5Param<ShardMapManager, StoreOperationCode, StoreShardMap, StoreMapping, UUID,
      IStoreOperation> setRemoveMappingOperation(boolean shouldThrow) {
    return (_smm, _opcode, _ssm, _sm, _loid) -> {
      StubRemoveMappingOperation op = new StubRemoveMappingOperation(_smm, _opcode, _ssm, _sm,
          _loid);
      op.setCallBase(true);
      op.doLocalSourceExecuteIStoreTransactionScope = (ts) -> {
        if (shouldThrow) {
          throw new StoreException("RemoveMappingOperation",
              ShardMapFaultHandlingTests.sqlException);
        } else {
          Func1Param<IStoreTransactionScope, StoreResults> original
              = op.doLocalSourceExecuteIStoreTransactionScope;
          op.doLocalSourceExecuteIStoreTransactionScope = null;
          try {
            return op.doLocalSourceExecute(ts);
          } finally {
            op.doLocalSourceExecuteIStoreTransactionScope = original;
          }
        }
      };
      op.undoGlobalPostLocalExecuteIStoreTransactionScope = (ts) -> {
        if (shouldThrow) {
          throw new StoreException("RemoveMappingOperation",
              ShardMapFaultHandlingTests.sqlException);
        } else {
          Func1Param<IStoreTransactionScope, StoreResults> original
              = op.undoGlobalPostLocalExecuteIStoreTransactionScope;
          op.undoGlobalPostLocalExecuteIStoreTransactionScope = null;
          try {
            return op.undoGlobalPostLocalExecute(ts);
          } finally {
            op.undoGlobalPostLocalExecuteIStoreTransactionScope = original;
          }
        }
      };
      return op;
    };
  }

  private Func7Param<ShardMapManager, StoreOperationCode, StoreShardMap, StoreMapping, StoreMapping,
      String, UUID, IStoreOperation> setUpdateMappingOperationGsmDo(boolean shouldThrow) {
    return (_smm, _opcode, _ssm, _sms, _smt, _p, _loid) -> {
      StubUpdateMappingOperation op = new StubUpdateMappingOperation(_smm, _opcode, _ssm, _sms,
          _smt, _p, _loid);
      op.setCallBase(true);
      if (shouldThrow) {
        op.doGlobalPostLocalExecuteIStoreTransactionScope = (ts) -> {
          throw new StoreException("UpdateMappingOperation",
              ShardMapFaultHandlingTests.sqlException);
        };
      }
      return op;
    };
  }

  private Func7Param<ShardMapManager, StoreOperationCode, StoreShardMap, StoreMapping, StoreMapping,
      String, UUID, IStoreOperation> setUpdateMappingOperationGsmDoUndo(boolean shouldThrow) {
    return (_smm, _opcode, _ssm, _sms, _smt, _p, _loid) -> {
      StubUpdateMappingOperation op = new StubUpdateMappingOperation(_smm, _opcode, _ssm, _sms,
          _smt, _p, _loid);
      op.setCallBase(true);
      op.doGlobalPostLocalExecuteIStoreTransactionScope = (ts) -> {
        if (shouldThrow) {
          throw new StoreException("UpdateMappingOperation",
              ShardMapFaultHandlingTests.sqlException);
        } else {
          Func1Param<IStoreTransactionScope, StoreResults> original
              = op.doGlobalPostLocalExecuteIStoreTransactionScope;
          op.doGlobalPostLocalExecuteIStoreTransactionScope = null;
          try {
            return op.doGlobalPostLocalExecute(ts);
          } finally {
            op.doGlobalPostLocalExecuteIStoreTransactionScope = original;
          }
        }
      };
      op.undoLocalSourceExecuteIStoreTransactionScope = (ts) -> {
        if (shouldThrow) {
          throw new StoreException("UpdateMappingOperation",
              ShardMapFaultHandlingTests.sqlException);
        } else {
          Func1Param<IStoreTransactionScope, StoreResults> original
              = op.undoLocalSourceExecuteIStoreTransactionScope;
          op.undoLocalSourceExecuteIStoreTransactionScope = null;
          try {
            return op.undoLocalSourceExecute(ts);
          } finally {
            op.undoLocalSourceExecuteIStoreTransactionScope = original;
          }
        }
      };
      return op;
    };
  }

  private Func5Param<ShardMapManager, StoreOperationCode, StoreShardMap,
      List<Pair<StoreMapping, UUID>>, List<Pair<StoreMapping, UUID>>, IStoreOperation>
  setSplitReplaceMappingOperation(boolean shouldThrow) {
    return (_smm, _opcode, _ssm, _smo, _smn) -> {
      StubReplaceMappingsOperation op = new StubReplaceMappingsOperation(_smm, _opcode, _ssm,
          _smo, _smn);
      op.setCallBase(true);
      op.doGlobalPostLocalExecuteIStoreTransactionScope = (ts) -> {
        if (shouldThrow) {
          throw new StoreException("ReplaceMappingOperation",
              ShardMapFaultHandlingTests.sqlException);
        } else {
          Func1Param<IStoreTransactionScope, StoreResults> original
              = op.doGlobalPostLocalExecuteIStoreTransactionScope;
          op.doGlobalPostLocalExecuteIStoreTransactionScope = null;
          try {
            return op.doGlobalPostLocalExecute(ts);
          } finally {
            op.doGlobalPostLocalExecuteIStoreTransactionScope = original;
          }
        }
      };
      setUndoPostLocalReplaceMappingOperation(op, shouldThrow);
      return op;
    };
  }

  private Func5Param<ShardMapManager, StoreOperationCode, StoreShardMap,
      List<Pair<StoreMapping, UUID>>, List<Pair<StoreMapping, UUID>>, IStoreOperation>
  setMergeReplaceMappingOperation(boolean shouldThrow) {
    return (_smm, _opcode, _ssm, _smo, _smn) -> {
      StubReplaceMappingsOperation op = new StubReplaceMappingsOperation(_smm, _opcode, _ssm,
          _smo, _smn);
      op.setCallBase(true);
      op.doLocalSourceExecuteIStoreTransactionScope = (ts) -> {
        if (shouldThrow) {
          throw new StoreException("ReplaceMappingOperation",
              ShardMapFaultHandlingTests.sqlException);
        } else {
          Func1Param<IStoreTransactionScope, StoreResults> original
              = op.doLocalSourceExecuteIStoreTransactionScope;
          op.doLocalSourceExecuteIStoreTransactionScope = null;
          try {
            return op.doLocalSourceExecute(ts);
          } finally {
            op.doLocalSourceExecuteIStoreTransactionScope = original;
          }
        }
      };
      setUndoPostLocalReplaceMappingOperation(op, shouldThrow);
      return op;
    };
  }

  private Func7Param<ShardMapManager, StoreOperationCode, StoreShardMap, StoreMapping, StoreMapping,
      String, UUID, IStoreOperation> setUpdateMappingOperationLsmDo(boolean shouldThrow) {
    return (_smm, _opcode, _ssm, _sms, _smt, _p, _loid) -> {
      StubUpdateMappingOperation op = new StubUpdateMappingOperation(_smm, _opcode, _ssm, _sms,
          _smt, _p, _loid);
      op.setCallBase(true);
      if (shouldThrow) {
        // Abort on target.
        op.doLocalTargetExecuteIStoreTransactionScope = (ts) -> {
          throw new StoreException("UpdateMappingOperation",
              ShardMapFaultHandlingTests.sqlException);
        };
      }
      return op;
    };
  }

  private Func1Param<String, IUserStoreConnection> setGetUserConnectionString(
      StubSqlStoreConnectionFactory scf, boolean shouldThrow) {
    return (cstr) -> {
      if (shouldThrow) {
        throw new StoreException("", ShardMapFaultHandlingTests.sqlException);
      } else {
        Func1Param<String, IUserStoreConnection> original = scf.getUserConnectionString;
        scf.getUserConnectionString = null;
        try {
          return scf.getUserConnection(cstr);
        } finally {
          scf.getUserConnectionString = original;
        }
      }
    };
  }

  private void setUndoPostLocalReplaceMappingOperation(StubReplaceMappingsOperation op,
      boolean shouldThrow) {
    op.undoGlobalPostLocalExecuteIStoreTransactionScope = (ts) -> {
      if (shouldThrow) {
        throw new StoreException("ReplaceMappingOperation",
            ShardMapFaultHandlingTests.sqlException);
      } else {
        Func1Param<IStoreTransactionScope, StoreResults> original
            = op.undoGlobalPostLocalExecuteIStoreTransactionScope;
        op.undoGlobalPostLocalExecuteIStoreTransactionScope = null;
        try {
          return op.undoGlobalPostLocalExecute(ts);
        } finally {
          op.undoGlobalPostLocalExecuteIStoreTransactionScope = original;
        }
      }
    };
  }

  private <T> void addPointMapping(List<T> keysToTest) throws SQLException {
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
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]);
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

  private <T> void addRangeMapping(List<T> keysToTest) throws SQLException {
    CountingCacheStore countingCache = new CountingCacheStore(new CacheStore());

    // TODO:RetryPolicy
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), new StoreOperationFactory(), countingCache,
        ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO, Duration.ZERO,
        Duration.ZERO),
        RetryBehavior.getDefaultRetryBehavior());

    assert 0 < keysToTest.size();

    ShardKeyType type = ShardKey.shardKeyTypeFromType(keysToTest.get(0).getClass());
    RangeShardMap<T> rsm =
        smm.createRangeShardMap(String.format("AddRangeMappingDefault%1$s", type.name()), type);
    assert rsm != null;

    ShardLocation sl =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapperTests.shardDBs[0]);
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
