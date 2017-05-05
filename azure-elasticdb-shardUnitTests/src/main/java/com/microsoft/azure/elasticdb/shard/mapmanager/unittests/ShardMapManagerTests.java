package com.microsoft.azure.elasticdb.shard.mapmanager.unittests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryBehavior;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.SqlProtocol;
import com.microsoft.azure.elasticdb.shard.cache.CacheStore;
import com.microsoft.azure.elasticdb.shard.map.ListShardMap;
import com.microsoft.azure.elasticdb.shard.map.RangeShardMap;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerCreateMode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerFactory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerLoadPolicy;
import com.microsoft.azure.elasticdb.shard.mapmanager.category.ExcludeFromGatedCheckin;
import com.microsoft.azure.elasticdb.shard.mapmanager.decorators.CountingCacheStore;
import com.microsoft.azure.elasticdb.shard.mapmanager.stubs.StubCacheStore;
import com.microsoft.azure.elasticdb.shard.mapmanager.stubs.StubStoreOperationFactory;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlShardMapManagerCredentials;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlStoreConnectionFactory;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreException;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationFactory;
import com.microsoft.azure.elasticdb.shard.storeops.mapmanger.AddShardMapGlobalOperation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test related to ShardMapManager class and it's methods.
 */
public class ShardMapManagerTests {

  // Shard map name used in the tests.
  private static String s_shardMapName = "Customer";

  /**
   * Initializes common state for tests in this class.
   */
  @BeforeClass
  public static void shardMapManagerTestsInitialize() throws SQLException {
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

      // Create the shard map manager.
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
   */
  @AfterClass
  public static void shardMapManagerTestsCleanup() throws SQLException {
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);
      // Create ShardMapManager database
      try (Statement stmt = conn.createStatement()) {
        String query =
            String.format(Globals.DROP_DATABASE_QUERY, Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
        stmt.executeUpdate(query);
      } catch (SQLException ex) {
        ex.printStackTrace();
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
  public void shardMapManagerTestInitialize() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);
    try {
      ShardMap sm = smm.getShardMap(ShardMapManagerTests.s_shardMapName);
      smm.deleteShardMap(sm);
    } catch (ShardManagementException smme) {
      assertTrue(smme.getErrorCode() == ShardManagementErrorCode.ShardMapLookupFailure);
    }
  }

  /**
   * Cleans up common state per-test.
   */
  @After
  public void shardMapManagerTestCleanup() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);
    try {
      ShardMap sm = smm.getShardMap(ShardMapManagerTests.s_shardMapName);
      smm.deleteShardMap(sm);
    } catch (ShardManagementException smme) {
      assertTrue(smme.getErrorCode() == ShardManagementErrorCode.ShardMapLookupFailure);
    }
  }

  /**
   * Create list shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void createListShardMapDefault() throws Exception {
    CountingCacheStore cacheStore = new CountingCacheStore(new CacheStore());

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), new StoreOperationFactory(), cacheStore,
        ShardMapManagerLoadPolicy.Lazy, RetryPolicy.DefaultRetryPolicy,
        RetryBehavior.getDefaultRetryBehavior());

    ListShardMap<Integer> lsm =
        smm.createListShardMap(ShardMapManagerTests.s_shardMapName, ShardKeyType.Int32);

    assertNotNull(lsm);

    ShardMap smLookup =
        smm.lookupShardMapByName("LookupShardMapByName", ShardMapManagerTests.s_shardMapName, true);
    assertNotNull(smLookup);
    assertEquals(ShardMapManagerTests.s_shardMapName, smLookup.getName());
    assertEquals(1, cacheStore.getLookupShardMapCount());
    assertEquals(1, cacheStore.getLookupShardMapHitCount());
  }

  /**
   * Create range shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void createRangeShardMapDefault() throws Exception {
    CountingCacheStore cacheStore = new CountingCacheStore(new CacheStore());

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), new StoreOperationFactory(), cacheStore,
        ShardMapManagerLoadPolicy.Lazy, RetryPolicy.DefaultRetryPolicy,
        RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm =
        smm.createRangeShardMap(ShardMapManagerTests.s_shardMapName, ShardKeyType.Int32);

    assertNotNull(rsm);
    assertEquals(ShardMapManagerTests.s_shardMapName, rsm.getName());

    ShardMap smLookup =
        smm.lookupShardMapByName("LookupShardMapByName", ShardMapManagerTests.s_shardMapName, true);

    assertNotNull(smLookup);
    assertEquals(ShardMapManagerTests.s_shardMapName, smLookup.getName());
    assertEquals(1, cacheStore.getLookupShardMapCount());
    assertEquals(1, cacheStore.getLookupShardMapHitCount());

  }

  /**
   * Add a list shard map with duplicate name to shard map manager.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void CreateListShardMapDuplicate() throws Exception {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ShardMap sm = smm.createListShardMap(ShardMapManagerTests.s_shardMapName, ShardKeyType.Int32);

    assertNotNull(sm);

    assertEquals(ShardMapManagerTests.s_shardMapName, sm.getName());

    boolean creationFailed = false;

    try {
      ListShardMap<Integer> lsm =
          smm.createListShardMap(ShardMapManagerTests.s_shardMapName, ShardKeyType.Int32);
    } catch (ShardManagementException sme) {
      assertEquals(ShardManagementErrorCategory.ShardMapManager, sme.getErrorCategory());
      assertEquals(ShardManagementErrorCode.ShardMapAlreadyExists, sme.getErrorCode());
      creationFailed = true;
    }
    assertTrue(creationFailed);
  }

  /**
   * Add a range shard map with duplicate name to shard map manager.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void CreateRangeShardMapDuplicate() throws Exception {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);
    ShardMap sm = smm.createRangeShardMap(ShardMapManagerTests.s_shardMapName, ShardKeyType.Int32);
    assertNotNull(sm);

    assertEquals(ShardMapManagerTests.s_shardMapName, sm.getName());

    boolean creationFailed = false;

    try {
      RangeShardMap<Integer> rsm =
          smm.createRangeShardMap(ShardMapManagerTests.s_shardMapName, ShardKeyType.Int32);

    } catch (ShardManagementException sme) {
      assertEquals(ShardManagementErrorCategory.ShardMapManager, sme.getErrorCategory());
      assertEquals(ShardManagementErrorCode.ShardMapAlreadyExists, sme.getErrorCode());
      creationFailed = true;
    }
    assertTrue(creationFailed);
  }

  /**
   * Get all shard maps from shard map manager.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void getShardMapsDefault() throws Exception {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);
    ShardMap sm = smm.createListShardMap(ShardMapManagerTests.s_shardMapName, ShardKeyType.Int32);
    assertNotNull(sm);

    assertEquals(ShardMapManagerTests.s_shardMapName, sm.getName());

    List<ShardMap> shardMaps = smm.getShardMaps();

    assertEquals(1, shardMaps.size());
  }

  /**
   * Remove a default shard map from shard map manager.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void deleteListShardMap() throws Exception {

    CountingCacheStore cacheStore = new CountingCacheStore(new CacheStore());

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), new StoreOperationFactory(), cacheStore,
        ShardMapManagerLoadPolicy.Lazy, RetryPolicy.DefaultRetryPolicy,
        RetryBehavior.getDefaultRetryBehavior());

    ShardMap sm = smm.createListShardMap(ShardMapManagerTests.s_shardMapName, ShardKeyType.Int32);

    assertNotNull(sm);

    assertEquals(ShardMapManagerTests.s_shardMapName, sm.getName());

    ShardMap smLookup =
        smm.lookupShardMapByName("LookupShardMapByName", ShardMapManagerTests.s_shardMapName, true);

    assertNotNull(smLookup);
    assertEquals(1, cacheStore.getLookupShardMapCount());
    assertEquals(1, cacheStore.getLookupShardMapHitCount());

    smm.deleteShardMap(sm);

    assertEquals(1, cacheStore.getDeleteShardMapCount());

    cacheStore.resetCounters();

    // Verify that shard map is removed from cache.
    ShardMap smLookupFailure =
        smm.lookupShardMapByName("LookupShardMapByName", ShardMapManagerTests.s_shardMapName, true);

    assertEquals(null, smLookupFailure);
    assertEquals(1, cacheStore.getLookupShardMapCount());
    assertEquals(1, cacheStore.getLookupShardMapMissCount());
  }

  /**
   * Remove non-existing shard map
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void deleteShardMapNonExisting() throws Exception {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ShardMap sm = smm.createListShardMap(ShardMapManagerTests.s_shardMapName, ShardKeyType.Int32);

    assertNotNull(sm);

    assertEquals(ShardMapManagerTests.s_shardMapName, sm.getName());

    smm.deleteShardMap(sm);

    smm.deleteShardMap(sm);
  }

  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void testShardMapManagerExceptionSerializability() throws Exception {

    ShardManagementErrorCategory errorCategory = ShardManagementErrorCategory.RangeShardMap;
    ShardManagementErrorCode errorCode = ShardManagementErrorCode.ShardMapDoesNotExist;

    ShardManagementException ex = new ShardManagementException(errorCategory, errorCode, "Testing");
    String exceptionToString = ex.toString();

    // Serialize and de-serialize with a BinaryFormatter
    ByteArrayOutputStream bs = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bs)) {
      // Serialize
      out.writeObject(ex);
      out.flush();
    }
    ByteArrayInputStream bais = new ByteArrayInputStream(bs.toByteArray());
    try (ObjectInputStream in = new ObjectInputStream(bais)) {
      // Deserialize
      ex = (ShardManagementException) in.readObject();
    }

    // Validate
    assertEquals(ex.getErrorCode(), errorCode);
    assertEquals(ex.getErrorCategory(), errorCategory);
    assertEquals(exceptionToString, ex.toString());
  }

  // region CacheAbortTests

  /**
   * Add a list shard map to shard map manager, do not add it to cache.
   * 
   * @throws Exception
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void addListShardMapNoCacheUpdate() throws Exception {

    // Create a cache store that always misses.
    StubCacheStore stubCacheStore = new StubCacheStore();
    stubCacheStore.setCallBase(true);
    stubCacheStore.LookupMappingByKeyIStoreShardMapShardKey = (ssm, sk) -> null;
    stubCacheStore.LookupShardMapByNameString = (n) -> null;

    CountingCacheStore cacheStore = new CountingCacheStore(stubCacheStore);

    // CountingCacheStore cacheStore = new CountingCacheStore(new StubCacheStore() {CallBase = true,
    // LookupMappingByKeyIStoreShardMapShardKey = (ssm, sk) -> null, LookupShardMapByNameString =
    // (n) -> null});

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), new StoreOperationFactory(), cacheStore,
        ShardMapManagerLoadPolicy.Lazy, RetryPolicy.DefaultRetryPolicy,
        RetryBehavior.getDefaultRetryBehavior());

    ShardMap sm = smm.createListShardMap(ShardMapManagerTests.s_shardMapName, ShardKeyType.Int32);
    assertNotNull(sm);
    assertEquals(ShardMapManagerTests.s_shardMapName, sm.getName());
    assertEquals(1, cacheStore.getAddShardMapCount());
    cacheStore.resetCounters();

    ShardMap smLookup =
        smm.lookupShardMapByName("LookupShardMapByName", ShardMapManagerTests.s_shardMapName, true);

    assertNotNull(smLookup);
    assertEquals(1, cacheStore.getAddShardMapCount());
    assertEquals(1, cacheStore.getLookupShardMapMissCount());

  }

  /**
   * Remove a default shard map from shard map manager, do not remove it from cache.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void removeListShardMapNoCacheUpdate() {

    StubCacheStore stubCacheStore = new StubCacheStore();
    stubCacheStore.setCallBase(true);
    stubCacheStore.DeleteMappingIStoreMapping = (csm) -> {
    };

    CountingCacheStore cacheStore = new CountingCacheStore(stubCacheStore);

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), new StoreOperationFactory(), cacheStore,
        ShardMapManagerLoadPolicy.Lazy, RetryPolicy.DefaultRetryPolicy,
        RetryBehavior.getDefaultRetryBehavior());

    ShardMap sm = smm.createListShardMap(ShardMapManagerTests.s_shardMapName, ShardKeyType.Int32);
    assertNotNull(sm);

    assertEquals(ShardMapManagerTests.s_shardMapName, sm.getName());

    smm.deleteShardMap(sm);

    assertEquals(1, cacheStore.getDeleteShardMapCount());

    ShardMap smLookup =
        smm.lookupShardMapByName("LookupShardMapByName", ShardMapManagerTests.s_shardMapName, true);

    assertNotNull(smLookup);
    assertEquals(1, cacheStore.getLookupShardMapHitCount());
  }

  // endregion CacheAbortTests

  // region ShardLocationTests
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testShardLocationPort() {
    String serverName = "testservername";
    String databaseName = "testdatabasename";
    SqlProtocol protocol = SqlProtocol.Default;

    // Below valid range
    AssertExtensions.<IllegalArgumentException>AssertThrows(
        () -> new ShardLocation(serverName, databaseName, protocol, Integer.MIN_VALUE));
    AssertExtensions.<IllegalArgumentException>AssertThrows(
        () -> new ShardLocation(serverName, databaseName, protocol, -1));

    // In valid range
    new ShardLocation(serverName, databaseName, protocol, 0);
    new ShardLocation(serverName, databaseName, protocol, 1);
    new ShardLocation(serverName, databaseName, protocol, 65535);

    // Above valid range
    AssertExtensions.<IllegalArgumentException>AssertThrows(
        () -> new ShardLocation(serverName, databaseName, protocol, 65536));
    AssertExtensions.<IllegalArgumentException>AssertThrows(
        () -> new ShardLocation(serverName, databaseName, protocol, Integer.MAX_VALUE));
  }

  // endregion

  // region GsmAbortTests

  private class NTimeFailingAddShardMapGlobalOperation extends AddShardMapGlobalOperation {
    private int _failureCountMax;
    private int _currentFailureCount;

    public NTimeFailingAddShardMapGlobalOperation(int failureCountMax,
        ShardMapManager shardMapManager, String operationName, StoreShardMap shardMap) {
      super(shardMapManager, operationName, shardMap);
      _failureCountMax = failureCountMax;
      _currentFailureCount = 0;
    }

    @Override
    public StoreResults doGlobalExecute(IStoreTransactionScope ts) {
      if (_currentFailureCount < _failureCountMax) {
        _currentFailureCount++;

        throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
      } else {
        return super.doGlobalExecute(ts);
      }
    }
  }

  private class NTimeFailingRemoveShardMapGlobalOperation extends AddShardMapGlobalOperation {
    private int _failureCountMax;
    private int _currentFailureCount;

    public NTimeFailingRemoveShardMapGlobalOperation(int failureCountMax,
        ShardMapManager shardMapManager, String operationName, StoreShardMap shardMap) {
      super(shardMapManager, operationName, shardMap);
      _failureCountMax = failureCountMax;
      _currentFailureCount = 0;
    }

    @Override
    public StoreResults doGlobalExecute(IStoreTransactionScope ts) {
      if (_currentFailureCount < _failureCountMax) {
        _currentFailureCount++;

        throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
      } else {
        return super.doGlobalExecute(ts);
      }
    }
  }

  /**
   * Remove a default shard map from shard map manager, do not commit GSM transaction.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void removeListShardMapAbortGSM() {
    StubStoreOperationFactory stubStoreOperationFactory = new StubStoreOperationFactory();
    stubStoreOperationFactory.setCallBase(true);
    stubStoreOperationFactory.CreateRemoveShardMapGlobalOperationShardMapManagerStringIStoreShardMap =
        (_smm, _opname, _ssm) -> new NTimeFailingRemoveShardMapGlobalOperation(10, _smm, _opname,
            _ssm);

    // TODO : new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero)
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING),
        new SqlStoreConnectionFactory(), stubStoreOperationFactory, new CacheStore(),
        ShardMapManagerLoadPolicy.Lazy, RetryPolicy.DefaultRetryPolicy,
        RetryBehavior.getDefaultRetryBehavior());

    ShardMap sm = smm.createListShardMap(ShardMapManagerTests.s_shardMapName, ShardKeyType.Int32);

    assert sm != null;

    assert ShardMapManagerTests.s_shardMapName == sm.getName();

    boolean storeOperationFailed = false;
    try {
      smm.deleteShardMap(sm);
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ShardMapManager == sme.getErrorCategory();
      assert ShardManagementErrorCode.StorageOperationFailure == sme.getErrorCode();
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // Verify that shard map still exist in store.
    ShardMap smNew = smm.lookupShardMapByName("LookupShardMapByName",
        ShardMapManagerTests.s_shardMapName, false);
    assert smNew != null;
  }

  /**
   * Create list shard map, do not commit GSM transaction.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void createListShardMapAbortGSM() {
    StubStoreOperationFactory stubOperationFactory = new StubStoreOperationFactory();
    stubOperationFactory.setCallBase(true);
    stubOperationFactory.CreateAddShardMapGlobalOperationShardMapManagerStringIStoreShardMap =
        (_smm, _opname, _ssm) -> new NTimeFailingAddShardMapGlobalOperation(10, _smm, _opname,
            _ssm);

    // TODO:new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero)
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), stubOperationFactory, new CacheStore(),
        ShardMapManagerLoadPolicy.Lazy, RetryPolicy.DefaultRetryPolicy,
        RetryBehavior.getDefaultRetryBehavior());

    boolean storeOperationFailed = false;
    try {
      ListShardMap<Integer> lsm =
          smm.createListShardMap(ShardMapManagerTests.s_shardMapName, ShardKeyType.Int32);
      assert lsm != null;
      assert ShardMapManagerTests.s_shardMapName == lsm.getName();
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ShardMapManager == sme.getErrorCategory();
      assert ShardManagementErrorCode.StorageOperationFailure == sme.getErrorCode();
      storeOperationFailed = true;
    }

    assert storeOperationFailed;
  }

  /**
   * Create range shard map, do not commit GSM transaction.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void createRangeShardMapAbortGSM() {
    StubStoreOperationFactory stubOperationFactory = new StubStoreOperationFactory();
    stubOperationFactory.setCallBase(true);
    stubOperationFactory.CreateAddShardMapGlobalOperationShardMapManagerStringIStoreShardMap =
        (_smm, _opname, _ssm) -> new NTimeFailingAddShardMapGlobalOperation(10, _smm, _opname,
            _ssm);

    // TODO: new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero)
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), stubOperationFactory, new CacheStore(),
        ShardMapManagerLoadPolicy.Lazy, RetryPolicy.DefaultRetryPolicy,
        RetryBehavior.getDefaultRetryBehavior());

    boolean storeOperationFailed = false;
    try {
      RangeShardMap<Integer> rsm =
          smm.createRangeShardMap(ShardMapManagerTests.s_shardMapName, ShardKeyType.Int32);
      assert rsm != null;
      assert ShardMapManagerTests.s_shardMapName == rsm.getName();
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ShardMapManager == sme.getErrorCategory();
      assert ShardManagementErrorCode.StorageOperationFailure == sme.getErrorCode();
      storeOperationFailed = true;
    }

    assert storeOperationFailed;
  }
  // endregion GsmAbortTests
}


