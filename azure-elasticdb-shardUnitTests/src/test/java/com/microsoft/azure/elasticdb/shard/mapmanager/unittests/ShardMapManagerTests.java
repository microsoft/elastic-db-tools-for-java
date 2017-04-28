package com.microsoft.azure.elasticdb.shard.mapmanager.unittests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryBehavior;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
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
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlShardMapManagerCredentials;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlStoreConnectionFactory;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationFactory;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

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
  public static void shardMapManagerTestsInitialize() throws SQLServerException {
    SQLServerConnection conn = null;
    try {
      conn = (SQLServerConnection) DriverManager
          .getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);
      try (Statement stmt = conn.createStatement()) {
        // Create ShardMapManager database
        String query = String
            .format(Globals.CREATE_DATABASE_QUERY, Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
        stmt.executeUpdate(query);
      } catch (SQLException ex) {
        ex.printStackTrace();
      }

      // Create the shard map manager.
      ShardMapManagerFactory.CreateSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING,
          ShardMapManagerCreateMode.ReplaceExisting);
    } catch (Exception e) {
      System.out
          .printf("Failed to connect to SQL database with connection string:", e.getMessage());
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
  public static void shardMapManagerTestsCleanup() throws SQLServerException {
    SQLServerConnection conn = null;
    try {
      conn = (SQLServerConnection) DriverManager
          .getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);
      // Create ShardMapManager database
      try (Statement stmt = conn.createStatement()) {
        String query = String
            .format(Globals.DROP_DATABASE_QUERY, Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
        stmt.executeUpdate(query);
      } catch (SQLException ex) {
        ex.printStackTrace();
      }
    } catch (Exception e) {
      System.out
          .printf("Failed to connect to SQL database with connection string:", e.getMessage());
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
    ShardMapManager smm = ShardMapManagerFactory
        .GetSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING,
            ShardMapManagerLoadPolicy.Lazy);
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
    ShardMapManager smm = ShardMapManagerFactory
        .GetSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING,
            ShardMapManagerLoadPolicy.Lazy);
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

    ListShardMap<Integer> lsm = smm
        .createListShardMap(ShardMapManagerTests.s_shardMapName, ShardKeyType.Int32);

    assertNotNull(lsm);

    ShardMap smLookup = smm
        .lookupShardMapByName("LookupShardMapByName", ShardMapManagerTests.s_shardMapName, true);
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

    RangeShardMap<Integer> rsm = smm
        .createRangeShardMap(ShardMapManagerTests.s_shardMapName, ShardKeyType.Int32);

    assertNotNull(rsm);
    assertEquals(ShardMapManagerTests.s_shardMapName, rsm.getName());

    ShardMap smLookup = smm
        .lookupShardMapByName("LookupShardMapByName", ShardMapManagerTests.s_shardMapName, true);

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
    ShardMapManager smm = ShardMapManagerFactory
        .GetSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING,
            ShardMapManagerLoadPolicy.Lazy);

    ShardMap sm = smm.createListShardMap(ShardMapManagerTests.s_shardMapName, ShardKeyType.Int32);

    assertNotNull(sm);

    assertEquals(ShardMapManagerTests.s_shardMapName, sm.getName());

    boolean creationFailed = false;

    try {
      ListShardMap<Integer> lsm = smm
          .createListShardMap(ShardMapManagerTests.s_shardMapName, ShardKeyType.Int32);
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
    ShardMapManager smm = ShardMapManagerFactory
        .GetSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING,
            ShardMapManagerLoadPolicy.Lazy);
    ShardMap sm = smm.createRangeShardMap(ShardMapManagerTests.s_shardMapName, ShardKeyType.Int32);
    assertNotNull(sm);

    assertEquals(ShardMapManagerTests.s_shardMapName, sm.getName());

    boolean creationFailed = false;

    try {
      RangeShardMap<Integer> rsm = smm.createRangeShardMap(ShardMapManagerTests.s_shardMapName,
          ShardKeyType.Int32);

    } catch (ShardManagementException sme) {
      assertEquals(ShardManagementErrorCategory.ShardMapManager, sme.getErrorCategory());
      assertEquals(ShardManagementErrorCode.ShardMapAlreadyExists, sme.getErrorCode());
      creationFailed = true;
    }
    assertTrue(creationFailed);
  }
  
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void getShardMapsDefault() throws Exception{
    ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING,
        ShardMapManagerLoadPolicy.Lazy);
    ShardMap sm = smm.createListShardMap(ShardMapManagerTests.s_shardMapName, ShardKeyType.Int32);
    assertNotNull(sm);
    
    assertEquals(ShardMapManagerTests.s_shardMapName, sm.getName());
    
    Iterable<ShardMap> shardMaps = smm.getShardMaps();
    
    int count = 0;
    Iterator<ShardMap> mIter = shardMaps.iterator();
    while(mIter.hasNext()){
      count++;
    }
    assertEquals(1, count);
  }

}
