package com.microsoft.azure.elasticdb.shard.unittests;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryBehavior;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.shard.base.MappingLockToken;
import com.microsoft.azure.elasticdb.shard.base.MappingStatus;
import com.microsoft.azure.elasticdb.shard.base.PointMapping;
import com.microsoft.azure.elasticdb.shard.base.PointMappingUpdate;
import com.microsoft.azure.elasticdb.shard.base.Range;
import com.microsoft.azure.elasticdb.shard.base.RangeMapping;
import com.microsoft.azure.elasticdb.shard.base.Shard;
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
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlShardMapManagerCredentials;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlStoreConnectionFactory;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test related to ShardMapper class and it's methods.
 */
public class DateTimeShardMapperTests {

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
    ListShardMap<LocalDateTime> lsm = null;
    ReferenceObjectHelper<ListShardMap<LocalDateTime>> tempRefLsm
        = new ReferenceObjectHelper<>(lsm);
    if (smm.tryGetListShardMap(DateTimeShardMapperTests.listShardMapName, ShardKeyType.DateTime,
        tempRefLsm)) {
      lsm = tempRefLsm.argValue;
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
      lsm = tempRefLsm.argValue;
    }

    // Remove all existing mappings from the range shard map.
    RangeShardMap<LocalDateTime> rsm = null;
    ReferenceObjectHelper<RangeShardMap<LocalDateTime>> tempRefRsm
        = new ReferenceObjectHelper<>(rsm);
    if (smm.tryGetRangeShardMap(DateTimeShardMapperTests.rangeShardMapName, ShardKeyType.DateTime,
        tempRefRsm)) {
      rsm = tempRefRsm.argValue;
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
      rsm = tempRefRsm.argValue;
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
        stmt.executeUpdate(query);
      } catch (SQLException ex) {
        ex.printStackTrace();
      }

      // Create shard databases
      for (int i = 0; i < DateTimeShardMapperTests.shardDBs.length; i++) {
        try (Statement stmt = conn.createStatement()) {
          String query =
              String.format(Globals.DROP_DATABASE_QUERY, DateTimeShardMapperTests.shardDBs[i]);
          stmt.executeUpdate(query);
        } catch (SQLException ex) {
          ex.printStackTrace();
        }
        try (Statement stmt = conn.createStatement()) {
          String query =
              String.format(Globals.CREATE_DATABASE_QUERY, DateTimeShardMapperTests.shardDBs[i]);
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

      ListShardMap<Integer> lsm = smm.createListShardMap(
          DateTimeShardMapperTests.listShardMapName, ShardKeyType.DateTime);

      assert Objects.equals(DateTimeShardMapperTests.listShardMapName, lsm.getName());

      // Create range shard map.
      RangeShardMap<Integer> rsm = smm.createRangeShardMap(
          DateTimeShardMapperTests.rangeShardMapName, ShardKeyType.DateTime);

      assert Objects.equals(DateTimeShardMapperTests.rangeShardMapName, rsm.getName());
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
      for (int i = 0; i < DateTimeShardMapperTests.shardDBs.length; i++) {
        try (Statement stmt = conn.createStatement()) {
          String query = String.format(Globals.DROP_DATABASE_QUERY,
              DateTimeShardMapperTests.shardDBs[i]);
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

  /**
   * Initializes common state per-test.
   */
  @Before
  public final void shardMapperTestInitialize() {
    DateTimeShardMapperTests.cleanShardMapsHelper();
  }

  /**
   * Cleans up common state per-test.
   */
  @After
  public final void shardMapperTestCleanup() {
    DateTimeShardMapperTests.cleanShardMapsHelper();
  }

  /**
   * All combinations of getting point mappings from a list shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void dateGetPointMappingsForRange() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ListShardMap<LocalDateTime> lsm = smm.getListShardMap(
        DateTimeShardMapperTests.listShardMapName, ShardKeyType.DateTime);

    assert lsm != null;

    Shard s1 = lsm.createShard(new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        DateTimeShardMapperTests.shardDBs[0]));
    assert s1 != null;

    Shard s2 = lsm.createShard(new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        DateTimeShardMapperTests.shardDBs[1]));
    assert s2 != null;

    LocalDateTime val1 = LocalDateTime.now().minusMinutes(10);
    PointMapping p1 = lsm.createPointMapping(val1, s1);
    assert p1 != null;

    LocalDateTime val2 = LocalDateTime.now().minusMinutes(20);
    PointMapping p2 = lsm.createPointMapping(val2, s1);
    assert p2 != null;

    LocalDateTime val3 = LocalDateTime.now().minusMinutes(30);
    PointMapping p3 = lsm.createPointMapping(val3, s2);
    assert p3 != null;

    // Get all mappings in shard map.
    List<PointMapping> allMappings = lsm.getMappings();
    assert 3 == allMappings.size();

    // Get all mappings in specified range.
    Range wantedRange = new Range(val3.plusMinutes(-5), val3.plusMinutes(15));
    List<PointMapping> mappingsInRange = lsm.getMappings(wantedRange);
    assert 2 == mappingsInRange.size();

    // Get all mappings for a shard.
    List<PointMapping> mappingsForShard = lsm.getMappings(s1);
    assert 2 == mappingsForShard.size();

    // Get all mappings in specified range for a particular shard.
    List<PointMapping> mappingsInRangeForShard = lsm.getMappings(wantedRange, s1);
    assert 1 == mappingsInRangeForShard.size();
  }

  /**
   * Add a duplicate point mapping to list shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void dateAddPointMappingDuplicate() {
    CountingCacheStore countingCache = new CountingCacheStore(new CacheStore());

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), new StoreOperationFactory(), countingCache,
        ShardMapManagerLoadPolicy.Lazy,
        new RetryPolicy(1, Duration.ZERO, Duration.ZERO, Duration.ZERO),
        RetryBehavior.getDefaultRetryBehavior());

    ListShardMap<LocalDateTime> lsm = smm.getListShardMap(
        DateTimeShardMapperTests.listShardMapName, ShardKeyType.DateTime);

    assert lsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        DateTimeShardMapperTests.shardDBs[0]);

    Shard s = lsm.createShard(sl);

    assert s != null;

    LocalDateTime val = LocalDateTime.now();
    PointMapping p1 = lsm.createPointMapping(val, s);

    assert p1 != null;

    boolean addFailed = false;
    try {
      // add same point mapping again.
      lsm.createPointMapping(val, s);
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ListShardMap.equals(sme.getErrorCategory());
      assert ShardManagementErrorCode.MappingPointAlreadyMapped.equals(sme.getErrorCode());
      addFailed = true;
    }

    assert addFailed;

    PointMapping p2 = lsm.getMappingForKey(val);

    assert p2 != null;
    assert 0 == countingCache.getLookupMappingHitCount();
  }

  /**
   * Delete existing point mapping from list shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void dateDeletePointMappingDefault() {
    CountingCacheStore countingCache = new CountingCacheStore(new CacheStore());

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), new StoreOperationFactory(), countingCache,
        ShardMapManagerLoadPolicy.Lazy,
        new RetryPolicy(1, Duration.ZERO, Duration.ZERO, Duration.ZERO),
        RetryBehavior.getDefaultRetryBehavior());

    ListShardMap<LocalDateTime> lsm = smm.getListShardMap(
        DateTimeShardMapperTests.listShardMapName, ShardKeyType.DateTime);

    assert lsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        DateTimeShardMapperTests.shardDBs[0]);

    Shard s = lsm.createShard(sl);

    assert s != null;

    LocalDateTime val = LocalDateTime.now();
    PointMapping p1 = lsm.createPointMapping(val, s);
    assert p1 != null;

    PointMapping p2 = lsm.getMappingForKey(val);

    assert p2 != null;
    assert 0 == countingCache.getLookupMappingHitCount();

    // The mapping must be made offline first before it can be deleted.
    PointMappingUpdate ru = new PointMappingUpdate();
    ru.setStatus(MappingStatus.Offline);

    PointMapping mappingToDelete = lsm.updateMapping(p1, ru);

    lsm.deleteMapping(mappingToDelete);

    // Verify that the mapping is removed from cache.
    boolean lookupFailed = false;
    try {
      lsm.getMappingForKey(val);
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ListShardMap.equals(sme.getErrorCategory());
      assert ShardManagementErrorCode.MappingNotFoundForKey.equals(sme.getErrorCode());
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
  public final void dateDeletePointMappingNonExisting() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ListShardMap<LocalDateTime> lsm = smm.getListShardMap(
        DateTimeShardMapperTests.listShardMapName, ShardKeyType.DateTime);

    assert lsm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        DateTimeShardMapperTests.shardDBs[0]);

    Shard s = lsm.createShard(sl);

    assert s != null;

    LocalDateTime val = LocalDateTime.now();
    PointMapping p1 = lsm.createPointMapping(val, s);

    assert p1 != null;

    PointMappingUpdate ru = new PointMappingUpdate();
    ru.setStatus(MappingStatus.Offline);

    // The mapping must be made offline before it can be deleted.
    p1 = lsm.updateMapping(p1, ru);

    lsm.deleteMapping(p1);

    boolean removeFailed = false;

    try {
      lsm.deleteMapping(p1);
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ListShardMap.equals(sme.getErrorCategory());
      assert ShardManagementErrorCode.MappingDoesNotExist.equals(sme.getErrorCode());
      removeFailed = true;
    }

    assert removeFailed;
  }
}
