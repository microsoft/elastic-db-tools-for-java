package com.microsoft.azure.elasticdb.shard.unittests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.microsoft.azure.elasticdb.shard.base.MappingLockToken;
import com.microsoft.azure.elasticdb.shard.base.MappingStatus;
import com.microsoft.azure.elasticdb.shard.base.Range;
import com.microsoft.azure.elasticdb.shard.base.RangeMapping;
import com.microsoft.azure.elasticdb.shard.base.RangeMappingUpdate;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.category.ExcludeFromGatedCheckin;
import com.microsoft.azure.elasticdb.shard.map.ListShardMap;
import com.microsoft.azure.elasticdb.shard.map.RangeShardMap;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerFactory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerLoadPolicy;
import com.microsoft.azure.elasticdb.shard.store.Version;
import com.microsoft.azure.elasticdb.shard.utils.GlobalConstants;
import com.microsoft.azure.elasticdb.shard.utils.SqlUtils;

public class ShardMapManagerUpgradeTests {

  /**
   * Sharded databases to create for the tests.
   */
  private static String[] s_shardedDBs = new String[] {"shard1", "shard2", "shard3"};


  /**
   * Shard maps to create for the tests.
   */
  private static String[] s_shardMapNames = new String[] {"shardMap1", "shardMap2"};

  /**
   * GSM version to deploy initially as part of class constructor.
   */
  private static Version s_initialGsmVersion = new Version(1, 0);

  /**
   * initial LSM version to deploy.
   */
  private static Version s_initialLsmVersion = new Version(1, 0);

  /// #region Common Methods

  /**
   * Initializes common state for tests in this class.
   * 
   * @param testContext The TestContext we are running in.
   */
  @BeforeClass
  public static void ShardMapManagerUpgradeTestsInitialize() {}

  /**
   * Cleans up common state for the all tests in this class.
   */
  @AfterClass
  public static void ShardMapManagerUpgradeTestsCleanup() {}

  /// #endregion Common Methods


  /**
   * Get distinct location from shard map manager.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void getDistinctLocations() {
    // Get shard map manager and 2 shard maps.
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    // Upgrade GSM to latest version
    smm.upgradeGlobalStore(s_initialGsmVersion);

    // create shard maps
    for (String name : ShardMapManagerUpgradeTests.s_shardMapNames) {
      ShardMap sm = smm.<Integer>createListShardMap(name, ShardKeyType.Int32);
      assert sm != null;
    }

    ShardMap sm1 = smm.getShardMap(ShardMapManagerUpgradeTests.s_shardMapNames[0]);
    assert sm1 != null;

    ShardMap sm2 = smm.getShardMap(ShardMapManagerUpgradeTests.s_shardMapNames[1]);
    assert sm2 != null;

    // Add shards to the shard maps.

    ShardLocation sl1 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapManagerUpgradeTests.s_shardedDBs[0]);
    ShardLocation sl2 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapManagerUpgradeTests.s_shardedDBs[1]);
    ShardLocation sl3 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapManagerUpgradeTests.s_shardedDBs[2]);

    Shard s1 = sm1.createShard(sl1);
    Shard s2 = sm1.createShard(sl2);
    Shard s3 = sm1.createShard(sl3);
    Shard s4 = sm2.createShard(sl2);
    Shard s5 = sm2.createShard(sl3);

    int count = 0;

    for (ShardLocation sl : smm.getDistinctShardLocations()) {
      count++;
    }

    assert 3 == count;
  }

  /**
   * Upgrade GSM.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void upgradeGsm() {
    // Get shard map manager
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    // Sanity check setup: version should be 1.0
    VerifyGlobalStore(smm, new Version(1, 0));

    // Upgrade to version 1.0: no-op
    smm.upgradeGlobalStore(new Version(1, 0));
    VerifyGlobalStore(smm, new Version(1, 0));

    // Upgrade to version 1.1
    smm.upgradeGlobalStore(new Version(1, 1));
    VerifyGlobalStore(smm, new Version(1, 1));

    // Upgrade to version 1.2
    smm.upgradeGlobalStore(new Version(1, 2));
    VerifyGlobalStore(smm, new Version(1, 2));

    // Upgrade to latest version
    smm.upgradeGlobalStore(null);
    VerifyGlobalStore(smm, GlobalConstants.GsmVersionClient);
  }

  /**
   * Upgrade LSM.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void upgradeLsm() {
    // Get shard map manager
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    // upgrade GSM to latest version.
    smm.upgradeGlobalStore(null);
    VerifyGlobalStore(smm, GlobalConstants.GsmVersionClient);

    // deploy LSM initial version.
    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, s_shardedDBs[0]);
    smm.upgradeLocalStore(sl, s_initialLsmVersion);

    // upgrade to version 1.1
    smm.upgradeLocalStore(sl, new Version(1, 1));

    // Library is still at LSM major version 1, so adding shard with LSM 1.0 should succeed.
    // Library will see that LSM schema already exists at 'sl' and hence will not deploy LSM again,
    // will just try to add the shard.
    // CreateShard will not work with LSM initial version (1.0) as it only has 'StoreVersion' column
    // in ShardMApManagerLocal table, from 1.1 onwards it follows latest schema for version table.
    ListShardMap<Integer> listsm = smm.<Integer>createListShardMap(
        ShardMapManagerUpgradeTests.s_shardMapNames[0], ShardKeyType.Int32);

    listsm.createShard(sl);


    // upgrade to version 1.2
    smm.upgradeLocalStore(sl, new Version(1, 2));

    // upgrade to latest version (1.2): no-op
    smm.upgradeLocalStore(sl);
  }

  /**
   * Test locking issue with version 1.1 and its fix in version 1.2
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void testLockingFixInVersion1_2() {
    // Get shard map manager
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    // Upgrade to version 1.1
    smm.upgradeGlobalStore(new Version(1, 1));

    // Create a range shard map and add few mappings
    RangeShardMap<Integer> rsm = smm.<Integer>createRangeShardMap(
        ShardMapManagerUpgradeTests.s_shardMapNames[1], ShardKeyType.Int32);
    assert rsm != null;

    Shard s = rsm.createShard(new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapManagerUpgradeTests.s_shardedDBs[0]));
    assert s != null;

    RangeMapping m1 = rsm.createRangeMapping(new Range(1, 10), s);
    RangeMapping m2 = rsm.createRangeMapping(new Range(10, 20), s);
    RangeMapping m3 = rsm.createRangeMapping(new Range(20, 30), s);

    // Lock first 2 mappings with same lockownerid and third with a different lock owner id

    MappingLockToken t1 = MappingLockToken.create();
    MappingLockToken t2 = MappingLockToken.create();

    rsm.lockMapping(m1, t1);
    rsm.lockMapping(m2, t1);
    rsm.lockMapping(m3, t2);

    // now try to unlock using token t2. In store version 1.1 it will unlock all mappings
    rsm.unlockMapping(t2);

    for (RangeMapping m : rsm.getMappings()) {
      assert MappingLockToken.NoLock == rsm.getMappingLockOwner(m);
    }

    // Now upgrade to version 1.2 and try same scenario above.
    smm.upgradeGlobalStore(new Version(1, 2));

    rsm.lockMapping(m1, t1);
    rsm.lockMapping(m2, t1);
    rsm.lockMapping(m3, t2);

    // Unlock using token t1. It should just unlock 2 mappings and leave last one locked.
    rsm.unlockMapping(t1);

    assert MappingLockToken.NoLock == rsm.getMappingLockOwner(rsm.getMappingForKey(5));
    assert MappingLockToken.NoLock == rsm.getMappingLockOwner(rsm.getMappingForKey(15));
    assert t2 == rsm.getMappingLockOwner(rsm.getMappingForKey(25));

    // Cleanup - Delete all mappings. shard will be removed in test cleanup.
    rsm.unlockMapping(t2);
    RangeMappingUpdate ru = new RangeMappingUpdate();
    ru.setStatus(MappingStatus.Offline);

    for (RangeMapping m : rsm.getMappings()) {
      rsm.deleteMapping(rsm.updateMapping(m, ru));
    }
  }

  private void VerifyGlobalStore(ShardMapManager smm, Version targetVersion) {
    // Verify upgrade
    assert targetVersion == GetGlobalStoreVersion();

    String shardMapName = String.format("MyShardMap_%1$s", UUID.randomUUID());
    if (targetVersion != null && Version.isFirstGreaterThan(new Version(1, 1), targetVersion)) {
      ShardManagementException sme = AssertExtensions.<ShardManagementException>assertThrows(
          () -> smm.<Integer>createListShardMap(shardMapName, ShardKeyType.Int32));
      assert ShardManagementErrorCode.GlobalStoreVersionMismatch == sme.getErrorCode();
    } else {
      // Below call should succeed as latest supported major version of library matches major
      // version of deployed store.
      ShardMap sm = smm.<Integer>createListShardMap(shardMapName, ShardKeyType.Int32);
      assert sm != null;
    }
  }


  private Version GetGlobalStoreVersion() {
    return GetVersion(SqlUtils.getCheckIfExistsGlobalScript().toString());
  }

  private Version GetVersion(String getVersionScript) {
    try (
        Connection conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING)) {
      try (Statement stmt = conn.createStatement()) {
        ResultSet reader = stmt.executeQuery(getVersionScript);
        ResultSetMetaData rsmd = reader.getMetaData();
        assert reader.next();
        if (rsmd.getColumnCount() == 2) {
          return new Version(reader.getInt(1), 0);
        } else if (rsmd.getColumnCount() == 3) {
          return new Version(reader.getInt(1), reader.getInt(2));
        } else {
          // throw new AssertFailedException(String.format("Unexpected FieldCount: %1$s",
          // rsmd.getColumnCount()));
        }
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    } catch (SQLException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    return null;
  }



}
