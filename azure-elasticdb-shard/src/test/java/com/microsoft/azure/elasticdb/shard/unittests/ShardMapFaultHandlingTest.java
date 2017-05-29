package com.microsoft.azure.elasticdb.shard.unittests;

import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryBehavior;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.shard.base.MappingStatus;
import com.microsoft.azure.elasticdb.shard.base.PointMapping;
import com.microsoft.azure.elasticdb.shard.base.Range;
import com.microsoft.azure.elasticdb.shard.base.RangeMapping;
import com.microsoft.azure.elasticdb.shard.base.RangeMappingUpdate;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.ShardStatus;
import com.microsoft.azure.elasticdb.shard.base.ShardUpdate;
import com.microsoft.azure.elasticdb.shard.cache.CacheStore;
import com.microsoft.azure.elasticdb.shard.category.ExcludeFromGatedCheckin;
import com.microsoft.azure.elasticdb.shard.map.ListShardMap;
import com.microsoft.azure.elasticdb.shard.map.RangeShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerCreateMode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerFactory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerLoadPolicy;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlShardMapManagerCredentials;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlStoreConnectionFactory;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreException;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreShard;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationCode;
import com.microsoft.azure.elasticdb.shard.storeops.map.AddShardOperation;
import com.microsoft.azure.elasticdb.shard.storeops.map.RemoveShardOperation;
import com.microsoft.azure.elasticdb.shard.storeops.map.UpdateShardOperation;
import com.microsoft.azure.elasticdb.shard.storeops.mapper.AddMappingOperation;
import com.microsoft.azure.elasticdb.shard.storeops.mapper.RemoveMappingOperation;
import com.microsoft.azure.elasticdb.shard.storeops.mapper.ReplaceMappingsOperation;
import com.microsoft.azure.elasticdb.shard.storeops.mapper.UpdateMappingOperation;
import com.microsoft.azure.elasticdb.shard.stubs.StubStoreOperationFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class ShardMapFaultHandlingTest {

  public static SQLException TransientSqlException = ShardMapFaultHandlingTest.createSqlException();

  /**
   * Sharded databases to create for the test.
   */
  private static String[] s_shardedDBs = new String[]{"shard1", "shard2"};

  /**
   * List shard map name.
   */
  private static String s_listShardMapName = "Customers_list";

  /**
   * Range shard map name.
   */
  private static String s_rangeShardMapName = "Customers_range";

  /// #region Common Methods

  /**
   * Helper function to clean list and range shard maps.
   */
  private static void cleanShardMapsHelper() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    // Remove all existing mappings from the list shard map.
    ListShardMap<Integer> lsm =
        smm.<Integer>getListShardMap(ShardMapFaultHandlingTest.s_listShardMapName);
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

    // Remove all existing mappings from the range shard map.
    RangeShardMap<Integer> rsm =
        smm.<Integer>getRangeShardMap(ShardMapFaultHandlingTest.s_rangeShardMapName);
    assert rsm != null;

    for (RangeMapping rm : rsm.getMappings()) {
      RangeMapping rmOffline = rsm.markMappingOffline(rm);
      assert rmOffline != null;
      rsm.deleteMapping(rmOffline);
    }

    // Remove all shards from range shard map
    for (Shard s : rsm.getShards()) {
      rsm.deleteShard(s);
    }
  }

  /**
   * Initializes common state for tests in this class.
   */
  @BeforeClass
  public static void shardMapFaultHandlingTestsInitialize() {
    try (
        Connection conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING)) {
      // Create ShardMapManager database
      try (Statement stmt = conn.createStatement()) {
        String query =
            String.format(Globals.CREATE_DATABASE_QUERY, Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
        stmt.executeQuery(query);
      } catch (SQLException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }

      // Create shard databases
      for (int i = 0; i < ShardMapFaultHandlingTest.s_shardedDBs.length; i++) {
        try (Statement stmt = conn.createStatement()) {
          String query =
              String.format(Globals.DROP_DATABASE_QUERY, ShardMapFaultHandlingTest.s_shardedDBs[i]);
          stmt.executeQuery(query);
        } catch (SQLException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }

        try (Statement stmt = conn.createStatement()) {
          String query = String.format(Globals.CREATE_DATABASE_QUERY,
              ShardMapFaultHandlingTest.s_shardedDBs[i]);
          stmt.executeQuery(query);
        } catch (SQLException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    } catch (SQLException e2) {
      // TODO Auto-generated catch block
      e2.printStackTrace();
    }

    // Create shard map manager.
    ShardMapManagerFactory.createSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING,
        ShardMapManagerCreateMode.ReplaceExisting);

    // Create list shard map.
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ListShardMap<Integer> lsm =
        smm.createListShardMap(ShardMapFaultHandlingTest.s_listShardMapName, ShardKeyType.Int32);

    assert lsm != null;

    assert ShardMapFaultHandlingTest.s_listShardMapName == lsm.getName();

    // Create range shard map.
    RangeShardMap<Integer> rsm =
        smm.createRangeShardMap(ShardMapFaultHandlingTest.s_rangeShardMapName, ShardKeyType.Int32);

    assert rsm != null;

    assert ShardMapFaultHandlingTest.s_rangeShardMapName == rsm.getName();
  }

  /**
   * Cleans up common state for the all tests in this class.
   */
  @AfterClass
  public static void shardMapFaultHandlingTestsCleanup() {

    try (
        Connection conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING)) {
      // Drop shard databases
      for (int i = 0; i < ShardMapFaultHandlingTest.s_shardedDBs.length; i++) {
        try (Statement stmt = conn.createStatement()) {
          String query =
              String.format(Globals.DROP_DATABASE_QUERY, ShardMapFaultHandlingTest.s_shardedDBs[i]);
          stmt.executeQuery(query);
        }
      }

      // Drop shard map manager database
      try (Statement stmt = conn.createStatement()) {
        String query =
            String.format(Globals.DROP_DATABASE_QUERY, Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
        stmt.executeQuery(query);
      }
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  // TODO:Reflection in java
  private static SQLException createSqlException() {
    /*
     * java.lang.reflect.Constructor ciSqlError = SqlError.class
     * .GetConstructors(BindingFlags.Instance.getValue() | BindingFlags.NonPublic.getValue())
     * .Single(c -> c.GetParameters().getLength() == 7);
     *
     * // C# TO JAVA CONVERTER WARNING: Unsigned integer types have no direct equivalent in Java: //
     * ORIGINAL LINE: SqlError se = (SqlError)ciSqlError.Invoke(new object[] { (int)10928, (byte)0,
     * // (byte)0, "", "", "", (int)0 }); SqlError se = (SqlError) ciSqlError .newInstance(new
     * Object[] {(int) 10928, (byte) 0, (byte) 0, "", "", "", (int) 0});
     *
     * java.lang.reflect.Constructor ciSqlErrorCollection = SqlErrorCollection.class
     * .GetConstructors(BindingFlags.Instance.getValue() | BindingFlags.NonPublic.getValue())
     * .Single();
     *
     * SqlErrorCollection sec = (SqlErrorCollection) ciSqlErrorCollection.newInstance(new
     * Object[0]);
     *
     * java.lang.reflect.Method miSqlErrorCollectionAdd = SqlErrorCollection.class.GetMethod("Add",
     * BindingFlags.Instance.getValue() | BindingFlags.NonPublic.getValue());
     *
     * miSqlErrorCollectionAdd.Invoke(sec, new Object[] {se});
     *
     * java.lang.reflect.Method miSqlExceptionCreate =
     * SQLException.class.GetMethod("CreateException", BindingFlags.Static.getValue() |
     * BindingFlags.NonPublic.getValue(), null, new java.lang.Class[] {SqlErrorCollection.class,
     * String.class}, null);
     *
     * SQLException sqlException = (SQLException) miSqlExceptionCreate.invoke(null, new Object[]
     * {sec, ""});
     */

    return null;
  }

  /**
   * Initializes common state per-test.
   */
  @Before
  public final void shardMapperTestInitialize() {
    ShardMapFaultHandlingTest.cleanShardMapsHelper();
  }

  /// #endregion Common Methods

  /**
   * Cleans up common state per-test.
   */
  @After
  public void shardMapperTestCleanup() {
    ShardMapFaultHandlingTest.cleanShardMapsHelper();
  }

  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void addPointMappingFailGsmAfterSuccessLsmSingleRetry() {
    StubStoreOperationFactory stubStoreOperationFactory = new StubStoreOperationFactory();
    stubStoreOperationFactory.setCallBase(true);
    stubStoreOperationFactory.createAddMappingOperation4Param = (smm, opCode, ssm,
        sm) -> new NTimeFailingAddMappingOperation(1, smm, opCode, ssm, sm);

    // new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero)
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), stubStoreOperationFactory, new CacheStore(),
        ShardMapManagerLoadPolicy.Lazy, RetryPolicy.getDefaultRetryPolicy(),
        RetryBehavior.getDefaultRetryBehavior());

    ListShardMap<Integer> lsm =
        smm.<Integer>getListShardMap(ShardMapFaultHandlingTest.s_listShardMapName);

    assert lsm != null;

    Shard s = lsm.createShard(new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapFaultHandlingTest.s_shardedDBs[0]));

    assert s != null;

    boolean failed = false;

    try {
      PointMapping p1 = lsm.createPointMapping(2, s);
    } catch (ShardManagementException e) {
      failed = true;
    }

    assert !failed;
  }

  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void addPointMappingFailGsmAfterSuccessLsm() {
    StubStoreOperationFactory ssof = new StubStoreOperationFactory();
    ssof.setCallBase(true);
    ssof.createAddMappingOperation4Param = (smm, opCode, ssm,
        sm) -> new NTimeFailingAddMappingOperation(2, smm, opCode, ssm, sm);

    // new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero)
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), ssof, new CacheStore(), ShardMapManagerLoadPolicy.Lazy,
        RetryPolicy.getDefaultRetryPolicy(), RetryBehavior.getDefaultRetryBehavior());

    ListShardMap<Integer> lsm =
        smm.<Integer>getListShardMap(ShardMapFaultHandlingTest.s_listShardMapName);

    assert lsm != null;

    Shard s = lsm.createShard(new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapFaultHandlingTest.s_shardedDBs[0]));

    assert s != null;

    boolean failed = false;

    try {
      // Inject GSM transaction failure at GSM commit time.
      PointMapping p1 = lsm.createPointMapping(2, s);
    } catch (ShardManagementException e) {
      failed = true;
    }

    assert failed;

    failed = false;

    ssof.createAddMappingOperation4Param = null;

    try {
      PointMapping p1 = lsm.createPointMapping(2, s);
    } catch (ShardManagementException e2) {
      failed = true;
    }

    assert !failed;
  }

  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void addRangeMappingFailGsmAfterSuccessLsmSingleRetry() {

    StubStoreOperationFactory stubStoreOperationFactory = new StubStoreOperationFactory();
    stubStoreOperationFactory.setCallBase(true);
    stubStoreOperationFactory.createAddMappingOperation4Param = (smm, opCode, ssm,
        sm) -> new NTimeFailingAddMappingOperation(1, smm, opCode, ssm, sm);

    // TODO:new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero)
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), stubStoreOperationFactory, new CacheStore(),
        ShardMapManagerLoadPolicy.Lazy, RetryPolicy.getDefaultRetryPolicy(),
        RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm =
        smm.<Integer>getRangeShardMap(ShardMapFaultHandlingTest.s_rangeShardMapName);

    assert rsm != null;

    Shard s = rsm.createShard(new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapFaultHandlingTest.s_shardedDBs[0]));

    assert s != null;

    boolean failed = false;

    try {
      // Inject GSM transaction failure at GSM commit time.
      RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);
    } catch (ShardManagementException e) {
      failed = true;
    }

    assert !failed;
  }

  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void addRangeMappingFailGsmAfterSuccessLsm() {
    StubStoreOperationFactory ssof = new StubStoreOperationFactory();
    ssof.setCallBase(true);
    ssof.createAddMappingOperation4Param = (smm, opCode, ssm,
        sm) -> new NTimeFailingAddMappingOperation(2, smm, opCode, ssm, sm);

    // TODO:new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero)
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), ssof, new CacheStore(), ShardMapManagerLoadPolicy.Lazy,
        RetryPolicy.getDefaultRetryPolicy(), RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm =
        smm.<Integer>getRangeShardMap(ShardMapFaultHandlingTest.s_rangeShardMapName);

    assert rsm != null;

    Shard s = rsm.createShard(new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapFaultHandlingTest.s_shardedDBs[0]));

    assert s != null;

    boolean failed = false;

    try {
      // Inject GSM transaction failure at GSM commit time.
      RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);
    } catch (ShardManagementException e) {
      failed = true;
    }

    assert failed;

    failed = false;

    ssof.createAddMappingOperation4Param = null;

    try {
      RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s);
    } catch (ShardManagementException e2) {
      failed = true;
    }

    assert !failed;
  }

  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void shardMapOperationsFailureAfterGlobalPreLocal() {
    StubStoreOperationFactory ssof = new StubStoreOperationFactory();
    ssof.setCallBase(true);
    ssof.createAddShardOperationShardMapManagerIStoreShardMapIStoreShard =
        (smm, sm, s) -> new AddShardOperationFailAfterGlobalPreLocal(smm, sm, s);
    ssof.createRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard =
        (smm, sm, s) -> new RemoveShardOperationFailAfterGlobalPreLocal(smm, sm, s);
    ssof.createUpdateShardOperation4Param = (smm, sm, sold,
        shardNew) -> new UpdateShardOperationFailAfterGlobalPreLocal(smm, sm, sold, shardNew);
    ssof.createAddMappingOperation4Param = (smm, opCode, ssm,
        sm) -> new AddMappingOperationFailAfterGlobalPreLocal(smm, opCode, ssm, sm);
    ssof.createRemoveMappingOperation5Param =
        (smm, opCode, sm, mapping, loid) -> new RemoveMappingOperationFailAfterGlobalPreLocal(
            smm, opCode, sm, mapping, loid);
    ssof.createUpdateMappingOperation7Param =
        (shardMapManager, operationCode, shardMap, mappingSource, mappingTarget,
            patternForKill, lockOwnerId) -> new UpdateMappingOperationFailAfterGlobalPreLocal(
            shardMapManager, operationCode, shardMap, mappingSource, mappingTarget,
            patternForKill, lockOwnerId);
    ssof.createReplaceMappingsOperation5Param = (smm, opCode, sm, mappingSource,
        mappingTarget) -> new ReplaceMappingsOperationFailAfterGlobalPreLocal(smm, opCode, sm,
        mappingSource, mappingTarget);

    // TODO:RetryPolicy
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), ssof, new CacheStore(), ShardMapManagerLoadPolicy.Lazy,
        RetryPolicy.getDefaultRetryPolicy(), RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm =
        smm.<Integer>getRangeShardMap(ShardMapFaultHandlingTest.s_rangeShardMapName);

    assert rsm != null;

    // test undo operations on shard

    // global pre-local only create shard
    Shard stemp = rsm.createShard(new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapFaultHandlingTest.s_shardedDBs[0]));

    // now creating shard with GSM and LSM operations
    ssof.createAddShardOperationShardMapManagerIStoreShardMapIStoreShard = null;
    Shard s = rsm.createShard(new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapFaultHandlingTest.s_shardedDBs[0]));

    // global pre-local only update shard

    ShardUpdate tempVar = new ShardUpdate();
    tempVar.setStatus(ShardStatus.Offline);
    rsm.updateShard(s, tempVar);

    // now update shard with GSM and LSM operations
    ssof.createUpdateShardOperation4Param = null;
    ShardUpdate tempVar2 = new ShardUpdate();
    tempVar2.setStatus(ShardStatus.Offline);
    Shard shardNew = rsm.updateShard(s, tempVar2);

    // global pre-local only remove shard
    rsm.deleteShard(shardNew);

    // now remove with GSM and LSM operations
    ssof.createRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard = null;
    rsm.deleteShard(shardNew);

    // test undo operations for shard mapings

    Shard s1 = rsm.createShard(new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapFaultHandlingTest.s_shardedDBs[0]));

    assert s1 != null;

    Shard s2 = rsm.createShard(new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapFaultHandlingTest.s_shardedDBs[1]));

    assert s2 != null;

    // first add mapping will just execute global pre-local and add operation into pending
    // operations log
    RangeMapping rtemp = rsm.createRangeMapping(new Range(1, 10), s1);

    ssof.createAddMappingOperation4Param = null;

    // now add mapping will succeed after undoing pending operation

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s1);

    assert r1 != null;

    RangeMappingUpdate ru = new RangeMappingUpdate();
    ru.setStatus(MappingStatus.Offline);

    // below call will only execute global pre-local step to create operations log
    RangeMapping r2 = rsm.updateMapping(r1, ru);

    ssof.createUpdateMappingOperation7Param = null;

    // now update same mapping again, this will undo previous pending operation and then add this
    // mapping

    RangeMapping r3 = rsm.updateMapping(r1, ru);

    // try mapping update failures with change in shard location
    // first reset CreateUpdateMappingOperation to just perform global pre-local
    ssof.createUpdateMappingOperation7Param =
        (shardMapManager, operationCode, shardMap, mappingSource, mappingTarget,
            patternForKill, lockOwnerId) -> new UpdateMappingOperationFailAfterGlobalPreLocal(
            shardMapManager, operationCode, shardMap, mappingSource, mappingTarget,
            patternForKill, lockOwnerId);

    RangeMappingUpdate tempVar3 = new RangeMappingUpdate();
    tempVar3.setShard(s2);
    RangeMapping r4 = rsm.updateMapping(r3, tempVar3);

    // now try with actual update mapping operation
    ssof.createUpdateMappingOperation7Param = null;
    RangeMappingUpdate tempVar4 = new RangeMappingUpdate();
    tempVar4.setShard(s2);
    RangeMapping r5 = rsm.updateMapping(r3, tempVar4);

    // split mapping toperform gsm-only pre-local operation

    List<RangeMapping> rlisttemp = rsm.splitMapping(r5, 5);

    // try actual operation which will undo previous pending op
    ssof.createReplaceMappingsOperation5Param = null;

    List<RangeMapping> rlist = rsm.splitMapping(r5, 5);

    // remove mapping to create operations log and then exit
    rsm.deleteMapping(rlist.get(0));

    ssof.createRemoveMappingOperation5Param = null;

    // now actually remove the mapping
    rsm.deleteMapping(rlist.get(0));
  }

  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void shardMapOperationsFailureAfterLocalSource() {
    StubStoreOperationFactory ssof = new StubStoreOperationFactory();
    ssof.setCallBase(true);
    ssof.createAddShardOperationShardMapManagerIStoreShardMapIStoreShard =
        (smm, sm, s) -> new AddShardOperationFailAfterLocalSource(smm, sm, s);
    ssof.createRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard =
        (smm, sm, s) -> new RemoveShardOperationFailAfterLocalSource(smm, sm, s);
    ssof.createUpdateShardOperation4Param = (smm, sm, sold,
        shardNew) -> new UpdateShardOperationFailAfterLocalSource(smm, sm, sold, shardNew);
    ssof.createAddMappingOperation4Param = (smm, opCode, ssm,
        sm) -> new AddMappingOperationFailAfterLocalSource(smm, opCode, ssm, sm);
    ssof.createRemoveMappingOperation5Param =
        (smm, opCode, sm, mapping, loid) -> new RemoveMappingOperationFailAfterLocalSource(
            smm, opCode, sm, mapping, loid);
    ssof.createUpdateMappingOperation7Param =
        (shardMapManager, operationCode, shardMap, mappingSource, mappingTarget,
            patternForKill, lockOwnerId) -> new UpdateMappingOperationFailAfterLocalSource(
            shardMapManager, operationCode, shardMap, mappingSource, mappingTarget,
            patternForKill, lockOwnerId);
    ssof.createReplaceMappingsOperation5Param = (smm, opCode, sm, mappingSource,
        mappingTarget) -> new ReplaceMappingsOperationFailAfterLocalSource(smm, opCode, sm,
        mappingSource, mappingTarget);

    // TODO:new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero)
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), ssof, new CacheStore(), ShardMapManagerLoadPolicy.Lazy,
        RetryPolicy.getDefaultRetryPolicy(), RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm =
        smm.<Integer>getRangeShardMap(ShardMapFaultHandlingTest.s_rangeShardMapName);

    assert rsm != null;

    // test undo operations on shard

    // global pre-local only create shard
    Shard stemp = rsm.createShard(new ShardLocation(Globals.SHARD_MAP_MANAGER_CONN_STRING,
        ShardMapFaultHandlingTest.s_shardedDBs[0]));

    // now creating shard with GSM and LSM operations
    ssof.createAddShardOperationShardMapManagerIStoreShardMapIStoreShard = null;
    Shard s = rsm.createShard(new ShardLocation(Globals.SHARD_MAP_MANAGER_CONN_STRING,
        ShardMapFaultHandlingTest.s_shardedDBs[0]));

    // global pre-local only update shard

    ShardUpdate tempVar = new ShardUpdate();
    tempVar.setStatus(ShardStatus.Offline);
    rsm.updateShard(s, tempVar);

    // now update shard with GSM and LSM operations
    ssof.createUpdateShardOperation4Param = null;
    ShardUpdate tempVar2 = new ShardUpdate();
    tempVar2.setStatus(ShardStatus.Offline);
    Shard shardNew = rsm.updateShard(s, tempVar2);

    // global pre-local only remove shard
    rsm.deleteShard(shardNew);

    // now remove with GSM and LSM operations
    ssof.createRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard = null;
    rsm.deleteShard(shardNew);

    // test undo operations for shard mapings

    Shard s1 = rsm.createShard(new ShardLocation(Globals.SHARD_MAP_MANAGER_CONN_STRING,
        ShardMapFaultHandlingTest.s_shardedDBs[0]));

    assert s1 != null;

    Shard s2 = rsm.createShard(new ShardLocation(Globals.SHARD_MAP_MANAGER_CONN_STRING,
        ShardMapFaultHandlingTest.s_shardedDBs[1]));

    assert s2 != null;

    // first add mapping will just execute global pre-local and add operation into pending
    // operations log
    RangeMapping rtemp = rsm.createRangeMapping(new Range(1, 10), s1);

    ssof.createAddMappingOperation4Param = null;

    // now add mapping will succeed after undoing pending operation

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s1);

    assert r1 != null;

    RangeMappingUpdate ru = new RangeMappingUpdate();
    ru.setStatus(MappingStatus.Offline);

    // below call will only execute global pre-local step to create operations log
    RangeMapping r2 = rsm.updateMapping(r1, ru);

    ssof.createUpdateMappingOperation7Param = null;

    // now update same mapping again, this will undo previous pending operation and then add this
    // mapping

    RangeMapping r3 = rsm.updateMapping(r1, ru);

    // try mapping update failures with change in shard location
    // first reset CreateUpdateMappingOperation to just perform global pre-local
    ssof.createUpdateMappingOperation7Param =
        (shardMapManager, operationCode, shardMap, mappingSource, mappingTarget,
            patternForKill, lockOwnerId) -> new UpdateMappingOperationFailAfterLocalSource(
            shardMapManager, operationCode, shardMap, mappingSource, mappingTarget,
            patternForKill, lockOwnerId);

    RangeMappingUpdate tempVar3 = new RangeMappingUpdate();
    tempVar3.setShard(s2);
    RangeMapping r4 = rsm.updateMapping(r3, tempVar3);

    // now try with actual update mapping operation
    ssof.createUpdateMappingOperation7Param = null;
    RangeMappingUpdate tempVar4 = new RangeMappingUpdate();
    tempVar4.setShard(s2);
    RangeMapping r5 = rsm.updateMapping(r3, tempVar4);

    // split mapping toperform gsm-only pre-local operation

    List<RangeMapping> rlisttemp = rsm.splitMapping(r5, 5);

    // try actual operation which will undo previous pending op
    ssof.createReplaceMappingsOperation5Param = null;

    List<RangeMapping> rlist = rsm.splitMapping(r5, 5);

    // remove mapping to create operations log and then exit
    rsm.deleteMapping(rlist.get(0));

    ssof.createRemoveMappingOperation5Param = null;

    // now actually remove the mapping
    rsm.deleteMapping(rlist.get(0));
  }

  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void shardMapOperationsFailureAfterLocalTarget() {
    StubStoreOperationFactory ssof = new StubStoreOperationFactory();
    ssof.setCallBase(true);
    ssof.createAddShardOperationShardMapManagerIStoreShardMapIStoreShard =
        (smm, sm, s) -> new AddShardOperationFailAfterLocalTarget(smm, sm, s);
    ssof.createRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard =
        (smm, sm, s) -> new RemoveShardOperationFailAfterLocalTarget(smm, sm, s);
    ssof.createUpdateShardOperation4Param = (smm, sm, sold,
        shardNew) -> new UpdateShardOperationFailAfterLocalTarget(smm, sm, sold, shardNew);
    ssof.createAddMappingOperation4Param = (smm, opCode, ssm,
        sm) -> new AddMappingOperationFailAfterLocalTarget(smm, opCode, ssm, sm);
    ssof.createRemoveMappingOperation5Param =
        (smm, opCode, sm, mapping, loid) -> new RemoveMappingOperationFailAfterLocalTarget(
            smm, opCode, sm, mapping, loid);
    ssof.createUpdateMappingOperation7Param =
        (shardMapManager, operationCode, shardMap, mappingSource, mappingTarget,
            patternForKill, lockOwnerId) -> new UpdateMappingOperationFailAfterLocalTarget(
            shardMapManager, operationCode, shardMap, mappingSource, mappingTarget,
            patternForKill, lockOwnerId);
    ssof.createReplaceMappingsOperation5Param = (smm, opCode, sm, mappingSource,
        mappingTarget) -> new ReplaceMappingsOperationFailAfterLocalTarget(smm, opCode, sm,
        mappingSource, mappingTarget);

    // TODO:new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero)
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), ssof, new CacheStore(), ShardMapManagerLoadPolicy.Lazy,
        RetryPolicy.getDefaultRetryPolicy(), RetryBehavior.getDefaultRetryBehavior());

    RangeShardMap<Integer> rsm =
        smm.<Integer>getRangeShardMap(ShardMapFaultHandlingTest.s_rangeShardMapName);

    assert rsm != null;

    // test undo operations on shard

    // global pre-local only create shard
    Shard stemp = rsm.createShard(new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapFaultHandlingTest.s_shardedDBs[0]));

    // now creating shard with GSM and LSM operations
    ssof.createAddShardOperationShardMapManagerIStoreShardMapIStoreShard = null;
    Shard s = rsm.createShard(new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapFaultHandlingTest.s_shardedDBs[0]));

    // global pre-local only update shard

    ShardUpdate tempVar = new ShardUpdate();
    tempVar.setStatus(ShardStatus.Offline);
    rsm.updateShard(s, tempVar);

    // now update shard with GSM and LSM operations
    ssof.createUpdateShardOperation4Param = null;
    ShardUpdate tempVar2 = new ShardUpdate();
    tempVar2.setStatus(ShardStatus.Offline);
    Shard shardNew = rsm.updateShard(s, tempVar2);

    // global pre-local only remove shard
    rsm.deleteShard(shardNew);

    // now remove with GSM and LSM operations
    ssof.createRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard = null;
    rsm.deleteShard(shardNew);

    // test undo operations for shard mapings

    Shard s1 = rsm.createShard(new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapFaultHandlingTest.s_shardedDBs[0]));

    assert s1 != null;

    Shard s2 = rsm.createShard(new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapFaultHandlingTest.s_shardedDBs[1]));

    assert s2 != null;

    // first add mapping will just execute global pre-local and add operation into pending
    // operations log
    RangeMapping rtemp = rsm.createRangeMapping(new Range(1, 10), s1);

    ssof.createAddMappingOperation4Param = null;

    // now add mapping will succeed after undoing pending operation

    RangeMapping r1 = rsm.createRangeMapping(new Range(1, 10), s1);

    assert r1 != null;

    RangeMappingUpdate ru = new RangeMappingUpdate();
    ru.setStatus(MappingStatus.Offline);

    // below call will only execute global pre-local step to create operations log
    RangeMapping r2 = rsm.updateMapping(r1, ru);

    ssof.createUpdateMappingOperation7Param = null;

    // now update same mapping again, this will undo previous pending operation and then add this
    // mapping

    RangeMapping r3 = rsm.updateMapping(r1, ru);

    // try mapping update failures with change in shard location
    // first reset CreateUpdateMappingOperation to just perform global pre-local
    ssof.createUpdateMappingOperation7Param =
        (shardMapManager, operationCode, shardMap, mappingSource, mappingTarget,
            patternForKill, lockOwnerId) -> new UpdateMappingOperationFailAfterLocalTarget(
            shardMapManager, operationCode, shardMap, mappingSource, mappingTarget,
            patternForKill, lockOwnerId);

    RangeMappingUpdate tempVar3 = new RangeMappingUpdate();
    tempVar3.setShard(s2);
    RangeMapping r4 = rsm.updateMapping(r3, tempVar3);

    // now try with actual update mapping operation
    ssof.createUpdateMappingOperation7Param = null;
    RangeMappingUpdate tempVar4 = new RangeMappingUpdate();
    tempVar4.setShard(s2);
    RangeMapping r5 = rsm.updateMapping(r3, tempVar4);

    // split mapping toperform gsm-only pre-local operation

    List<RangeMapping> rlisttemp = rsm.splitMapping(r5, 5);

    // try actual operation which will undo previous pending op
    ssof.createReplaceMappingsOperation5Param = null;

    List<RangeMapping> rlist = rsm.splitMapping(r5, 5);

    // remove mapping to create operations log and then exit
    rsm.deleteMapping(rlist.get(0));

    ssof.createRemoveMappingOperation5Param = null;

    // now actually remove the mapping
    rsm.deleteMapping(rlist.get(0));
  }

  private class NTimeFailingAddMappingOperation extends AddMappingOperation {

    private int failureCountMax;
    private int currentFailureCount;

    public NTimeFailingAddMappingOperation(int failureCountMax, ShardMapManager shardMapManager,
        StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping mapping) {
      super(shardMapManager, operationCode, shardMap, mapping);
      this.failureCountMax = failureCountMax;
      currentFailureCount = 0;
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      if (currentFailureCount < failureCountMax) {
        currentFailureCount++;
        throw new StoreException("", TransientSqlException);
      } else {
        return super.doGlobalPostLocalExecute(ts);
      }
    }
  }

  private class AddShardOperationFailAfterGlobalPreLocal extends AddShardOperation {

    public AddShardOperationFailAfterGlobalPreLocal(ShardMapManager shardMapManager,
        StoreShardMap shardMap, StoreShard shard) {
      super(shardMapManager, shardMap, shard);
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }

    @Override
    public StoreResults doLocalSourceExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }

    @Override
    public StoreResults doLocalTargetExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }
  }

  private class RemoveShardOperationFailAfterGlobalPreLocal extends RemoveShardOperation {

    public RemoveShardOperationFailAfterGlobalPreLocal(ShardMapManager shardMapManager,
        StoreShardMap shardMap, StoreShard shard) {
      super(shardMapManager, shardMap, shard);
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }

    @Override
    public StoreResults doLocalSourceExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }

    @Override
    public StoreResults doLocalTargetExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }
  }

  private class UpdateShardOperationFailAfterGlobalPreLocal extends UpdateShardOperation {

    public UpdateShardOperationFailAfterGlobalPreLocal(ShardMapManager shardMapManager,
        StoreShardMap shardMap, StoreShard shardOld, StoreShard shardNew) {
      super(shardMapManager, shardMap, shardOld, shardNew);
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }

    @Override
    public StoreResults doLocalSourceExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }

    @Override
    public StoreResults doLocalTargetExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }
  }

  private class AddMappingOperationFailAfterGlobalPreLocal extends AddMappingOperation {

    public AddMappingOperationFailAfterGlobalPreLocal(ShardMapManager shardMapManager,
        StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping mapping) {
      super(shardMapManager, operationCode, shardMap, mapping);
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }

    @Override
    public StoreResults doLocalSourceExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }

    @Override
    public StoreResults doLocalTargetExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }
  }

  private class RemoveMappingOperationFailAfterGlobalPreLocal extends RemoveMappingOperation {

    public RemoveMappingOperationFailAfterGlobalPreLocal(ShardMapManager shardMapManager,
        StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping mapping,
        UUID lockOwnerId) {
      super(shardMapManager, operationCode, shardMap, mapping, lockOwnerId);
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }

    @Override
    public StoreResults doLocalSourceExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }

    @Override
    public StoreResults doLocalTargetExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }
  }

  private class UpdateMappingOperationFailAfterGlobalPreLocal extends UpdateMappingOperation {

    public UpdateMappingOperationFailAfterGlobalPreLocal(ShardMapManager shardMapManager,
        StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping mappingSource,
        StoreMapping mappingTarget, String patternForKill, UUID lockOwnerId) {
      super(shardMapManager, operationCode, shardMap, mappingSource, mappingTarget, patternForKill,
          lockOwnerId);
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }

    @Override
    public StoreResults doLocalSourceExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }

    @Override
    public StoreResults doLocalTargetExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }
  }

  private class ReplaceMappingsOperationFailAfterGlobalPreLocal extends ReplaceMappingsOperation {

    public ReplaceMappingsOperationFailAfterGlobalPreLocal(ShardMapManager shardMapManager,
        StoreOperationCode operationCode, StoreShardMap shardMap,
        List<Pair<StoreMapping, UUID>> mappingsSource,
        List<Pair<StoreMapping, UUID>> mappingsTarget) {
      super(shardMapManager, operationCode, shardMap, mappingsSource, mappingsTarget);
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }

    @Override
    public StoreResults doLocalSourceExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }

    @Override
    public StoreResults doLocalTargetExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }
  }

  private class AddShardOperationFailAfterLocalSource extends AddShardOperation {

    public AddShardOperationFailAfterLocalSource(ShardMapManager shardMapManager,
        StoreShardMap shardMap, StoreShard shard) {
      super(shardMapManager, shardMap, shard);
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }

    @Override
    public StoreResults doLocalTargetExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }
  }

  private class RemoveShardOperationFailAfterLocalSource extends RemoveShardOperation {

    public RemoveShardOperationFailAfterLocalSource(ShardMapManager shardMapManager,
        StoreShardMap shardMap, StoreShard shard) {
      super(shardMapManager, shardMap, shard);
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }

    @Override
    public StoreResults doLocalTargetExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }
  }

  private class UpdateShardOperationFailAfterLocalSource extends UpdateShardOperation {

    public UpdateShardOperationFailAfterLocalSource(ShardMapManager shardMapManager,
        StoreShardMap shardMap, StoreShard shardOld, StoreShard shardNew) {
      super(shardMapManager, shardMap, shardOld, shardNew);
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }

    @Override
    public StoreResults doLocalTargetExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }
  }

  private class AddMappingOperationFailAfterLocalSource extends AddMappingOperation {

    public AddMappingOperationFailAfterLocalSource(ShardMapManager shardMapManager,
        StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping mapping) {
      super(shardMapManager, operationCode, shardMap, mapping);
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }

    @Override
    public StoreResults doLocalTargetExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }
  }

  private class RemoveMappingOperationFailAfterLocalSource extends RemoveMappingOperation {

    public RemoveMappingOperationFailAfterLocalSource(ShardMapManager shardMapManager,
        StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping mapping,
        UUID lockOwnerId) {
      super(shardMapManager, operationCode, shardMap, mapping, lockOwnerId);
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }

    @Override
    public StoreResults doLocalTargetExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }
  }

  private class UpdateMappingOperationFailAfterLocalSource extends UpdateMappingOperation {

    public UpdateMappingOperationFailAfterLocalSource(ShardMapManager shardMapManager,
        StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping mappingSource,
        StoreMapping mappingTarget, String patternForKill, UUID lockOwnerId) {
      super(shardMapManager, operationCode, shardMap, mappingSource, mappingTarget, patternForKill,
          lockOwnerId);
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }

    @Override
    public StoreResults doLocalTargetExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }
  }

  private class ReplaceMappingsOperationFailAfterLocalSource extends ReplaceMappingsOperation {

    public ReplaceMappingsOperationFailAfterLocalSource(ShardMapManager shardMapManager,
        StoreOperationCode operationCode, StoreShardMap shardMap,
        List<Pair<StoreMapping, UUID>> mappingsSource,
        List<Pair<StoreMapping, UUID>> mappingsTarget) {
      super(shardMapManager, operationCode, shardMap, mappingsSource, mappingsTarget);
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }

    @Override
    public StoreResults doLocalTargetExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }
  }

  private class AddShardOperationFailAfterLocalTarget extends AddShardOperation {

    public AddShardOperationFailAfterLocalTarget(ShardMapManager shardMapManager,
        StoreShardMap shardMap, StoreShard shard) {
      super(shardMapManager, shardMap, shard);
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }
  }

  private class RemoveShardOperationFailAfterLocalTarget extends RemoveShardOperation {

    public RemoveShardOperationFailAfterLocalTarget(ShardMapManager shardMapManager,
        StoreShardMap shardMap, StoreShard shard) {
      super(shardMapManager, shardMap, shard);
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }
  }

  private class UpdateShardOperationFailAfterLocalTarget extends UpdateShardOperation {

    public UpdateShardOperationFailAfterLocalTarget(ShardMapManager shardMapManager,
        StoreShardMap shardMap, StoreShard shardOld, StoreShard shardNew) {
      super(shardMapManager, shardMap, shardOld, shardNew);
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }
  }

  private class AddMappingOperationFailAfterLocalTarget extends AddMappingOperation {

    public AddMappingOperationFailAfterLocalTarget(ShardMapManager shardMapManager,
        StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping mapping) {
      super(shardMapManager, operationCode, shardMap, mapping);
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }
  }

  private class RemoveMappingOperationFailAfterLocalTarget extends RemoveMappingOperation {

    public RemoveMappingOperationFailAfterLocalTarget(ShardMapManager shardMapManager,
        StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping mapping,
        UUID lockOwnerId) {
      super(shardMapManager, operationCode, shardMap, mapping, lockOwnerId);
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }
  }

  private class UpdateMappingOperationFailAfterLocalTarget extends UpdateMappingOperation {

    public UpdateMappingOperationFailAfterLocalTarget(ShardMapManager shardMapManager,
        StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping mappingSource,
        StoreMapping mappingTarget, String patternForKill, UUID lockOwnerId) {
      super(shardMapManager, operationCode, shardMap, mappingSource, mappingTarget, patternForKill,
          lockOwnerId);
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }
  }

  private class ReplaceMappingsOperationFailAfterLocalTarget extends ReplaceMappingsOperation {

    public ReplaceMappingsOperationFailAfterLocalTarget(ShardMapManager shardMapManager,
        StoreOperationCode operationCode, StoreShardMap shardMap,
        List<Pair<StoreMapping, UUID>> mappingsSource,
        List<Pair<StoreMapping, UUID>> mappingsTarget) {
      super(shardMapManager, operationCode, shardMap, mappingsSource, mappingsTarget);
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      StoreResults results = new StoreResults();

      return results;
    }
  }
}
