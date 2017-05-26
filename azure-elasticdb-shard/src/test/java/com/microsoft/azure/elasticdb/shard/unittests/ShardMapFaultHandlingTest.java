package com.microsoft.azure.elasticdb.shard.unittests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryBehavior;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.shard.base.PointMapping;
import com.microsoft.azure.elasticdb.shard.base.RangeMapping;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
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
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationCode;
import com.microsoft.azure.elasticdb.shard.storeops.mapper.AddMappingOperation;
import com.microsoft.azure.elasticdb.shard.stubs.StubStoreOperationFactory;

public class ShardMapFaultHandlingTest {

  public static SQLException TransientSqlException = ShardMapFaultHandlingTest.createSqlException();

  /**
   * Sharded databases to create for the test.
   */
  private static String[] s_shardedDBs = new String[] {"shard1", "shard2"};

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
   * 
   * @param testContext The TestContext we are running in.
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
  Cleans up common state for the all tests in this class.
*/
  @AfterClass
 public static void ShardMapFaultHandlingTestsCleanup()
 {

     try (Connection conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING))
     {
         // Drop shard databases
         for (int i = 0; i < ShardMapFaultHandlingTest.s_shardedDBs.length; i++)
         {
             try (Statement stmt = conn.createStatement())
             {
               String query = String.format(Globals.DROP_DATABASE_QUERY, ShardMapFaultHandlingTest.s_shardedDBs[i]);
               stmt.executeQuery(query);
             }
         }

         // Drop shard map manager database
         try (Statement stmt = conn.createStatement())
         {
           String query = String.format(Globals.DROP_DATABASE_QUERY, Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
           stmt.executeQuery(query);
         }
     } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
 }
  
  /** 
  Initializes common state per-test.
*/
  @Before
 public final void ShardMapperTestInitialize()
 {
     ShardMapFaultHandlingTest.cleanShardMapsHelper();
 }

 /** 
  Cleans up common state per-test.
 */
@After
 public void ShardMapperTestCleanup()
 {
     ShardMapFaultHandlingTest.cleanShardMapsHelper();
 }

 ///#endregion Common Methods

 private class NTimeFailingAddMappingOperation extends AddMappingOperation
 {
     private int _failureCountMax;
     private int _currentFailureCount;

     public NTimeFailingAddMappingOperation(int failureCountMax, ShardMapManager shardMapManager, StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping mapping)
     {
         super(shardMapManager, operationCode, shardMap, mapping);
         _failureCountMax = failureCountMax;
         _currentFailureCount = 0;
     }

     @Override
     public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts)
     {
         if (_currentFailureCount < _failureCountMax)
         {
             _currentFailureCount++;
             throw new StoreException("", TransientSqlException);
         }
         else
         {
             return super.doGlobalPostLocalExecute(ts);
         }
     }
 }

 @Test
 @Category(value = ExcludeFromGatedCheckin.class)
  public void addPointMappingFailGSMAfterSuccessLSMSingleRetry() {
    StubStoreOperationFactory stubStoreOperationFactory = new StubStoreOperationFactory();
    stubStoreOperationFactory.setCallBase(true);
    stubStoreOperationFactory.createAddMappingOperation4Param = (_smm, _opcode, _ssm,
        _sm) -> new NTimeFailingAddMappingOperation(1, _smm, _opcode, _ssm, _sm);

    // new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero)
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), stubStoreOperationFactory, new CacheStore(),
        ShardMapManagerLoadPolicy.Lazy, RetryPolicy.DefaultRetryPolicy,
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
  public void addPointMappingFailGSMAfterSuccessLSM() {
    StubStoreOperationFactory ssof = new StubStoreOperationFactory();
    ssof.setCallBase(true);
    ssof.createAddMappingOperation4Param = (_smm, _opcode, _ssm,
        _sm) -> new NTimeFailingAddMappingOperation(2, _smm, _opcode, _ssm, _sm);

    // new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero)
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), ssof, new CacheStore(), ShardMapManagerLoadPolicy.Lazy,
        RetryPolicy.DefaultRetryPolicy, RetryBehavior.getDefaultRetryBehavior());

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
}
