package com.microsoft.azure.elasticdb.shard.unittests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryBehavior;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardCreationInfo;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.ShardStatus;
import com.microsoft.azure.elasticdb.shard.base.ShardUpdate;
import com.microsoft.azure.elasticdb.shard.cache.CacheStore;
import com.microsoft.azure.elasticdb.shard.category.ExcludeFromGatedCheckin;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
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
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreException;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreShard;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationFactory;
import com.microsoft.azure.elasticdb.shard.storeops.map.AddShardOperation;
import com.microsoft.azure.elasticdb.shard.storeops.map.RemoveShardOperation;
import com.microsoft.azure.elasticdb.shard.storeops.map.UpdateShardOperation;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func1Param;
import com.microsoft.azure.elasticdb.shard.stubs.StubAddShardOperation;
import com.microsoft.azure.elasticdb.shard.stubs.StubRemoveShardOperation;
import com.microsoft.azure.elasticdb.shard.stubs.StubStoreOperationFactory;
import com.microsoft.azure.elasticdb.shard.stubs.StubUpdateShardOperation;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test related to ShardMap class and it's methods.
 */
public class ShardMapTest {

  /**
   * Sharded databases to create for the test.
   */
  private static String[] shardDbs = new String[]{"shard1", "shard2"};

  /**
   * Default shard map name.
   */
  private static String defaultShardMapName = "CustomersDefault";

  /**
   * Helper function to clean default shard map.
   */
  private static void cleanShardMapsHelper() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    // Remove all existing mappings from the list shard map.
    ShardMap sm = smm.getShardMap(ShardMapTest.defaultShardMapName);
    assertNotNull(sm);

    // Remove all shards from list shard map
    Iterable<Shard> s = sm.getShards();
    try {
      for (Shard value : s) {
        sm.deleteShard(value);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  /**
   * Initializes common state for tests in this class.
   */
  @BeforeClass
  public static void shardMapTestsInitialize() throws SQLException {
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
      for (int i = 0; i < ShardMapTest.shardDbs.length; i++) {
        try (Statement stmt = conn.createStatement()) {
          String query = String.format(Globals.DROP_DATABASE_QUERY, ShardMapTest.shardDbs[i]);
          stmt.executeUpdate(query);
        } catch (SQLException ex) {
          ex.printStackTrace();
        }
        try (Statement stmt = conn.createStatement()) {
          String query = String.format(Globals.CREATE_DATABASE_QUERY, ShardMapTest.shardDbs[i]);
          stmt.executeUpdate(query);
        } catch (SQLException ex) {
          ex.printStackTrace();
        }
      }

      // Create shard map manager.
      ShardMapManagerFactory.createSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING,
          ShardMapManagerCreateMode.ReplaceExisting);

      // Create default shard map.
      ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

      ShardMap sm = smm.createListShardMap(ShardMapTest.defaultShardMapName, ShardKeyType.Int32);
      assertNotNull(sm);
      assertEquals(ShardMapTest.defaultShardMapName, sm.getName());

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
  public static void shardMapTestsCleanup() throws SQLException {
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

      // Drop shard databases
      for (int i = 0; i < ShardMapTest.shardDbs.length; i++) {
        try (Statement stmt = conn.createStatement()) {
          String query = String.format(Globals.DROP_DATABASE_QUERY, ShardMapTest.shardDbs[i]);
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
  public void shardMapTestInitialize() {
    ShardMapTest.cleanShardMapsHelper();
  }

  /**
   * Cleans up common state per-test.
   */
  @After
  public void shardMapTestCleanup() {
    ShardMapTest.cleanShardMapsHelper();
  }

  /**
   * Add a shard to shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void createShardDefault() throws SQLException {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ShardMap sm = smm.getShardMap(ShardMapTest.defaultShardMapName);
    assertNotNull(sm);

    // TODO: shardlocation with sqlprotocol and port name provided
    ShardLocation s1 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTest.shardDbs[0]);

    Shard sNew = sm.createShard(s1);

    assertNotNull(sNew);

    assertEquals(s1.toString(), sNew.getLocation().toString());
    assertEquals(s1.toString(), sm.getShard(s1).getLocation().toString());

    try (Connection conn =
        sNew.openConnection(Globals.SHARD_USER_CONN_STRING, ConnectionOptions.Validate)) {
      // TODO?
      conn.close();
    }
  }

  /**
   * Add a duplicate shard to shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void createShardDuplicate() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ShardMap sm = smm.getShardMap(ShardMapTest.defaultShardMapName);

    assertNotNull(sm);

    ShardLocation s1 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTest.shardDbs[0]);

    Shard sNew = sm.createShard(s1);

    assertNotNull(sNew);

    boolean addFailed = false;

    try {
      sm.createShard(s1);
    } catch (ShardManagementException sme) {
      assertEquals(ShardManagementErrorCategory.ShardMap, sme.getErrorCategory());
      assertEquals(ShardManagementErrorCode.ShardLocationAlreadyExists, sme.getErrorCode());
      addFailed = true;
    }
    assertTrue(addFailed);
  }

  /**
   * Add a shard with null location to shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void createShardNullLocation() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ShardMap sm = smm.getShardMap(ShardMapTest.defaultShardMapName);
    assertNotNull(sm);

    boolean addFailed = false;

    try {
      new ShardLocation("", "");
    } catch (IllegalArgumentException ex) {
      addFailed = true;
    }

    assertTrue(addFailed);
  }

  /**
   * Remove existing shard from shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void deleteShardDefault() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);
    ShardMap sm = smm.getShardMap(ShardMapTest.defaultShardMapName);
    assertNotNull(sm);

    ShardLocation s1 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTest.shardDbs[0]);

    Shard sNew = sm.createShard(s1);

    assertNotNull(sNew);

    sm.deleteShard(sNew);

    ReferenceObjectHelper<Shard> refShard = new ReferenceObjectHelper<>(sNew);
    sm.tryGetShard(s1, refShard);
    assertNull(refShard.argValue);
  }

  /**
   * Remove an already removed shard from shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void deleteShardDuplicate() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ShardMap sm = smm.getShardMap(ShardMapTest.defaultShardMapName);
    assertNotNull(sm);

    ShardLocation s1 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTest.shardDbs[0]);

    Shard sNew = sm.createShard(s1);

    assertNotNull(sNew);

    sm.deleteShard(sNew);

    boolean removeFailed = false;

    try {
      sm.deleteShard(sNew);
    } catch (ShardManagementException sme) {
      assertEquals(ShardManagementErrorCategory.ShardMap, sme.getErrorCategory());
      assertEquals(ShardManagementErrorCode.ShardDoesNotExist, sme.getErrorCode());
      removeFailed = true;
    }
    assertTrue(removeFailed);

  }

  /**
   * Remove a shard with shard version mismatch.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void deleteShardVersionMismatch() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ShardMap sm = smm.getShardMap(ShardMapTest.defaultShardMapName);
    assertNotNull(sm);

    ShardLocation s1 =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, Globals.SHARD_MAP_MANAGER_DATABASE_NAME);

    Shard sNew = sm.createShard(s1);

    // Update shard to increment version

    ShardUpdate su = new ShardUpdate();
    su.setStatus(ShardStatus.Offline);

    sm.updateShard(sNew, su);

    boolean removeFailed = false;

    try {
      sm.deleteShard(sNew);
    } catch (ShardManagementException sme) {
      assertEquals(ShardManagementErrorCategory.ShardMap, sme.getErrorCategory());
      assertEquals(ShardManagementErrorCode.ShardVersionMismatch, sme.getErrorCode());
      removeFailed = true;
    }

    assertTrue(removeFailed);
  }

  /**
   * Update shard.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void updateShard() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ShardMap sm = smm.getShardMap(ShardMapTest.defaultShardMapName);
    assertNotNull(sm);

    ShardLocation s1 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTest.shardDbs[0]);

    Shard sNew = sm.createShard(new ShardCreationInfo(s1, ShardStatus.Online));

    ShardUpdate su = new ShardUpdate();
    su.setStatus(ShardStatus.Offline);

    Shard sUpdated = sm.updateShard(sNew, su);
    assertNotNull(sNew);
    assertNotNull(sUpdated);
  }

  /**
   * Update shard with version mismatch.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void updateShardVersionMismatch() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ShardMap sm = smm.getShardMap(ShardMapTest.defaultShardMapName);
    assertNotNull(sm);

    ShardLocation s1 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTest.shardDbs[0]);

    Shard sNew = sm.createShard(new ShardCreationInfo(s1, ShardStatus.Online));

    ShardUpdate su = new ShardUpdate();
    su.setStatus(ShardStatus.Offline);

    Shard sUpdated = sm.updateShard(sNew, su);
    assertNotNull(sNew);
    assertNotNull(sUpdated);

    boolean updateFailed = false;

    try {
      sm.updateShard(sNew, su);
    } catch (ShardManagementException sme) {
      assertEquals(ShardManagementErrorCategory.ShardMap, sme.getErrorCategory());
      assertEquals(ShardManagementErrorCode.ShardVersionMismatch, sme.getErrorCode());
      updateFailed = true;
    }
    assertTrue(updateFailed);
  }

  /**
   * Validate shard.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void validateShard() throws SQLException {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ShardMap sm = smm.getShardMap(ShardMapTest.defaultShardMapName);
    assertNotNull(sm);

    ShardLocation s1 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTest.shardDbs[0]);

    Shard sNew = sm.createShard(new ShardCreationInfo(s1, ShardStatus.Online));

    ShardUpdate su = new ShardUpdate();
    su.setStatus(ShardStatus.Offline);

    Shard sUpdated = sm.updateShard(sNew, su);
    assertNotNull(sUpdated);

    boolean validationFailed = false;

    try (Connection conn =
        sNew.openConnection(Globals.SHARD_USER_CONN_STRING, ConnectionOptions.Validate)) {
      conn.close();
    } catch (ShardManagementException sme) {
      validationFailed = true;
      assertEquals(ShardManagementErrorCategory.Validation, sme.getErrorCategory());
      assertEquals(ShardManagementErrorCode.ShardVersionMismatch, sme.getErrorCode());
    }

    assertTrue(validationFailed);

    validationFailed = false;

    try (Connection conn =
        sUpdated.openConnection(Globals.SHARD_USER_CONN_STRING, ConnectionOptions.Validate)) {
      conn.close();
    } catch (ShardManagementException ex) {
      validationFailed = true;
    }

    assertFalse(validationFailed);
  }

  /**
   * Add a shard to shard map, abort transaction in GSM.
   */
  /*@Test
  @Category(value = ExcludeFromGatedCheckin.class)*/
  public void createShardAbortGSM() {
    int retryCount = 0;

    // TODO EventHandler<RetryingEventArgs> eventHandler = (sender, arg) ->
    // {
    // retryCount++;
    // };

    StubStoreOperationFactory stubStoreOperationFactory = new StubStoreOperationFactory();
    stubStoreOperationFactory.setCallBase(true);
    stubStoreOperationFactory.CreateAddShardOperationShardMapManagerIStoreShardMapIStoreShard =
        (_smm, _sm, _s) -> new NTimeFailingAddShardOperation(10, _smm, _sm, _s);

    // TODO:new RetryPolicy(5, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero)
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), stubStoreOperationFactory, new CacheStore(),
        ShardMapManagerLoadPolicy.Lazy, RetryPolicy.DefaultRetryPolicy,
        RetryBehavior.getDefaultRetryBehavior());

    ShardMap sm = smm.getShardMap(ShardMapTest.defaultShardMapName);

    assert sm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTest.shardDbs[0]);

    boolean storeOperationFailed = false;

    // TODO:smm.ShardMapManagerRetrying += eventHandler;

    try {
      Shard sNew = sm.createShard(sl);
      assert sNew != null;
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ShardMap == sme.getErrorCategory();
      assert ShardManagementErrorCode.StorageOperationFailure == sme.getErrorCode();
      storeOperationFailed = true;
    }

    // TODO:smm.ShardMapManagerRetrying -= eventHandler;

    assert 5 == retryCount;

    assert storeOperationFailed;

    // verify that shard map does not have any shards.
    int count = 0;
    List<Shard> sList = sm.getShards();

    Iterator<Shard> sEnum = sList.iterator();
    while (sEnum.hasNext()) {
      count++;
    }
    assert 0 == count;
  }

  /**
   * Add a shard to shard map, abort transaction in GSM Do and GSM Undo.
   */
  /*@Test
  @Category(value = ExcludeFromGatedCheckin.class)*/
  public void createShardAbortGSMDoAndLSMUndo() {
    final boolean shouldThrow = true;

    StubStoreOperationFactory stubOperationFactory = new StubStoreOperationFactory();
    stubOperationFactory.setCallBase(true);
    stubOperationFactory.CreateAddShardOperationShardMapManagerIStoreShardMapIStoreShard =
        (_smm, _sm, _s) -> {
          StubAddShardOperation op = new StubAddShardOperation(_smm, _sm, _s);
          op.setCallBase(true);
          op.DoGlobalPostLocalExecuteIStoreTransactionScope = (ts) -> {
            if (shouldThrow) {
              throw new StoreException("", ShardMapFaultHandlingTest.TransientSqlException);
            } else {
              Object original = op.DoGlobalPostLocalExecuteIStoreTransactionScope;
              op.DoGlobalPostLocalExecuteIStoreTransactionScope = null;
              try {
                return op.doGlobalPostLocalExecute(ts);
              } finally {
                op.DoGlobalPostLocalExecuteIStoreTransactionScope =
                    (Func1Param<IStoreTransactionScope, StoreResults>) original;
              }
            }
          };
          op.UndoLocalSourceExecuteIStoreTransactionScope = (ts) -> {
            if (shouldThrow) {
              throw new StoreException("", ShardMapFaultHandlingTest.TransientSqlException);
            } else {
              Object original = op.UndoLocalSourceExecuteIStoreTransactionScope;
              op.UndoLocalSourceExecuteIStoreTransactionScope = null;
              try {
                return op.undoLocalSourceExecute(ts);
              } finally {
                op.UndoLocalSourceExecuteIStoreTransactionScope =
                    (Func1Param<IStoreTransactionScope, StoreResults>) original;
              }
            }
          };
          return op;
        };
    IStoreOperationFactory sof = stubOperationFactory;

    // TODO:new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero)
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), sof, new CacheStore(), ShardMapManagerLoadPolicy.Lazy,
        RetryPolicy.DefaultRetryPolicy, RetryBehavior.getDefaultRetryBehavior());

    ShardMap sm = smm.getShardMap(ShardMapTest.defaultShardMapName);

    assert sm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTest.shardDbs[0]);

    boolean storeOperationFailed = false;
    try {
      Shard sNew = sm.createShard(sl);
      assert sNew != null;
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ShardMap == sme.getErrorCategory();
      assert ShardManagementErrorCode.StorageOperationFailure == sme.getErrorCode();
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // Obtain the pending operations.
    // TODO
    // var pendingOperations = ShardMapperTest.GetPendingStoreOperations();
    // assert pendingOperations.Count() == 1;

    // verify that shard map does not have any shards.
    assert 0 == sm.getShards().size();

    // TODO:shouldThrow = false;
    storeOperationFailed = false;
    try {
      Shard sNew = sm.createShard(sl);
      assert sNew != null;
    } catch (ShardManagementException e) {
      storeOperationFailed = true;
    }

    assert !storeOperationFailed;
    assert 1 == sm.getShards().size();
  }

  /*@Test
  @Category(value = ExcludeFromGatedCheckin.class)*/
  public void deleteShardAbortGSM() {
    StubStoreOperationFactory stubOperationFactory = new StubStoreOperationFactory();
    stubOperationFactory.setCallBase(true);
    stubOperationFactory.CreateRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard =
        (_smm, _sm, _s) -> new NTimeFailingRemoveShardOperation(10, _smm, _sm, _s);

    // TODO: new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero)
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), stubOperationFactory, new CacheStore(),
        ShardMapManagerLoadPolicy.Lazy, RetryPolicy.DefaultRetryPolicy,
        RetryBehavior.getDefaultRetryBehavior());

    ShardMap sm = smm.getShardMap(ShardMapTest.defaultShardMapName);
    assert sm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTest.shardDbs[0]);

    Shard sNew = sm.createShard(sl);

    assert sNew != null;

    boolean storeOperationFailed = false;
    try {
      sm.deleteShard(sNew);
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ShardMap == sme.getErrorCategory();
      assert ShardManagementErrorCode.StorageOperationFailure == sme.getErrorCode();
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // verify that the shard exists in store.
    Shard sValidate = sm.getShard(sl);
    assert sValidate != null;
  }

  /*@Test
  @Category(value = ExcludeFromGatedCheckin.class)*/
  public void deleteShardAbortGSMDoAndLSMUndo() {
    final boolean shouldThrow = true;

    StubStoreOperationFactory stubStoreOperationFactory = new StubStoreOperationFactory();
    stubStoreOperationFactory.setCallBase(true);
    stubStoreOperationFactory.CreateRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard =
        (_smm, _sm, _s) -> {
          StubRemoveShardOperation op = new StubRemoveShardOperation(_smm, _sm, _s);
          op.setCallBase(true);
          op.DoGlobalPostLocalExecuteIStoreTransactionScope = (ts) -> {
            if (shouldThrow) {
              throw new StoreException("", ShardMapFaultHandlingTest.TransientSqlException);
            } else {
              Object original = op.DoGlobalPostLocalExecuteIStoreTransactionScope;
              op.DoGlobalPostLocalExecuteIStoreTransactionScope = null;
              try {
                return op.doGlobalPostLocalExecute(ts);
              } finally {
                op.DoGlobalPostLocalExecuteIStoreTransactionScope =
                    (Func1Param<IStoreTransactionScope, StoreResults>) original;
              }
            }
          };
          op.UndoLocalSourceExecuteIStoreTransactionScope = (ts) -> {
            if (shouldThrow) {
              throw new StoreException("", ShardMapFaultHandlingTest.TransientSqlException);
            } else {
              Object original = op.UndoLocalSourceExecuteIStoreTransactionScope;
              op.UndoLocalSourceExecuteIStoreTransactionScope = null;
              try {
                return op.undoLocalSourceExecute(ts);
              } finally {
                op.UndoLocalSourceExecuteIStoreTransactionScope =
                    (Func1Param<IStoreTransactionScope, StoreResults>) original;
              }
            }
          };
          return op;
        };
    IStoreOperationFactory sof = stubStoreOperationFactory;

    // TODO:new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero)
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), sof, new CacheStore(), ShardMapManagerLoadPolicy.Lazy,
        RetryPolicy.DefaultRetryPolicy, RetryBehavior.getDefaultRetryBehavior());

    ShardMap sm = smm.getShardMap(ShardMapTest.defaultShardMapName);
    assert sm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTest.shardDbs[0]);

    Shard sNew = sm.createShard(sl);

    assert sNew != null;

    boolean storeOperationFailed = false;
    try {
      sm.deleteShard(sNew);
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ShardMap == sme.getErrorCategory();
      assert ShardManagementErrorCode.StorageOperationFailure == sme.getErrorCode();
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // verify that the shard exists in store.
    Shard sValidate = sm.getShard(sl);
    assert sValidate != null;

    // Obtain the pending operations.
    // C# TO JAVA CONVERTER TODO TASK: There is no equivalent to implicit typing in Java:
    // TODO Object pendingOperations = ShardMapperTest.GetPendingStoreOperations();
    // assert pendingOperations.Count() == 1;

    // TODO:shouldThrow = false;
    storeOperationFailed = false;
    try {
      sm.deleteShard(sNew);
    } catch (ShardManagementException e) {
      storeOperationFailed = true;
    }

    assert !storeOperationFailed;
    assert 0 == sm.getShards().size();
  }

  /*@Test
  @Category(value = ExcludeFromGatedCheckin.class)*/
  public void updateShardAbortGSM() {
    StubStoreOperationFactory stubStoreOperationFactory = new StubStoreOperationFactory();
    stubStoreOperationFactory.setCallBase(true);
    stubStoreOperationFactory.CreateUpdateShardOperationShardMapManagerIStoreShardMapIStoreShardIStoreShard =
        (_smm, _sm, _so, _sn) -> new NTimeFailingUpdateShardOperation(10, _smm, _sm, _so, _sn);

    // TODO: new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero)
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), stubStoreOperationFactory, new CacheStore(),
        ShardMapManagerLoadPolicy.Lazy, RetryPolicy.DefaultRetryPolicy,
        RetryBehavior.getDefaultRetryBehavior());
    ShardMap sm = smm.getShardMap(ShardMapTest.defaultShardMapName);
    assert sm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTest.shardDbs[0]);

    Shard sNew = sm.createShard(new ShardCreationInfo(sl, ShardStatus.Online));

    ShardUpdate su = new ShardUpdate();
    su.setStatus(ShardStatus.Offline);

    boolean storeOperationFailed = false;
    try {
      Shard sUpdated = sm.updateShard(sNew, su);
      assert sNew != null;
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ShardMap == sme.getErrorCategory();
      assert ShardManagementErrorCode.StorageOperationFailure == sme.getErrorCode();
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // verify that shard status is not changed.
    Shard sValidate = sm.getShard(sl);
    assert sNew.getStatus() == sValidate.getStatus();
  }

  /*@Test
  @Category(value = ExcludeFromGatedCheckin.class)*/
  public void updateShardAbortGSMDoAndLSMUndo() {
    boolean shouldThrow = true;

    StubStoreOperationFactory stubStoreOperationFactory = new StubStoreOperationFactory();
    stubStoreOperationFactory.setCallBase(true);
    stubStoreOperationFactory.CreateUpdateShardOperationShardMapManagerIStoreShardMapIStoreShardIStoreShard =
        (_smm, _sm, _so, _sn) -> {
          StubUpdateShardOperation op = new StubUpdateShardOperation(_smm, _sm, _so, _sn);
          op.setCallBase(true);
          op.DoGlobalPostLocalExecuteIStoreTransactionScope = (ts) -> {
            if (shouldThrow) {
              throw new StoreException("", ShardMapFaultHandlingTest.TransientSqlException);
            } else {
              Object original = op.DoGlobalPostLocalExecuteIStoreTransactionScope;
              op.DoGlobalPostLocalExecuteIStoreTransactionScope = null;
              try {
                return op.doGlobalPostLocalExecute(ts);
              } finally {
                op.DoGlobalPostLocalExecuteIStoreTransactionScope =
                    (Func1Param<IStoreTransactionScope, StoreResults>) original;
              }
            }
          };
          op.UndoLocalSourceExecuteIStoreTransactionScope = (ts) -> {
            if (shouldThrow) {
              throw new StoreException("", ShardMapFaultHandlingTest.TransientSqlException);
            } else {
              Object original = op.UndoLocalSourceExecuteIStoreTransactionScope;
              op.UndoLocalSourceExecuteIStoreTransactionScope = null;
              try {
                return op.undoLocalSourceExecute(ts);
              } finally {
                op.UndoLocalSourceExecuteIStoreTransactionScope =
                    (Func1Param<IStoreTransactionScope, StoreResults>) original;
              }
            }
          };
          return op;
        };

    IStoreOperationFactory sof = stubStoreOperationFactory;

    // TODO:new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero)
    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), sof, new CacheStore(), ShardMapManagerLoadPolicy.Lazy,
        RetryPolicy.DefaultRetryPolicy, RetryBehavior.getDefaultRetryBehavior());

    ShardMap sm = smm.getShardMap(ShardMapTest.defaultShardMapName);
    assert sm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTest.shardDbs[0]);

    Shard sNew = sm.createShard(new ShardCreationInfo(sl, ShardStatus.Online));

    ShardUpdate su = new ShardUpdate();
    su.setStatus(ShardStatus.Offline);

    boolean storeOperationFailed = false;
    try {
      Shard sUpdated = sm.updateShard(sNew, su);
      assert sNew != null;
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ShardMap == sme.getErrorCategory();
      assert ShardManagementErrorCode.StorageOperationFailure == sme.getErrorCode();
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // verify that shard status is not changed.
    Shard sValidate = sm.getShard(sl);
    assert sNew.getStatus() == sValidate.getStatus();

    // Obtain the pending operations.
    // C# TO JAVA CONVERTER TODO TASK: There is no equivalent to implicit typing in Java:
    // TODO var pendingOperations = ShardMapperTest.GetPendingStoreOperations();
    // assert pendingOperations.Count() == 1;
    //
    // TODO shouldThrow = false;
    storeOperationFailed = false;
    try {
      sm.updateShard(sNew, su);
    } catch (ShardManagementException e) {
      storeOperationFailed = true;
    }

    assert !storeOperationFailed;
    sValidate = sm.getShard(sl);
    assert su.getStatus() == sValidate.getStatus();
  }

  private class NTimeFailingAddShardOperation extends AddShardOperation {

    private int _failureCountMax;
    private int _currentFailureCount;

    public NTimeFailingAddShardOperation(int failureCountMax, ShardMapManager shardMapManager,
        StoreShardMap shardMap, StoreShard shard) {
      super(shardMapManager, shardMap, shard);
      _failureCountMax = failureCountMax;
      _currentFailureCount = 0;
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      if (_currentFailureCount < _failureCountMax) {
        _currentFailureCount++;

        throw new StoreException("", ShardMapFaultHandlingTest.TransientSqlException);
      } else {
        return super.doGlobalPostLocalExecute(ts);
      }
    }
  }

  private class NTimeFailingRemoveShardOperation extends RemoveShardOperation {

    private int _failureCountMax;
    private int _currentFailureCount;

    public NTimeFailingRemoveShardOperation(int failureCountMax, ShardMapManager shardMapManager,
        StoreShardMap shardMap, StoreShard shard) {
      super(shardMapManager, shardMap, shard);
      _failureCountMax = failureCountMax;
      _currentFailureCount = 0;
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      if (_currentFailureCount < _failureCountMax) {
        _currentFailureCount++;

        throw new StoreException("", ShardMapFaultHandlingTest.TransientSqlException);
      } else {
        return super.doGlobalPostLocalExecute(ts);
      }
    }
  }

  private class NTimeFailingUpdateShardOperation extends UpdateShardOperation {

    private int _failureCountMax;
    private int _currentFailureCount;

    public NTimeFailingUpdateShardOperation(int failureCountMax, ShardMapManager shardMapManager,
        StoreShardMap shardMap, StoreShard shardOld, StoreShard shardNew) {
      super(shardMapManager, shardMap, shardOld, shardNew);
      _failureCountMax = failureCountMax;
      _currentFailureCount = 0;
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      if (_currentFailureCount < _failureCountMax) {
        _currentFailureCount++;

        throw new StoreException("", ShardMapFaultHandlingTest.TransientSqlException);
      } else {
        return super.doGlobalPostLocalExecute(ts);
      }
    }
  }
}