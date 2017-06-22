package com.microsoft.azure.elasticdb.shard.unittests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.microsoft.azure.elasticdb.core.commons.helpers.EventHandler;
import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryBehavior;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryingEventArgs;
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
import com.microsoft.azure.elasticdb.shard.store.StoreLogEntry;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreShard;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperation;
import com.microsoft.azure.elasticdb.shard.storeops.map.AddShardOperation;
import com.microsoft.azure.elasticdb.shard.storeops.map.RemoveShardOperation;
import com.microsoft.azure.elasticdb.shard.storeops.map.UpdateShardOperation;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func1Param;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func3Param;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func4Param;
import com.microsoft.azure.elasticdb.shard.stubs.StubAddShardOperation;
import com.microsoft.azure.elasticdb.shard.stubs.StubRemoveShardOperation;
import com.microsoft.azure.elasticdb.shard.stubs.StubStoreOperationFactory;
import com.microsoft.azure.elasticdb.shard.stubs.StubUpdateShardOperation;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
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
public class ShardMapTests {

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
    ShardMap sm = smm.getShardMap(ShardMapTests.defaultShardMapName);
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
      for (int i = 0; i < ShardMapTests.shardDbs.length; i++) {
        try (Statement stmt = conn.createStatement()) {
          String query = String.format(Globals.DROP_DATABASE_QUERY, ShardMapTests.shardDbs[i]);
          stmt.executeUpdate(query);
        } catch (SQLException ex) {
          ex.printStackTrace();
        }
        try (Statement stmt = conn.createStatement()) {
          String query = String.format(Globals.CREATE_DATABASE_QUERY, ShardMapTests.shardDbs[i]);
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

      ShardMap sm = smm.createListShardMap(ShardMapTests.defaultShardMapName, ShardKeyType.Int32);
      assertNotNull(sm);
      assertEquals(ShardMapTests.defaultShardMapName, sm.getName());

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
      for (int i = 0; i < ShardMapTests.shardDbs.length; i++) {
        try (Statement stmt = conn.createStatement()) {
          String query = String.format(Globals.DROP_DATABASE_QUERY, ShardMapTests.shardDbs[i]);
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
    ShardMapTests.cleanShardMapsHelper();
  }

  /**
   * Cleans up common state per-test.
   */
  @After
  public void shardMapTestCleanup() {
    ShardMapTests.cleanShardMapsHelper();
  }

  /**
   * Add a shard to shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void createShardDefault() throws SQLException {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ShardMap sm = smm.getShardMap(ShardMapTests.defaultShardMapName);
    assertNotNull(sm);

    // TODO: shard location with sqlprotocol and port name provided
    ShardLocation s1 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTests.shardDbs[0]);

    Shard shardNew = sm.createShard(s1);

    assertNotNull(shardNew);

    assertEquals(s1.toString(), shardNew.getLocation().toString());
    assertEquals(s1.toString(), sm.getShard(s1).getLocation().toString());

    try (Connection conn =
        shardNew.openConnection(Globals.SHARD_USER_CONN_STRING, ConnectionOptions.Validate)) {
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

    ShardMap sm = smm.getShardMap(ShardMapTests.defaultShardMapName);

    assertNotNull(sm);

    ShardLocation s1 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTests.shardDbs[0]);

    Shard shardNew = sm.createShard(s1);

    assertNotNull(shardNew);

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

    ShardMap sm = smm.getShardMap(ShardMapTests.defaultShardMapName);
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
    ShardMap sm = smm.getShardMap(ShardMapTests.defaultShardMapName);
    assertNotNull(sm);

    ShardLocation s1 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTests.shardDbs[0]);

    Shard shardNew = sm.createShard(s1);

    assertNotNull(shardNew);

    sm.deleteShard(shardNew);

    ReferenceObjectHelper<Shard> refShard = new ReferenceObjectHelper<>(shardNew);
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

    ShardMap sm = smm.getShardMap(ShardMapTests.defaultShardMapName);
    assertNotNull(sm);

    ShardLocation s1 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTests.shardDbs[0]);

    Shard shardNew = sm.createShard(s1);

    assertNotNull(shardNew);

    sm.deleteShard(shardNew);

    boolean removeFailed = false;

    try {
      sm.deleteShard(shardNew);
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

    ShardMap sm = smm.getShardMap(ShardMapTests.defaultShardMapName);
    assertNotNull(sm);

    ShardLocation s1 =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, Globals.SHARD_MAP_MANAGER_DATABASE_NAME);

    Shard shardNew = sm.createShard(s1);

    // Update shard to increment version

    ShardUpdate su = new ShardUpdate();
    su.setStatus(ShardStatus.Offline);

    sm.updateShard(shardNew, su);

    boolean removeFailed = false;

    try {
      sm.deleteShard(shardNew);
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

    ShardMap sm = smm.getShardMap(ShardMapTests.defaultShardMapName);
    assertNotNull(sm);

    ShardLocation s1 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTests.shardDbs[0]);

    Shard shardNew = sm.createShard(new ShardCreationInfo(s1, ShardStatus.Online));

    ShardUpdate su = new ShardUpdate();
    su.setStatus(ShardStatus.Offline);

    Shard shardUpdated = sm.updateShard(shardNew, su);
    assertNotNull(shardNew);
    assertNotNull(shardUpdated);
  }

  /**
   * Update shard with version mismatch.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void updateShardVersionMismatch() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ShardMap sm = smm.getShardMap(ShardMapTests.defaultShardMapName);
    assertNotNull(sm);

    ShardLocation s1 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTests.shardDbs[0]);

    Shard shardNew = sm.createShard(new ShardCreationInfo(s1, ShardStatus.Online));

    ShardUpdate su = new ShardUpdate();
    su.setStatus(ShardStatus.Offline);

    Shard shardUpdated = sm.updateShard(shardNew, su);
    assertNotNull(shardNew);
    assertNotNull(shardUpdated);

    boolean updateFailed = false;

    try {
      sm.updateShard(shardNew, su);
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

    ShardMap sm = smm.getShardMap(ShardMapTests.defaultShardMapName);
    assertNotNull(sm);

    ShardLocation s1 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTests.shardDbs[0]);

    Shard shardNew = sm.createShard(new ShardCreationInfo(s1, ShardStatus.Online));

    ShardUpdate su = new ShardUpdate();
    su.setStatus(ShardStatus.Offline);

    Shard shardUpdated = sm.updateShard(shardNew, su);
    assertNotNull(shardUpdated);

    boolean validationFailed = false;

    try (Connection conn =
        shardNew.openConnection(Globals.SHARD_USER_CONN_STRING, ConnectionOptions.Validate)) {
      conn.close();
    } catch (ShardManagementException sme) {
      validationFailed = true;
      assertEquals(ShardManagementErrorCategory.Validation, sme.getErrorCategory());
      assertEquals(ShardManagementErrorCode.ShardVersionMismatch, sme.getErrorCode());
    }

    assertTrue(validationFailed);

    validationFailed = false;

    try (Connection conn =
        shardUpdated.openConnection(Globals.SHARD_USER_CONN_STRING, ConnectionOptions.Validate)) {
      conn.close();
    } catch (ShardManagementException ex) {
      validationFailed = true;
    }

    assertFalse(validationFailed);
  }

  /**
   * Add a shard to shard map, abort transaction in GSM.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void createShardAbortGsm() {
    int retryCount = 0;

    EventHandler<RetryingEventArgs> eventHandler = (sender, arg) -> {
      //TODO: retryCount++;
    };

    StubStoreOperationFactory stubStoreOperationFactory = new StubStoreOperationFactory();
    stubStoreOperationFactory.setCallBase(true);
    stubStoreOperationFactory.createAddShardOperationShardMapManagerIStoreShardMapIStoreShard =
        (smm, sm, s) -> new NTimeFailingAddShardOperation(10, smm, sm, s);

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), stubStoreOperationFactory, new CacheStore(),
        ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(5, Duration.ZERO, Duration.ZERO,
        Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    ShardMap sm = smm.getShardMap(ShardMapTests.defaultShardMapName);

    assert sm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTests.shardDbs[0]);

    boolean storeOperationFailed = false;

    //TODO: smm.shardMapManagerRetrying += eventHandler;

    try {
      Shard shardNew = sm.createShard(sl);
      assert shardNew != null;
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
    List<Shard> shardList = sm.getShards();

    Iterator<Shard> shardIterator = shardList.iterator();
    while (shardIterator.hasNext()) {
      count++;
    }
    assert 0 == count;
  }

  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void updateShardAbortGsm() {
    StubStoreOperationFactory stubStoreOperationFactory = new StubStoreOperationFactory();
    stubStoreOperationFactory.setCallBase(true);
    stubStoreOperationFactory.createUpdateShardOperation4Param = (smm, sm, so, sn) ->
        new NTimeFailingUpdateShardOperation(10, smm, sm, so, sn);

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), stubStoreOperationFactory, new CacheStore(),
        ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO, Duration.ZERO,
        Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());
    ShardMap sm = smm.getShardMap(ShardMapTests.defaultShardMapName);
    assert sm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTests.shardDbs[0]);

    Shard shardNew = sm.createShard(new ShardCreationInfo(sl, ShardStatus.Online));

    ShardUpdate su = new ShardUpdate();
    su.setStatus(ShardStatus.Offline);

    boolean storeOperationFailed = false;
    try {
      Shard shardUpdated = sm.updateShard(shardNew, su);
      assert shardNew != null;
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ShardMap == sme.getErrorCategory();
      assert ShardManagementErrorCode.StorageOperationFailure == sme.getErrorCode();
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // verify that shard status is not changed.
    Shard shardValidate = sm.getShard(sl);
    assert shardNew.getStatus() == shardValidate.getStatus();
  }

  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void deleteShardAbortGsm() {
    StubStoreOperationFactory stubOperationFactory = new StubStoreOperationFactory();
    stubOperationFactory.setCallBase(true);
    stubOperationFactory.createRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard =
        (smm, sm, s) -> new NTimeFailingRemoveShardOperation(10, smm, sm, s);

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), stubOperationFactory, new CacheStore(),
        ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO, Duration.ZERO,
        Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    ShardMap sm = smm.getShardMap(ShardMapTests.defaultShardMapName);
    assert sm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTests.shardDbs[0]);

    Shard shardNew = sm.createShard(sl);

    assert shardNew != null;

    boolean storeOperationFailed = false;
    try {
      sm.deleteShard(shardNew);
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ShardMap == sme.getErrorCategory();
      assert ShardManagementErrorCode.StorageOperationFailure == sme.getErrorCode();
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // verify that the shard exists in store.
    Shard shardValidate = sm.getShard(sl);
    assert shardValidate != null;
  }

  /**
   * Add a shard to shard map, abort transaction in GSM Do and GSM Undo.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void createShardAbortGsmDoAndLsmUndo() {
    StubStoreOperationFactory stubOperationFactory = new StubStoreOperationFactory();
    stubOperationFactory.setCallBase(true);
    stubOperationFactory.createAddShardOperationShardMapManagerIStoreShardMapIStoreShard =
        setAddShardOperation(true);

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), stubOperationFactory, new CacheStore(),
        ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO, Duration.ZERO,
        Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    ShardMap sm = smm.getShardMap(ShardMapTests.defaultShardMapName);

    assert sm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTests.shardDbs[0]);

    boolean storeOperationFailed = false;
    try {
      Shard shardNew = sm.createShard(sl);
      assert shardNew != null;
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ShardMap == sme.getErrorCategory();
      assert ShardManagementErrorCode.StorageOperationFailure == sme.getErrorCode();
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // Obtain the pending operations.
    List<StoreLogEntry> pendingOperations = ShardMapperTests.getPendingStoreOperations();
    assert pendingOperations.size() == 1;

    // verify that shard map does not have any shards.
    assert 0 == sm.getShards().size();

    stubOperationFactory.createAddShardOperationShardMapManagerIStoreShardMapIStoreShard =
        setAddShardOperation(false);
    storeOperationFailed = false;
    try {
      Shard shardNew = sm.createShard(sl);
      assert shardNew != null;
    } catch (ShardManagementException e) {
      storeOperationFailed = true;
    }

    assert !storeOperationFailed;
    assert 1 == sm.getShards().size();
  }

  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void updateShardAbortGsmDoAndLsmUndo() {
    StubStoreOperationFactory stubStoreOperationFactory = new StubStoreOperationFactory();
    stubStoreOperationFactory.setCallBase(true);
    stubStoreOperationFactory.createUpdateShardOperation4Param = setUpdateShardOperation(true);

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), stubStoreOperationFactory, new CacheStore(),
        ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO, Duration.ZERO,
        Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    ShardMap sm = smm.getShardMap(ShardMapTests.defaultShardMapName);
    assert sm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTests.shardDbs[0]);

    Shard shardNew = sm.createShard(new ShardCreationInfo(sl, ShardStatus.Online));

    ShardUpdate su = new ShardUpdate();
    su.setStatus(ShardStatus.Offline);

    boolean storeOperationFailed = false;
    try {
      sm.updateShard(shardNew, su);
      assert shardNew != null;
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ShardMap == sme.getErrorCategory();
      assert ShardManagementErrorCode.StorageOperationFailure == sme.getErrorCode();
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // verify that shard status is not changed.
    Shard shardValidate = sm.getShard(sl);
    assert shardNew.getStatus() == shardValidate.getStatus();

    // Obtain the pending operations.
    List<StoreLogEntry> pendingOperations = ShardMapperTests.getPendingStoreOperations();
    assert 1 == pendingOperations.size();

    stubStoreOperationFactory.createUpdateShardOperation4Param = setUpdateShardOperation(false);
    storeOperationFailed = false;
    try {
      sm.updateShard(shardNew, su);
    } catch (ShardManagementException e) {
      storeOperationFailed = true;
    }

    assert !storeOperationFailed;
    shardValidate = sm.getShard(sl);
    assert su.getStatus() == shardValidate.getStatus();
  }

  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void deleteShardAbortGsmDoAndLsmUndo() {
    StubStoreOperationFactory stubStoreOperationFactory = new StubStoreOperationFactory();
    stubStoreOperationFactory.setCallBase(true);
    stubStoreOperationFactory.createRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard =
        setRemoveShardOperation(true);

    ShardMapManager smm = new ShardMapManager(
        new SqlShardMapManagerCredentials(Globals.SHARD_MAP_MANAGER_CONN_STRING),
        new SqlStoreConnectionFactory(), stubStoreOperationFactory, new CacheStore(),
        ShardMapManagerLoadPolicy.Lazy, new RetryPolicy(1, Duration.ZERO, Duration.ZERO,
        Duration.ZERO), RetryBehavior.getDefaultRetryBehavior());

    ShardMap sm = smm.getShardMap(ShardMapTests.defaultShardMapName);
    assert sm != null;

    ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTests.shardDbs[0]);

    Shard shardNew = sm.createShard(sl);

    assert shardNew != null;

    boolean storeOperationFailed = false;
    try {
      sm.deleteShard(shardNew);
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ShardMap == sme.getErrorCategory();
      assert ShardManagementErrorCode.StorageOperationFailure == sme.getErrorCode();
      storeOperationFailed = true;
    }

    assert storeOperationFailed;

    // verify that the shard exists in store.
    Shard shardValidate = sm.getShard(sl);
    assert shardValidate != null;

    // Obtain the pending operations.
    List<StoreLogEntry> pendingOperations = ShardMapperTests.getPendingStoreOperations();
    assert pendingOperations.size() == 1;

    stubStoreOperationFactory.createRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard =
        setRemoveShardOperation(false);
    storeOperationFailed = false;
    try {
      sm.deleteShard(shardNew);
    } catch (ShardManagementException e) {
      storeOperationFailed = true;
    }

    assert !storeOperationFailed;
    assert 0 == sm.getShards().size();
  }

  private Func3Param<ShardMapManager, StoreShardMap, StoreShard, IStoreOperation>
  setAddShardOperation(boolean shouldThrow) {
    return (smm, sm, s) -> {
      StubAddShardOperation op = new StubAddShardOperation(smm, sm, s);
      op.setCallBase(true);
      op.doGlobalPostLocalExecuteIStoreTransactionScope = (ts) -> {
        if (shouldThrow) {
          throw new StoreException("StubAddShardOperation",
              ShardMapFaultHandlingTests.TransientSqlException);
        } else {
          Object original = op.doGlobalPostLocalExecuteIStoreTransactionScope;
          op.doGlobalPostLocalExecuteIStoreTransactionScope = null;
          try {
            return op.doGlobalPostLocalExecute(ts);
          } finally {
            op.doGlobalPostLocalExecuteIStoreTransactionScope =
                (Func1Param<IStoreTransactionScope, StoreResults>) original;
          }
        }
      };
      op.undoLocalSourceExecuteIStoreTransactionScope = (ts) -> {
        if (shouldThrow) {
          throw new StoreException("StubAddShardOperation",
              ShardMapFaultHandlingTests.TransientSqlException);
        } else {
          Object original = op.undoLocalSourceExecuteIStoreTransactionScope;
          op.undoLocalSourceExecuteIStoreTransactionScope = null;
          try {
            return op.undoLocalSourceExecute(ts);
          } finally {
            op.undoLocalSourceExecuteIStoreTransactionScope =
                (Func1Param<IStoreTransactionScope, StoreResults>) original;
          }
        }
      };
      return op;
    };
  }

  private Func4Param<ShardMapManager, StoreShardMap, StoreShard, StoreShard, IStoreOperation>
  setUpdateShardOperation(boolean shouldThrow) {
    return (smm, sm, so, sn) -> {
      StubUpdateShardOperation op = new StubUpdateShardOperation(smm, sm, so, sn);
      op.setCallBase(true);
      op.doGlobalPostLocalExecuteIStoreTransactionScope = (ts) -> {
        if (shouldThrow) {
          throw new StoreException("StubUpdateShardOperation",
              ShardMapFaultHandlingTests.TransientSqlException);
        } else {
          Object original = op.doGlobalPostLocalExecuteIStoreTransactionScope;
          op.doGlobalPostLocalExecuteIStoreTransactionScope = null;
          try {
            return op.doGlobalPostLocalExecute(ts);
          } finally {
            op.doGlobalPostLocalExecuteIStoreTransactionScope =
                (Func1Param<IStoreTransactionScope, StoreResults>) original;
          }
        }
      };
      op.undoLocalSourceExecuteIStoreTransactionScope = (ts) -> {
        if (shouldThrow) {
          throw new StoreException("StubUpdateShardOperation",
              ShardMapFaultHandlingTests.TransientSqlException);
        } else {
          Object original = op.undoLocalSourceExecuteIStoreTransactionScope;
          op.undoLocalSourceExecuteIStoreTransactionScope = null;
          try {
            return op.undoLocalSourceExecute(ts);
          } finally {
            op.undoLocalSourceExecuteIStoreTransactionScope =
                (Func1Param<IStoreTransactionScope, StoreResults>) original;
          }
        }
      };
      return op;
    };
  }

  private Func3Param<ShardMapManager, StoreShardMap, StoreShard, IStoreOperation>
  setRemoveShardOperation(boolean shouldThrow) {
    return (smm, sm, s) -> {
      StubRemoveShardOperation op = new StubRemoveShardOperation(smm, sm, s);
      op.setCallBase(true);
      op.doGlobalPostLocalExecuteIStoreTransactionScope = (ts) -> {
        if (shouldThrow) {
          throw new StoreException("StubRemoveShardOperation",
              ShardMapFaultHandlingTests.TransientSqlException);
        } else {
          Object original = op.doGlobalPostLocalExecuteIStoreTransactionScope;
          op.doGlobalPostLocalExecuteIStoreTransactionScope = null;
          try {
            return op.doGlobalPostLocalExecute(ts);
          } finally {
            op.doGlobalPostLocalExecuteIStoreTransactionScope =
                (Func1Param<IStoreTransactionScope, StoreResults>) original;
          }
        }
      };
      op.undoLocalSourceExecuteIStoreTransactionScope = (ts) -> {
        if (shouldThrow) {
          throw new StoreException("StubRemoveShardOperation",
              ShardMapFaultHandlingTests.TransientSqlException);
        } else {
          Object original = op.undoLocalSourceExecuteIStoreTransactionScope;
          op.undoLocalSourceExecuteIStoreTransactionScope = null;
          try {
            return op.undoLocalSourceExecute(ts);
          } finally {
            op.undoLocalSourceExecuteIStoreTransactionScope =
                (Func1Param<IStoreTransactionScope, StoreResults>) original;
          }
        }
      };
      return op;
    };
  }

  private class NTimeFailingAddShardOperation extends AddShardOperation {

    private int failureCountMax;
    private int currentFailureCount;

    public NTimeFailingAddShardOperation(int failureCountMax, ShardMapManager shardMapManager,
        StoreShardMap shardMap, StoreShard shard) {
      super(shardMapManager, shardMap, shard);
      this.failureCountMax = failureCountMax;
      currentFailureCount = 0;
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      if (currentFailureCount < failureCountMax) {
        currentFailureCount++;

        throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
      } else {
        return super.doGlobalPostLocalExecute(ts);
      }
    }
  }

  private class NTimeFailingRemoveShardOperation extends RemoveShardOperation {

    private int failureCountMax;
    private int currentFailureCount;

    public NTimeFailingRemoveShardOperation(int failureCountMax, ShardMapManager shardMapManager,
        StoreShardMap shardMap, StoreShard shard) {
      super(shardMapManager, shardMap, shard);
      this.failureCountMax = failureCountMax;
      currentFailureCount = 0;
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      if (currentFailureCount < failureCountMax) {
        currentFailureCount++;

        throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
      } else {
        return super.doGlobalPostLocalExecute(ts);
      }
    }
  }

  private class NTimeFailingUpdateShardOperation extends UpdateShardOperation {

    private int failureCountMax;
    private int currentFailureCount;

    public NTimeFailingUpdateShardOperation(int failureCountMax, ShardMapManager shardMapManager,
        StoreShardMap shardMap, StoreShard shardOld, StoreShard shardNew) {
      super(shardMapManager, shardMap, shardOld, shardNew);
      this.failureCountMax = failureCountMax;
      currentFailureCount = 0;
    }

    @Override
    public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
      if (currentFailureCount < failureCountMax) {
        currentFailureCount++;

        throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
      } else {
        return super.doGlobalPostLocalExecute(ts);
      }
    }
  }
}