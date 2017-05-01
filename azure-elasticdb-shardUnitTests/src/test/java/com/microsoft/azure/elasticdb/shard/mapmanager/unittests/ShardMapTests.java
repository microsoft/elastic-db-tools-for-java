package com.microsoft.azure.elasticdb.shard.mapmanager.unittests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardCreationInfo;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.ShardStatus;
import com.microsoft.azure.elasticdb.shard.base.ShardUpdate;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerCreateMode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerFactory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerLoadPolicy;
import com.microsoft.azure.elasticdb.shard.mapmanager.category.ExcludeFromGatedCheckin;
import com.microsoft.azure.elasticdb.shard.mapper.ConnectionOptions;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
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
  public static void shardMapTestsInitialize() throws SQLServerException {
    SQLServerConnection conn = null;
    try {
      conn = (SQLServerConnection) DriverManager
          .getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);
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
          String query =
              String.format(Globals.CREATE_DATABASE_QUERY, ShardMapTests.shardDbs[i]);
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
  public static void shardMapTestsCleanup() throws SQLServerException {
    SQLServerConnection conn = null;
    try {
      conn = (SQLServerConnection) DriverManager
          .getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

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
  public void createShardDefault() throws SQLServerException {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ShardMap sm = smm.getShardMap(ShardMapTests.defaultShardMapName);
    assertNotNull(sm);

    // TODO: shardlocation with sqlprotocol and port name provided
    ShardLocation s1 =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTests.shardDbs[0]);

    Shard sNew = sm.createShard(s1);

    assertNotNull(sNew);

    assertEquals(s1.toString(), sNew.getLocation().toString());
    assertEquals(s1.toString(), sm.getShard(s1).getLocation().toString());

    try (SQLServerConnection conn = (SQLServerConnection) sNew
        .openConnection(Globals.SHARD_USER_CONN_STRING, ConnectionOptions.Validate)) {
      //TODO?
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

    ShardLocation s1 =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTests.shardDbs[0]);

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

    ShardLocation s1 =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTests.shardDbs[0]);

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

    ShardMap sm = smm.getShardMap(ShardMapTests.defaultShardMapName);
    assertNotNull(sm);

    ShardLocation s1 =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTests.shardDbs[0]);

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

    ShardMap sm = smm.getShardMap(ShardMapTests.defaultShardMapName);
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

    ShardMap sm = smm.getShardMap(ShardMapTests.defaultShardMapName);
    assertNotNull(sm);

    ShardLocation s1 =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTests.shardDbs[0]);

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

    ShardMap sm = smm.getShardMap(ShardMapTests.defaultShardMapName);
    assertNotNull(sm);

    ShardLocation s1 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapTests.shardDbs[0]);

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
  public void validateShard() throws SQLServerException {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ShardMap sm = smm.getShardMap(ShardMapTests.defaultShardMapName);
    assertNotNull(sm);

    ShardLocation s1 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapTests.shardDbs[0]);

    Shard sNew = sm.createShard(new ShardCreationInfo(s1, ShardStatus.Online));

    ShardUpdate su = new ShardUpdate();
    su.setStatus(ShardStatus.Offline);

    Shard sUpdated = sm.updateShard(sNew, su);
    assertNotNull(sUpdated);

    boolean validationFailed = false;

    try (SQLServerConnection conn = (SQLServerConnection) sNew
        .openConnection(Globals.SHARD_USER_CONN_STRING, ConnectionOptions.Validate)) {
      conn.close();
    } catch (ShardManagementException sme) {
      validationFailed = true;
      assertEquals(ShardManagementErrorCategory.Validation, sme.getErrorCategory());
      assertEquals(ShardManagementErrorCode.ShardVersionMismatch, sme.getErrorCode());
    }

    assertTrue(validationFailed);

    validationFailed = false;

    try (SQLServerConnection conn = (SQLServerConnection) sUpdated
        .openConnection(Globals.SHARD_USER_CONN_STRING, ConnectionOptions.Validate)) {
      conn.close();
    } catch (ShardManagementException ex) {
      validationFailed = true;
    }

    assertFalse(validationFailed);
  }
}
