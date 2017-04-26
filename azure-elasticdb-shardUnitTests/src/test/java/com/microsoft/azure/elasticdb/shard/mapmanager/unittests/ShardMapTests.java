package com.microsoft.azure.elasticdb.shard.mapmanager.unittests;

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

import static org.junit.Assert.*;

import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.SqlProtocol;
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

/**
 * 
 * Test related to ShardMap class and it's methods.
 *
 */
public class ShardMapTests {
  /**
   * Sharded databases to create for the test.
   */
  private static String[] s_shardedDBs = new String[] {"shard1", "shard2"};

  /**
   * Default shard map name.
   */
  private static String s_defaultShardMapName = "CustomersDefault";

  /**
   * Helper function to clean default shard map.
   */
  private static void cleanShardMapsHelper() {
    ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    // Remove all existing mappings from the list shard map.
    ShardMap sm = smm.GetShardMap(ShardMapTests.s_defaultShardMapName);
    assertNotNull(sm);

    // Remove all shards from list shard map
    Iterable<Shard> s = sm.getShards();
    try {
      Iterator<Shard> sEnum = s.iterator();
      while (sEnum.hasNext()) {
        //TODO: as of now Delete shard doesn't work as expected
        sm.DeleteShard(sEnum.next());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  /**
   * Initializes common state for tests in this class.
   * 
   * @throws SQLServerException
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
      for (int i = 0; i < ShardMapTests.s_shardedDBs.length; i++) {
//TODO        try (Statement stmt = conn.createStatement()) {
//          String query = String.format(Globals.DROP_DATABASE_QUERY, ShardMapTests.s_shardedDBs[i]);
//          stmt.executeUpdate(query);
//        } catch (SQLException ex) {
//          ex.printStackTrace();
//        }
        try (Statement stmt = conn.createStatement()) {
          String query =
              String.format(Globals.CREATE_DATABASE_QUERY, ShardMapTests.s_shardedDBs[i]);
          stmt.executeUpdate(query);
        } catch (SQLException ex) {
          ex.printStackTrace();
        }
      }

      // Create shard map manager.
      ShardMapManagerFactory.CreateSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING,
          ShardMapManagerCreateMode.ReplaceExisting);

      // Create default shard map.
      ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

      ShardMap sm = smm.CreateListShardMap(ShardMapTests.s_defaultShardMapName, ShardKeyType.Int32);
      assertNotNull(sm);
      assertEquals(ShardMapTests.s_defaultShardMapName, sm.getName());

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
   * 
   * @throws SQLServerException
   */
  @AfterClass
  public static void shardMapTestsCleanup() throws SQLServerException {
    SQLServerConnection conn = null;
    try {
      conn = (SQLServerConnection) DriverManager
          .getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

      // Drop shard databases
      for (int i = 0; i < ShardMapTests.s_shardedDBs.length; i++) {
        try (Statement stmt = conn.createStatement()) {
          String query = String.format(Globals.DROP_DATABASE_QUERY, ShardMapTests.s_shardedDBs[i]);
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
   * 
   * @throws SQLServerException
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void createShardDefault() throws SQLServerException {
    ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ShardMap sm = smm.GetShardMap(ShardMapTests.s_defaultShardMapName);
    assertNotNull(sm);

    //TODO: shardlocation with sqlprotocol and port name provided
    ShardLocation s1 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
        ShardMapTests.s_shardedDBs[0]);

    Shard sNew = sm.CreateShard(s1);

    assertNotNull(sNew);
    
    assertEquals(s1.toString(), sNew.getLocation().toString());
    assertEquals(s1.toString(), sm.GetShard(s1).getLocation().toString());

    try (SQLServerConnection conn = (SQLServerConnection) sNew
        .OpenConnection(Globals.SHARD_USER_CONN_STRING, ConnectionOptions.Validate)) {

    }
  }

  /**
   *  Add a duplicate shard to shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void createShardDuplicate() {
    ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ShardMap sm = smm.GetShardMap(ShardMapTests.s_defaultShardMapName);

    assertNotNull(sm);

    ShardLocation s1 =
        new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTests.s_shardedDBs[0]);

    Shard sNew = sm.CreateShard(s1);

    assertNotNull(sNew);

    boolean addFailed = false;

    try {
      Shard sDuplicate = sm.CreateShard(s1);
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
    ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ShardMap sm = smm.GetShardMap(ShardMapTests.s_defaultShardMapName);
    assertNotNull(sm);

    boolean addFailed = false;

    try {
      ShardLocation s1 = new ShardLocation("", "");
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
  public void deleteShardDefault(){
    ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);
    ShardMap sm = smm.GetShardMap(ShardMapTests.s_defaultShardMapName);
    assertNotNull(sm);
    
    ShardLocation s1 = new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ShardMapTests.s_shardedDBs[0]);
    
    Shard sNew = sm.CreateShard(s1);
    
    assertNotNull(sNew);
    
    sm.DeleteShard(sNew);
    
  }

}
