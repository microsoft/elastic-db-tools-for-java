package com.microsoft.azure.elasticdb.shard.unittests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryBehavior;
import com.microsoft.azure.elasticdb.shard.category.ExcludeFromGatedCheckin;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerCreateMode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerFactory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerLoadPolicy;
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

/**
 * Tests related to ShardMapManagerFactory class and it's methods.
 */
public class ShardMapManagerFactoryTests {

  /**
   * Initializes common state for tests in this class.
   */
  @BeforeClass
  public static void shardMapManagerFactoryTestsInitialize() throws SQLException {
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);
      // Create ShardMapManager database
      try (Statement stmt = conn.createStatement()) {
        String query = String
            .format(Globals.CREATE_DATABASE_QUERY, Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
        stmt.executeUpdate(query);
      } catch (SQLException ex) {
        ex.printStackTrace();
      }

      /* Testing TryGetSqlShardMapManager failure case here instead of in
      TryGetShardMapManager_Fail() There is no method to cleanup GSM objects, so if some other test
      runs in lab before TryGetShardMapManager_Fail, then this call will actually succeed as it will
      find earlier SMM structures. Calling it just after creating database makes sure that GSM does
      not exist. Other options were to recreate SMM database in tests (this will increase test
      duration) or delete storage structures (t-sql delete) in the test which is not very clean
      solution. */

      ShardMapManager smm = null;
      ReferenceObjectHelper<ShardMapManager> smmref = new ReferenceObjectHelper<>(smm);
      boolean lookupSmm = ShardMapManagerFactory.tryGetSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Eager,
          RetryBehavior.getDefaultRetryBehavior(), smmref);
      assertFalse(lookupSmm);
    } catch (Exception e) {
      System.out.printf("Failed to connect to SQL database with connection string: "
          + e.getMessage());
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
  public static void shardMapManagerFactoryTestsCleanup() throws SQLException {
    Globals.dropShardMapManager();
  }

  /**
   * Initializes common state per-test.
   */
  @Before
  public void shardMapManagerFactoryTestInitialize() {
  }

  /**
   * Cleans up common state per-test.
   */
  @After
  public void shardMapManagerFactoryTestCleanup() {
  }

  /**
   * Get shard map manager, expects success.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void getShardMapManager_Success() {
    ShardMapManagerFactory.createSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING,
        ShardMapManagerCreateMode.ReplaceExisting);
    for (ShardMapManagerLoadPolicy policy : ShardMapManagerLoadPolicy.values()) {
      ShardMapManager smm1 = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, policy);
      assertNotNull(smm1);

      ShardMapManager smm2 = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, policy, RetryBehavior.getDefaultRetryBehavior());
      assertNotNull(smm2);
    }
  }

  /**
   * Tries to get shard map manager, expects success.
   */
  @SuppressWarnings("unused")
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void tryGetShardMapManager_Success() {
    ShardMapManagerFactory.createSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING,
        ShardMapManagerCreateMode.ReplaceExisting);
    for (ShardMapManagerLoadPolicy policy : ShardMapManagerLoadPolicy.values()) {
      ShardMapManager smm = null;
      ReferenceObjectHelper<ShardMapManager> smmref = new ReferenceObjectHelper<>(smm);
      boolean success;

      success = ShardMapManagerFactory.tryGetSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, policy, smmref);
      assertTrue(success);
      assertNotNull(smmref.argValue);

      success = ShardMapManagerFactory
          .tryGetSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING,
              policy, RetryBehavior.getDefaultRetryBehavior(), smmref);
      assertTrue(success);
      assertNotNull(smmref.argValue);
    }
  }

  /**
   * Tries to get shard map manager, expects failure.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void tryGetShardMapManager_Fail() {
    ShardMapManagerFactory.createSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING,
        ShardMapManagerCreateMode.ReplaceExisting);

    ShardMapManager smm = null;
    ReferenceObjectHelper<ShardMapManager> smmref = new ReferenceObjectHelper<>(smm);
    boolean success = false;
    try {
      success = ShardMapManagerFactory.tryGetSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Eager, null, smmref);
    } catch (IllegalArgumentException e) {
      assertFalse(success);
      assertNull(smmref.argValue);
    }
  }

  /**
   * Create shard map manager.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void createShardMapManager_Overwrite() {
    ShardMapManagerFactory.createSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING,
        ShardMapManagerCreateMode.ReplaceExisting);
  }

  /**
   * Create shard map manager, disallowing over-write.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void createShardMapManager_NoOverwrite() {
    ShardMapManagerFactory.createSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING,
        ShardMapManagerCreateMode.ReplaceExisting);
    try {
      ShardMapManagerFactory.createSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING,
          ShardMapManagerCreateMode.KeepExisting);
    } catch (ShardManagementException smme) {
      assertEquals(ShardManagementErrorCategory.ShardMapManagerFactory, smme.getErrorCategory());
      assertEquals(ShardManagementErrorCode.ShardMapManagerStoreAlreadyExists, smme.getErrorCode());
    }
  }
}
