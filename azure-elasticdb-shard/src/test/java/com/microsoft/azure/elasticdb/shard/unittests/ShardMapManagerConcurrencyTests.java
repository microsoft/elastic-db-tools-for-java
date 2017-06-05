package com.microsoft.azure.elasticdb.shard.unittests;

import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.category.ExcludeFromGatedCheckin;
import com.microsoft.azure.elasticdb.shard.map.ListShardMap;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
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
import java.util.List;
import java.util.Objects;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class ShardMapManagerConcurrencyTests {

  /**
   * Shard map name used in the tests.
   */
  private static String s_shardMapName = "Customer";

  /**
   * Initializes common state for tests in this class.
   */
  @BeforeClass
  public static void shardMapManagerConcurrencyTestsInitialize() {

    try (
        Connection conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING)) {
      // Create ShardMapManager database
      try (Statement stmt = conn.createStatement()) {
        String query =
            String.format(Globals.CREATE_DATABASE_QUERY, Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
        stmt.executeUpdate(query);
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    } catch (SQLException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    // Create the shard map manager.
    ShardMapManagerFactory.createSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING,
        ShardMapManagerCreateMode.ReplaceExisting);
  }

  /**
   * Cleans up common state for the all tests in this class.
   */
  @AfterClass
  public static void shardMapManagerConcurrencyTestsCleanup() throws SQLException {
    Globals.dropShardMapManager();
  }

  /**
   * Initializes common state per-test.
   */
  @Before
  public void shardMapManagerConcurrencyTestInitialize() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);
    try {
      ShardMap sm = smm.getShardMap(ShardMapManagerConcurrencyTests.s_shardMapName);
      smm.deleteShardMap(sm);
    } catch (ShardManagementException smme) {
      assert smme.getErrorCode() == ShardManagementErrorCode.ShardMapLookupFailure;
    }
  }

  /**
   * Cleans up common state per-test.
   */
  @After
  public void shardMapManagerConcurrencyTestCleanup() {
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);
    try {
      ShardMap sm = smm.getShardMap(ShardMapManagerConcurrencyTests.s_shardMapName);
      smm.deleteShardMap(sm);
    } catch (ShardManagementException smme) {
      assert smme.getErrorCode() == ShardManagementErrorCode.ShardMapLookupFailure;
    }
  }

  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void concurrencyScenarioListShardMap() {
    boolean operationFailed; // variable to track status of negative test scenarios

    // Create 2 SMM objects representing management and client

    ShardMapManager smmMgmt = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    ShardMapManager smmClient = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    /// #region CreateShardMap

    // Add a shard map from management SMM.
    ShardMap smMgmt = smmMgmt.<Integer>createListShardMap(
        ShardMapManagerConcurrencyTests.s_shardMapName, ShardKeyType.Int32);

    assert Objects.equals(ShardMapManagerConcurrencyTests.s_shardMapName, smMgmt.getName());

    // Lookup shard map from client SMM.
    ShardMap smClient = smmClient.getShardMap(ShardMapManagerConcurrencyTests.s_shardMapName);

    assert smClient != null;

    /// #endregion CreateShardMap

    /// #region ConvertToListShardMap

    ListShardMap<Integer> lsmMgmt = smmMgmt
        .getListShardMap(ShardMapManagerConcurrencyTests.s_shardMapName);
    assert lsmMgmt != null;

    // look up shard map again, it will
    ListShardMap<Integer> lsmClient = smmClient
        .getListShardMap(ShardMapManagerConcurrencyTests.s_shardMapName);
    assert lsmClient != null;

    /// #endregion ConvertToListShardMap

    /// #region DeleteShardMap

    // verify that smClient is accessible

    List<Shard> shardClient = lsmClient.getShards();

    smmMgmt.deleteShardMap(lsmMgmt);

    operationFailed = false;

    try {
      // smClient does not exist, below call will fail.
      List<Shard> sCNew = lsmClient.getShards();
    } catch (ShardManagementException sme) {
      assert ShardManagementErrorCategory.ShardMap == sme.getErrorCategory();
      assert ShardManagementErrorCode.ShardMapDoesNotExist == sme.getErrorCode();
      operationFailed = true;
    }

    assert operationFailed;

    /// #endregion DeleteShardMap
  }
}
