package com.microsoft.azure.elasticdb.shard.unittests;

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.base.MappingStatus;
import com.microsoft.azure.elasticdb.shard.base.PointMapping;
import com.microsoft.azure.elasticdb.shard.base.PointMappingUpdate;
import com.microsoft.azure.elasticdb.shard.base.Range;
import com.microsoft.azure.elasticdb.shard.base.RangeMapping;
import com.microsoft.azure.elasticdb.shard.base.RangeMappingUpdate;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.ShardStatus;
import com.microsoft.azure.elasticdb.shard.base.ShardUpdate;
import com.microsoft.azure.elasticdb.shard.cache.PerfCounterCreationData;
import com.microsoft.azure.elasticdb.shard.cache.PerfCounterInstance;
import com.microsoft.azure.elasticdb.shard.cache.PerformanceCounterName;
import com.microsoft.azure.elasticdb.shard.cache.PerformanceCounterWrapper;
import com.microsoft.azure.elasticdb.shard.category.ExcludeFromGatedCheckin;
import com.microsoft.azure.elasticdb.shard.map.ListShardMap;
import com.microsoft.azure.elasticdb.shard.map.RangeShardMap;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerCreateMode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerFactory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerLoadPolicy;
import com.microsoft.azure.elasticdb.shard.mapper.ConnectionOptions;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlConnectionStringBuilder;
import com.microsoft.azure.elasticdb.shard.utils.PerformanceCounters;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.AssumptionViolatedException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests based on scenarios which cover various aspects of the ShardMapManager library.
 **/
public class ScenarioTests {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  // Shards with single user per tenant model.
  private static String[] perTenantDBs =
      new String[]{"PerTenantDB1", "PerTenantDB2", "PerTenantDB3", "PerTenantDB4"};
  // Shards with multiple users per tenant model.
  private static String[] multiTenantDBs = new String[]{"MultiTenantDB1", "MultiTenantDB2",
      "MultiTenantDB3", "MultiTenantDB4", "MultiTenantDB5"};
  // Test user to create for Sql Login tests.
  private static String testUser = "TestUser";
  // Password for test user.
  private static String testPassword = "dogmat1C";

  /**
   * Initializes common state for tests in this class.
   */
  @BeforeClass
  public static void scenarioTestsInitialize() {
    // TODO: Clear all connection pools.

    try (
        Connection conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING)) {
      // Create ShardMapManager database
      try (Statement stmt = conn.createStatement()) {
        String query =
            String.format(Globals.CREATE_DATABASE_QUERY, Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
        stmt.executeUpdate(query);
      }

      // Create PerTenantDB databases
      for (int i = 0; i < ScenarioTests.perTenantDBs.length; i++) {
        try (Statement stmt = conn.createStatement()) {
          String query =
              String.format(Globals.DROP_DATABASE_QUERY, ScenarioTests.perTenantDBs[i]);
          stmt.executeUpdate(query);
        }

        try (Statement stmt = conn.createStatement()) {
          String query =
              String.format(Globals.CREATE_DATABASE_QUERY, ScenarioTests.perTenantDBs[i]);
          stmt.executeUpdate(query);
        }
      }

      // Create MultiTenantDB databases
      for (int i = 0; i < ScenarioTests.multiTenantDBs.length; i++) {
        try (Statement stmt = conn.createStatement()) {
          String query =
              String.format(Globals.DROP_DATABASE_QUERY, ScenarioTests.multiTenantDBs[i]);
          stmt.executeUpdate(query);
        }

        try (Statement stmt = conn.createStatement()) {
          String query =
              String.format(Globals.CREATE_DATABASE_QUERY, ScenarioTests.multiTenantDBs[i]);
          stmt.executeUpdate(query);
        }
      }
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * Cleans up common state for the all tests in this class.
   */
  @AfterClass
  public static void scenarioTestsCleanup() throws SQLException {
    Globals.dropShardMapManager();
  }

  /**
   * Initializes common state per-test.
   */
  @Before
  public void scenarioTestInitialize() {
  }

  /**
   * Cleans up common state per-test.
   */
  @After
  public void scenarioTestCleanup() {
  }

  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void basicScenarioDefaultShardMaps() {
    boolean success = true;
    try {
      // Deploy shard map manager.
      ShardMapManagerFactory.createSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING,
          ShardMapManagerCreateMode.ReplaceExisting);

      // Obtain shard map manager.
      ShardMapManager shardMapManager = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

      // Create a single user per-tenant shard map.
      ShardMap defaultShardMap =
          shardMapManager.<Integer>createListShardMap("DefaultShardMap", ShardKeyType.Int32);

      for (int i = 0; i < ScenarioTests.perTenantDBs.length; i++) {
        // Create the shard.
        defaultShardMap.createShard(
            new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ScenarioTests.perTenantDBs[i]));
      }

      // Find the shard by location.
      Shard shardToUpdate = defaultShardMap
          .getShard(new ShardLocation(Globals.TEST_CONN_SERVER_NAME, "PerTenantDB1"));

      // Perform the actual update. Mark offline.
      ShardUpdate tempVar = new ShardUpdate();
      tempVar.setStatus(ShardStatus.Offline);
      Shard updatedShard = defaultShardMap.updateShard(shardToUpdate, tempVar);

      // Verify that update succeeded.
      assert ShardStatus.Offline == updatedShard.getStatus();

      // Find the shard by location.
      Shard shardToDelete = defaultShardMap
          .getShard(new ShardLocation(Globals.TEST_CONN_SERVER_NAME, "PerTenantDB4"));

      defaultShardMap.deleteShard(shardToDelete);

      // Verify that delete succeeded.
      Shard deletedShard = null;

      ReferenceObjectHelper<Shard> tempRefDeletedShard =
          new ReferenceObjectHelper<>(deletedShard);
      defaultShardMap.tryGetShard(shardToDelete.getLocation(), tempRefDeletedShard);
      deletedShard = tempRefDeletedShard.argValue;

      assert deletedShard == null;

      // Now add the shard back for further tests. Create the shard.
      defaultShardMap.createShard(shardToDelete.getLocation());

      // Find the shard by location.
      Shard shardForConnection = defaultShardMap
          .getShard(new ShardLocation(Globals.TEST_CONN_SERVER_NAME, "PerTenantDB1"));

      try (Connection conn = shardForConnection.openConnection(Globals.SHARD_USER_CONN_STRING,
          ConnectionOptions.None)) {
        conn.close();
      } catch (SQLException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }

      // Use the stale state of "shardToUpdate" shard & see if validation works.
      boolean validationFailed = false;
      try (Connection conn = shardToDelete.openConnection(Globals.SHARD_USER_CONN_STRING,
          ConnectionOptions.Validate)) {
        conn.close();
      } catch (ShardManagementException smme) {
        assert smme.getErrorCode() == ShardManagementErrorCode.ShardDoesNotExist;
        validationFailed = true;
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      assert validationFailed;

      // Find the shard by location.
      shardForConnection = defaultShardMap
          .getShard(new ShardLocation(Globals.TEST_CONN_SERVER_NAME, "PerTenantDB1"));

      try (Connection conn = shardForConnection.openConnection(Globals.SHARD_USER_CONN_STRING,
          ConnectionOptions.None)) {
        conn.close();
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      // Use the stale state of "shardToUpdate" shard & see if validation works.
      validationFailed = false;
      try (Connection conn = shardToDelete.openConnection(Globals.SHARD_USER_CONN_STRING,
          ConnectionOptions.Validate)) {
        conn.close();
      } catch (ShardManagementException smme) {
        // TODO:Aggregate
        assert smme.getErrorCode() == ShardManagementErrorCode.ShardDoesNotExist;
        validationFailed = true;
      } catch (Exception ex) {
        ex.printStackTrace();
      }

      assert validationFailed;

    } catch (ShardManagementException smme) {
      success = false;

      log.info(String.format("Error Category: %1$s", smme.getErrorCategory()));
      log.info(String.format("Error Code    : %1$s", smme.getErrorCode()));
      log.info(String.format("Error Message : %1$s", smme.getMessage()));

      if (smme.getCause() != null) {
        log.info(String.format("Storage Error Message : %1$s", smme.getCause().getMessage()));

        if (smme.getCause().getCause() != null) {
          log.info(String.format("SqlClient Error Message : %1$s", smme.getCause().getMessage()));
        }
      }
    }

    assert success;
  }

  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void basicScenarioListShardMapsWithIntegratedSecurity() {
    basicScenarioListShardMapsInternal(Globals.SHARD_MAP_MANAGER_CONN_STRING,
        Globals.SHARD_USER_CONN_STRING);
  }

  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void basicScenarioListShardMapsWithSqlAuthentication() {
    // Try to create a test login
    if (createTestLogin()) {
      SqlConnectionStringBuilder gsmSb =
          new SqlConnectionStringBuilder(Globals.SHARD_MAP_MANAGER_CONN_STRING);
      gsmSb.setIntegratedSecurity(false);
      gsmSb.setUser(testUser);
      gsmSb.setPassword(testPassword);

      SqlConnectionStringBuilder lsmSb =
          new SqlConnectionStringBuilder(Globals.SHARD_USER_CONN_STRING);
      lsmSb.setIntegratedSecurity(false);
      lsmSb.setUser(testUser);
      lsmSb.setPassword(testPassword);

      basicScenarioListShardMapsInternal(gsmSb.getConnectionString(), lsmSb.getConnectionString());

      // Drop test login
      dropTestLogin();
    } else {
      // TODO: Assert.Inconclusive("Failed to create sql login, test skipped");
    }
  }

  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void basicScenarioRangeShardMaps() {
    boolean success = true;
    String rangeShardMapName = "MultiTenantShardMap";
    String tracerMessage = "ScenarioTest; BasicScenarioRangeShardMaps; Test: {} Success: {}";

    try {
      // Deploy shard map manager.
      ShardMapManagerFactory.createSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING,
          ShardMapManagerCreateMode.ReplaceExisting);

      // Obtain shard map manager.
      ShardMapManager shardMapManager = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

      // Create a single user per-tenant shard map.
      RangeShardMap<Integer> multiTenantShardMap =
          shardMapManager.createRangeShardMap(rangeShardMapName, ShardKeyType.Int32);

      for (int i = 0; i < ScenarioTests.multiTenantDBs.length; i++) {
        // Create the shard.
        Shard s = multiTenantShardMap.createShard(
            new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ScenarioTests.multiTenantDBs[i]));

        // Create the mapping.
        multiTenantShardMap.createRangeMapping(new Range(i * 10, (i + 1) * 10), s);
      }

      // Let's add [50, 60) and map it to same shard as 23 i.e. MultiTenantDB3.
      RangeMapping mappingFor23 = multiTenantShardMap.getMappingForKey(23);

      RangeMapping mappingFor50To60 =
          multiTenantShardMap.createRangeMapping(new Range(50, 60), mappingFor23.getShard());

      assert mappingFor23.getShard().getLocation()
          .equals(mappingFor50To60.getShard().getLocation());

      // Move [10, 20) from MultiTenantDB2 to MultiTenantDB1
      RangeMapping mappingToUpdate = multiTenantShardMap.getMappingForKey(10);
      RangeMapping mappingFor5 = multiTenantShardMap.getMappingForKey(5);

      // Try updating that shard in the mapping without taking it offline first.
      boolean updateFailed = false;
      try {
        RangeMappingUpdate tempVar = new RangeMappingUpdate();
        tempVar.setShard(mappingFor5.getShard());
        multiTenantShardMap.updateMapping(mappingToUpdate, tempVar);
      } catch (ShardManagementException smme) {
        assert smme.getErrorCode() == ShardManagementErrorCode.MappingIsNotOffline;
        updateFailed = true;
      }

      log.info(tracerMessage, "Try updating that shard in the mapping without taking it offline.",
          updateFailed);
      assert updateFailed;

      // Mark mapping offline, update shard location.
      RangeMapping newMappingFor10To20Offline = markMappingOfflineAndUpdateShard(
          multiTenantShardMap, mappingToUpdate, mappingFor5.getShard());

      // Verify that update succeeded.
      assert newMappingFor10To20Offline.getShard().getLocation()
          .equals(mappingFor5.getShard().getLocation());
      assert newMappingFor10To20Offline.getStatus() == MappingStatus.Offline;

      // Bring the mapping back online.
      RangeMappingUpdate tempVar2 = new RangeMappingUpdate();
      tempVar2.setStatus(MappingStatus.Online);
      RangeMapping newMappingFor10To20Online =
          multiTenantShardMap.updateMapping(newMappingFor10To20Offline, tempVar2);

      // Verify that update succeeded.
      assert newMappingFor10To20Online.getStatus() == MappingStatus.Online;

      // Find mapping for [0, 10).
      RangeMapping mappingToDelete = multiTenantShardMap.getMappingForKey(5);

      // Try to delete mapping while it is online, the delete should fail.
      boolean operationFailed = false;
      try {
        multiTenantShardMap.deleteMapping(mappingToDelete);
      } catch (ShardManagementException smme) {
        assert smme.getErrorCode() == ShardManagementErrorCode.MappingIsNotOffline;
        operationFailed = true;
      }

      log.info(tracerMessage, "Try to delete mapping while it is online.", operationFailed);
      assert operationFailed;

      // The mapping must be made offline first before it can be deleted.
      RangeMappingUpdate ru = new RangeMappingUpdate();
      ru.setStatus(MappingStatus.Offline);

      mappingToDelete = multiTenantShardMap.updateMapping(mappingToDelete, ru);

      log.info(tracerMessage, "Update the mapping to Offline status.",
          mappingToDelete.getStatus() == MappingStatus.Offline);
      assert (mappingToDelete.getStatus() == MappingStatus.Offline);

      multiTenantShardMap.deleteMapping(mappingToDelete);

      // Verify that delete succeeded.
      try {
        multiTenantShardMap.getMappingForKey(5);
      } catch (ShardManagementException smme) {
        assert smme.getErrorCode() == ShardManagementErrorCode.MappingNotFoundForKey;
      }

      try (Connection conn = multiTenantShardMap.openConnectionForKey(20,
          Globals.SHARD_USER_CONN_STRING, ConnectionOptions.None)) {
        conn.close();
      } catch (SQLException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }

      // Use the stale state of "shardToUpdate" shard & see if validation works.
      boolean validationFailed = false;
      try (Connection conn = multiTenantShardMap.openConnection(mappingToDelete,
          Globals.SHARD_USER_CONN_STRING, ConnectionOptions.Validate)) {
        conn.close();
      } catch (ShardManagementException smme) {
        assert smme.getErrorCode() == ShardManagementErrorCode.MappingDoesNotExist;
        validationFailed = true;
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      log.info(tracerMessage, "Use stale state of shard & check if validation fails.",
          validationFailed);
      assert validationFailed;

      // Obtain new shard map manager instance
      ShardMapManager newShardMapManager = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

      // Get the Range Shard Map
      RangeShardMap<Integer> newMultiTenantShardMap =
          newShardMapManager.getRangeShardMap(rangeShardMapName, ShardKeyType.Int32);

      try (Connection conn = newMultiTenantShardMap.openConnectionForKey(20,
          Globals.SHARD_USER_CONN_STRING, ConnectionOptions.None)) {
        conn.close();
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      // Obtain new shard map manager instance
      newShardMapManager = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

      // Get the Range Shard Map
      newMultiTenantShardMap = newShardMapManager.getRangeShardMap(rangeShardMapName,
          ShardKeyType.Int32);

      // Create a new mapping
      RangeMapping newMappingToDelete = newMultiTenantShardMap.createRangeMapping(new Range(70, 80),
          newMultiTenantShardMap.getMappingForKey(23).getShard());

      // Delete the mapping
      RangeMappingUpdate tempVar = new RangeMappingUpdate();
      tempVar.setStatus(MappingStatus.Offline);
      newMappingToDelete = newMultiTenantShardMap.updateMapping(newMappingToDelete, tempVar);

      newMultiTenantShardMap.deleteMapping(newMappingToDelete);

      // Use the stale state of "shardToUpdate" shard & see if validation works.
      validationFailed = false;
      try (Connection conn = newMultiTenantShardMap.openConnection(newMappingToDelete,
          Globals.SHARD_USER_CONN_STRING, ConnectionOptions.Validate)) {
        conn.close();
      } catch (ShardManagementException smme) {
        // TODO: AggregateException
        assert smme.getErrorCode() == ShardManagementErrorCode.MappingDoesNotExist;
        validationFailed = true;
      } catch (Exception ex) {
        ex.printStackTrace();
      }

      assert validationFailed;

      // Perform tenant lookup. This will populate the cache.
      for (int i = 0; i < ScenarioTests.multiTenantDBs.length; i++) {
        RangeMapping result = shardMapManager.getRangeShardMap("MultiTenantShardMap",
            ShardKeyType.Int32).getMappingForKey((i + 1) * 10);

        log.info(result.getShard().getLocation().toString());
        assertEquals(i, result);
      }

      // Perform tenant lookup. This will read from the cache.
      for (int i = 0; i < ScenarioTests.multiTenantDBs.length; i++) {
        RangeMapping result = shardMapManager.getRangeShardMap("MultiTenantShardMap",
            ShardKeyType.Int32).getMappingForKey((i + 1) * 10);

        log.info(String.valueOf(result.getShard().getLocation()));
        assertEquals(i, result);
      }

      int splitPoint = 55;

      // Split [50, 60) into [50, 55) and [55, 60)
      RangeMapping mappingToSplit = multiTenantShardMap.getMappingForKey(splitPoint);

      List<RangeMapping> rangesAfterSplit = new ArrayList<>(
          multiTenantShardMap.splitMapping(mappingToSplit, splitPoint));

      rangesAfterSplit.sort(Comparator.comparing(RangeMapping::getRange));

      // We should get 2 ranges back.
      assert 2 == rangesAfterSplit.size();

      assert rangesAfterSplit.get(0).getValue().getLow().equals((new Range(50, 55)).getLow());
      assert rangesAfterSplit.get(0).getValue().getHigh().equals((new Range(50, 55)).getHigh());
      assert rangesAfterSplit.get(1).getValue().getLow().equals((new Range(55, 60)).getLow());
      assert rangesAfterSplit.get(1).getValue().getHigh().equals((new Range(55, 60)).getHigh());

      // Split [50, 55) into [50, 52) and [52, 55)
      List<RangeMapping> newRangesAfterAdd = new ArrayList<>(
          multiTenantShardMap.splitMapping(rangesAfterSplit.get(0), 52));

      newRangesAfterAdd.sort(Comparator.comparing(RangeMapping::getRange));

      // We should get 2 ranges back.
      assert 2 == newRangesAfterAdd.size();

      assert newRangesAfterAdd.get(0).getValue().getLow().equals((new Range(50, 52)).getLow());
      assert newRangesAfterAdd.get(0).getValue().getHigh().equals((new Range(50, 52)).getHigh());
      assert newRangesAfterAdd.get(1).getValue().getLow().equals((new Range(52, 55)).getLow());
      assert newRangesAfterAdd.get(1).getValue().getHigh().equals((new Range(52, 55)).getHigh());

      // Move [50, 52) to MultiTenantDB1
      Shard targetShard = multiTenantShardMap.getShard(
          new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ScenarioTests.multiTenantDBs[0]));

      // Mark mapping offline, update shard location.
      RangeMapping movedMapping1 = markMappingOfflineAndUpdateShard(multiTenantShardMap,
          newRangesAfterAdd.get(0), targetShard);

      // Bring the mapping back online.
      RangeMappingUpdate tempVarUpdate = new RangeMappingUpdate();
      tempVarUpdate.setStatus(MappingStatus.Online);
      movedMapping1 = multiTenantShardMap.updateMapping(movedMapping1, tempVarUpdate);

      // Mark mapping offline, update shard location.
      RangeMapping movedMapping2 = markMappingOfflineAndUpdateShard(multiTenantShardMap,
          newRangesAfterAdd.get(1), targetShard);

      // Bring the mapping back online.
      RangeMappingUpdate temp = new RangeMappingUpdate();
      temp.setStatus(MappingStatus.Online);
      movedMapping2 = multiTenantShardMap.updateMapping(movedMapping2, temp);

      // Obtain the final moved mapping.
      RangeMapping finalMovedMapping =
          multiTenantShardMap.mergeMappings(movedMapping1, movedMapping2);

      assert finalMovedMapping.getValue().getLow().equals((new Range(50, 55)).getLow());
      assert finalMovedMapping.getValue().getHigh().equals((new Range(50, 55)).getHigh());

    } catch (ShardManagementException smme) {
      success = catchException(smme);
    }

    assert success;
  }

  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void listShardMapPerformanceCounterValidation() {
    if (PerfCounterInstance.hasCreatePerformanceCategoryPermissions()) {
      String shardMapName = "PerTenantShardMap";

      // Deploy shard map manager.
      ShardMapManager shardMapManager = ShardMapManagerFactory.createSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerCreateMode.ReplaceExisting);

      // Create a single user per-tenant shard map.
      ListShardMap<Integer> perTenantShardMap =
          shardMapManager.createListShardMap(shardMapName, ShardKeyType.Int32);

      ShardLocation sl1 =
          new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ScenarioTests.perTenantDBs[0]);

      // Create first shard and add 1 point mapping.
      Shard s = perTenantShardMap.createShard(sl1);

      // Create the mapping.
      PointMapping p1 = perTenantShardMap.createPointMapping(1, s);

      // Delete and recreate perf counter catagory.
      ShardMapManagerFactory.createPerformanceCategoryAndCounters();

      // Eager loading of shard map manager
      ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Eager);

      // check if perf counter instance exists, instance name logic is from PerfCounterInstance.cs
      String instanceName = String.join("-", String.valueOf(Process.class), shardMapName);

      assert validateInstanceExists(instanceName);

      // verify # of mappings.
      assert validateCounterValue(instanceName, PerformanceCounterName.MappingsCount, 1);

      ListShardMap<Integer> lsm = smm.getListShardMap(shardMapName, ShardKeyType.Int32);

      // Add a new shard and mapping and verify updated counters
      ShardLocation sl2 =
          new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ScenarioTests.perTenantDBs[1]);

      Shard s2 = lsm.createShard(sl2);

      PointMapping p2 = lsm.createPointMapping(2, s2);
      assert validateCounterValue(instanceName, PerformanceCounterName.MappingsCount, 2);

      // Create few more mappings and validate MappingsAddOrUpdatePerSec counter
      s2 = lsm.getShard(sl2);
      for (int i = 3; i < 11; i++) {
        lsm.createPointMapping(i, s2);
        s2 = lsm.getShard(sl2);
      }

      assert validateNonZeroCounterValue(instanceName,
          PerformanceCounterName.MappingsAddOrUpdatePerSec);

      // try to lookup non-existing mapping and verify MappingsLookupFailedPerSec
      for (int i = 0; i < 10; i++) {
        ShardManagementException exception =
            AssertExtensions.assertThrows(
                () -> lsm.openConnectionForKey(20, Globals.SHARD_USER_CONN_STRING));
      }

      assert validateNonZeroCounterValue(instanceName,
          PerformanceCounterName.MappingsLookupFailedPerSec);

      // perform DDR operation few times and validate non-zero counter values
      for (int i = 0; i < 10; i++) {
        try (Connection conn = lsm.openConnectionForKey(1, Globals.SHARD_USER_CONN_STRING)) {
          conn.close();
        } catch (SQLException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }

      assert validateNonZeroCounterValue(instanceName, PerformanceCounterName.DdrOperationsPerSec);
      assert validateNonZeroCounterValue(instanceName,
          PerformanceCounterName.MappingsLookupSucceededPerSec);

      // Remove shard map after removing mappings and shard
      for (int i = 1; i < 11; i++) {
        lsm.deleteMapping(lsm.markMappingOffline(lsm.getMappingForKey(i)));
      }

      assert validateNonZeroCounterValue(instanceName, PerformanceCounterName.MappingsRemovePerSec);

      lsm.deleteShard(lsm.getShard(sl1));
      lsm.deleteShard(lsm.getShard(sl2));

      assert validateCounterValue(instanceName, PerformanceCounterName.MappingsCount, 0);

      smm.deleteShardMap(lsm);

      // make sure that perf counter instance is removed
      assert !validateInstanceExists(instanceName);
    } else {
      throw new AssumptionViolatedException("Inconclusive: Do not have permissions to create"
          + "performance counter category, test skipped");
    }
  }

  private void assertEquals(int i, RangeMapping result) {
    if (i == 0) {
      // Since we moved [10,20) to database 1 earlier.
      assert Objects.equals(result.getShard().getLocation().getDatabase(),
          ScenarioTests.multiTenantDBs[0]);
    } else {
      if (i < 4) {
        assert Objects.equals(result.getShard().getLocation().getDatabase(),
            ScenarioTests.multiTenantDBs[i + 1]);
      } else {
        assert Objects.equals(result.getShard().getLocation().getDatabase(),
            ScenarioTests.multiTenantDBs[2]);
      }
    }
  }

  private boolean createTestLogin() {
    try {
      try (Connection conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_CONN_STRING)) {
        conn.setCatalog("master");
        try (Statement stmt = conn.createStatement()) {
          String query = String
              .format("" + "\r\n" + "if exists (select name from syslogins where name = '%1$s')"
                  + "\r\n" + "begin" + "\r\n" + "   drop login %1$s" + "\r\n" + "end" + "\r\n"
                  + "create login %1$s with password = '%2$s'", testUser, testPassword);
          stmt.executeUpdate(query);

          String query2 = String.format("SP_ADDSRVROLEMEMBER  '%1$s', 'sysadmin'", testUser);
          stmt.executeUpdate(query2);

          return true;
        }
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    } catch (RuntimeException e) {
      log.info(String.format("Exception caught in CreateTestLogin(): %1$s", e.toString()));
    }

    return false;
  }

  private void dropTestLogin() {
    try {
      try (Connection conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_CONN_STRING)) {
        conn.setCatalog("master");
        try (Statement stmt = conn.createStatement()) {
          String query =
              String.format(
                  "" + "\r\n" + "if exists (select name from syslogins where name = '%1$s')"
                      + "\r\n" + "begin" + "\r\n" + "   drop login %1$s" + "\r\n" + "end",
                  testUser);
          stmt.executeUpdate(query);
        }
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    } catch (RuntimeException e) {
      log.info(String.format("Exception caught in DropTestLogin(): %1$s", e.toString()));
    }
  }

  private void basicScenarioListShardMapsInternal(String shardMapManagerConnectionString,
      String shardUserConnectionString) {
    boolean success = true;
    String shardMapName = "PerTenantShardMap";

    try {
      // Deploy shard map manager.
      ShardMapManagerFactory.createSqlShardMapManager(shardMapManagerConnectionString,
          ShardMapManagerCreateMode.ReplaceExisting);

      // Obtain shard map manager.
      ShardMapManager shardMapManager = ShardMapManagerFactory
          .getSqlShardMapManager(shardMapManagerConnectionString, ShardMapManagerLoadPolicy.Lazy);

      // Create a single user per-tenant shard map.
      ListShardMap<Integer> perTenantShardMap =
          shardMapManager.createListShardMap(shardMapName, ShardKeyType.Int32);

      for (int i = 0; i < ScenarioTests.perTenantDBs.length; i++) {
        // Create the shard.
        Shard s = perTenantShardMap.createShard(
            new ShardLocation(Globals.TEST_CONN_SERVER_NAME, ScenarioTests.perTenantDBs[i]));

        // Create the mapping.
        PointMapping p = perTenantShardMap.createPointMapping(i + 1, s);
      }

      // Let's add another point 5 and map it to same shard as 1.

      PointMapping mappingForOne = perTenantShardMap.getMappingForKey(1);

      PointMapping mappingForFive =
          perTenantShardMap.createPointMapping(5, mappingForOne.getShard());

      assert mappingForOne.getShard().getLocation().equals(mappingForFive.getShard().getLocation());

      // Move 3 from PerTenantDB3 to PerTenantDB for 5.
      PointMapping mappingToUpdate = perTenantShardMap.getMappingForKey(3);
      boolean updateFailed = false;

      // Try updating that shard in the mapping without taking it offline first.
      try {
        PointMappingUpdate tempVar = new PointMappingUpdate();
        tempVar.setShard(mappingForFive.getShard());
        perTenantShardMap.updateMapping(mappingToUpdate, tempVar);
      } catch (ShardManagementException smme) {
        assert smme.getErrorCode() == ShardManagementErrorCode.MappingIsNotOffline;
        updateFailed = true;
      }

      assert updateFailed;
      // Perform the actual update.
      PointMapping newMappingFor3 = markMappingOfflineAndUpdateShard(perTenantShardMap,
          mappingToUpdate, mappingForFive.getShard());

      // Verify that update succeeded.
      assert newMappingFor3.getShard().getLocation()
          .equals(mappingForFive.getShard().getLocation());
      assert newMappingFor3.getStatus() == MappingStatus.Offline;

      // Find the shard by location.
      PointMapping mappingToDelete = perTenantShardMap.getMappingForKey(5);

      // Try to delete mapping while it is online, the delete should fail.
      boolean operationFailed = false;
      try {
        perTenantShardMap.deleteMapping(mappingToDelete);
      } catch (ShardManagementException smme) {
        assert smme.getErrorCode() == ShardManagementErrorCode.MappingIsNotOffline;
        operationFailed = true;
      }
      assert operationFailed;

      // TODO:Trace.Assert(operationFailed);

      // The mapping must be taken offline first before it can be deleted.
      PointMappingUpdate tempVar = new PointMappingUpdate();
      tempVar.setStatus(MappingStatus.Offline);

      mappingToDelete = perTenantShardMap.updateMapping(mappingToDelete, tempVar);

      perTenantShardMap.deleteMapping(mappingToDelete);

      // Verify that delete succeeded.
      try {
        perTenantShardMap.getMappingForKey(5);
      } catch (ShardManagementException smme) {
        assert smme.getErrorCode() == ShardManagementErrorCode.MappingNotFoundForKey;
      }

      try (Connection conn = perTenantShardMap.openConnectionForKey(2, shardUserConnectionString,
          ConnectionOptions.None)) {
        conn.close();
      } catch (SQLException e3) {
        // TODO Auto-generated catch block
        e3.printStackTrace();
      }

      // Use the stale state of "shardToUpdate" shard & see if validation works.
      boolean validationFailed = false;
      try {
        try (Connection conn = perTenantShardMap.openConnection(mappingToDelete,
            shardUserConnectionString, ConnectionOptions.Validate)) {
          conn.close();
        } catch (SQLException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      } catch (ShardManagementException smme) {
        assert smme.getErrorCode() == ShardManagementErrorCode.MappingDoesNotExist;
        validationFailed = true;
      }

      assert validationFailed;

      // Obtain a new ShardMapManager instance
      ShardMapManager newShardMapManager = ShardMapManagerFactory
          .getSqlShardMapManager(shardMapManagerConnectionString, ShardMapManagerLoadPolicy.Lazy);

      // Get the ShardMap
      ListShardMap<Integer> newPerTenantShardMap =
          newShardMapManager.getListShardMap(shardMapName, ShardKeyType.Int32);

      try (Connection conn = newPerTenantShardMap.openConnectionForKey(2, shardUserConnectionString,
          ConnectionOptions.None)) {
        conn.close();
      } catch (SQLException e2) {
        // TODO Auto-generated catch block
        e2.printStackTrace();
      }

      // Use the stale state of "shardToUpdate" shard & see if validation works.
      validationFailed = false;

      // Obtain a new ShardMapManager instance
      newShardMapManager = ShardMapManagerFactory
          .getSqlShardMapManager(shardMapManagerConnectionString, ShardMapManagerLoadPolicy.Lazy);

      // Get the ShardMap
      newPerTenantShardMap = newShardMapManager.getListShardMap(shardMapName, ShardKeyType.Int32);

      // Create a new mapping
      PointMapping newMappingToDelete = newPerTenantShardMap.createPointMapping(6,
          newPerTenantShardMap.getMappingForKey(1).getShard());

      // Delete the mapping
      PointMappingUpdate tempVar1 = new PointMappingUpdate();
      tempVar1.setStatus(MappingStatus.Offline);
      newMappingToDelete = newPerTenantShardMap.updateMapping(newMappingToDelete, tempVar1);

      newPerTenantShardMap.deleteMapping(newMappingToDelete);

      try {
        try (Connection conn = newPerTenantShardMap.openConnection(newMappingToDelete,
            shardUserConnectionString, ConnectionOptions.Validate)) {
          conn.close();
        } catch (SQLException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      } catch (ShardManagementException smme) {
        validationFailed = true;
        assert smme.getErrorCode() == ShardManagementErrorCode.MappingDoesNotExist;
      }

      assert validationFailed;

      try (Connection conn = perTenantShardMap.openConnectionForKeyAsync(2,
          shardUserConnectionString, ConnectionOptions.None).call()) {
        conn.close();
      } catch (Exception e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }

      // Use the stale state of "shardToUpdate" shard & see if validation works.
      assert openConnectionAndValidate(perTenantShardMap, mappingToDelete,
          shardUserConnectionString);

      // Obtain a new ShardMapManager instance
      newShardMapManager = ShardMapManagerFactory
          .getSqlShardMapManager(shardMapManagerConnectionString, ShardMapManagerLoadPolicy.Lazy);

      // Get the ShardMap
      newPerTenantShardMap = newShardMapManager.getListShardMap(shardMapName, ShardKeyType.Int32);
      try (Connection conn = newPerTenantShardMap.openConnectionForKeyAsync(2,
          shardUserConnectionString, ConnectionOptions.None).call()) {
        conn.close();
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      // Use the stale state of "shardToUpdate" shard & see if validation works.
      validationFailed = false;

      // Obtain a new ShardMapManager instance
      newShardMapManager = ShardMapManagerFactory
          .getSqlShardMapManager(shardMapManagerConnectionString, ShardMapManagerLoadPolicy.Lazy);

      // Get the ShardMap
      newPerTenantShardMap = newShardMapManager.getListShardMap(shardMapName, ShardKeyType.Int32);

      // Create a new mapping
      newMappingToDelete = newPerTenantShardMap.createPointMapping(6,
          newPerTenantShardMap.getMappingForKey(1).getShard());

      // Delete the mapping
      PointMappingUpdate tempVar2 = new PointMappingUpdate();
      tempVar2.setStatus(MappingStatus.Offline);

      newMappingToDelete = newPerTenantShardMap.updateMapping(newMappingToDelete, tempVar2);

      newPerTenantShardMap.deleteMapping(newMappingToDelete);

      assert openConnectionAndValidate(newPerTenantShardMap, newMappingToDelete,
          shardUserConnectionString);

      // Perform tenant lookup. This will populate the cache.
      for (int i = 0; i < ScenarioTests.perTenantDBs.length; i++) {
        PointMapping result = shardMapManager.getListShardMap("PerTenantShardMap",
            ShardKeyType.Int32).getMappingForKey(i + 1);

        log.info(result.getShard().getLocation().toString());

        // Since we moved 3 to database 1 earlier.
        assert Objects.equals(result.getShard().getLocation()
            .getDatabase(), ScenarioTests.perTenantDBs[i != 2 ? i : 0]);
      }

      // Perform tenant lookup. This will read from the cache.
      for (int i = 0; i < ScenarioTests.perTenantDBs.length; i++) {
        PointMapping result = shardMapManager.getListShardMap("PerTenantShardMap",
            ShardKeyType.Int32).getMappingForKey(i + 1);

        log.info(result.getShard().getLocation().toString());

        // Since we moved 3 to database 1 earlier.
        assert Objects.equals(result.getShard().getLocation()
            .getDatabase(), ScenarioTests.perTenantDBs[i != 2 ? i : 0]);
      }
    } catch (ShardManagementException smme) {
      success = catchException(smme);
    }

    assert success;
  }

  private boolean openConnectionAndValidate(ShardMap shardMap, PointMapping mapping,
      String connString) {
    try (Connection conn = shardMap.openConnectionAsync(mapping,
        connString, ConnectionOptions.Validate).call()) {
      conn.close();
    } catch (ShardManagementException smme) {
      //TODO: Aggregate - RuntimeException runtimeException = (RuntimeException) ex.getCause();
      assert smme.getErrorCode() == ShardManagementErrorCode.MappingDoesNotExist;
      return true;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return false;
  }

  private boolean catchException(ShardManagementException smme) {
    log.info(String.format("Error Category: %1$s", smme.getErrorCategory()));
    log.info(String.format("Error Code    : %1$s", smme.getErrorCode()));
    log.info(String.format("Error Message : %1$s", smme.getMessage()));

    if (smme.getCause() != null) {
      log.info(String.format("Storage Error Message : %1$s", smme.getCause().getMessage()));

      if (smme.getCause().getCause() != null) {
        log.info(String.format("SqlClient Error Message : %1$s",
            smme.getCause().getCause().getMessage()));
      }
    }

    return false;
  }

  private boolean validateNonZeroCounterValue(String instanceName,
      PerformanceCounterName counterName) {
    String counterdisplayName =
        PerfCounterInstance.counterList.stream().filter(c -> c.getCounterName() == counterName)
            .map(PerfCounterCreationData::getCounterDisplayName).findFirst().toString();

    try (PerformanceCounterWrapper pc =
        new PerformanceCounterWrapper(PerformanceCounters.ShardManagementPerformanceCounterCategory,
            counterdisplayName, instanceName)) {
      // TODO:return pc.RawValue != 0;
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return false;
  }

  private boolean validateCounterValue(String instanceName, PerformanceCounterName counterName,
      long value) {
    String counterdisplayName =
        PerfCounterInstance.counterList.stream().filter(c -> c.getCounterName() == counterName)
            .map(PerfCounterCreationData::getCounterDisplayName).findFirst().toString();

    try (PerformanceCounterWrapper pc =
        new PerformanceCounterWrapper(PerformanceCounters.ShardManagementPerformanceCounterCategory,
            counterdisplayName, instanceName)) {
      // TODO return (new Long(pc.RawValue)).equals(value);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return false;
  }

  private boolean validateInstanceExists(String instanceName) {
    return false;
    // TODO: PerformanceCounterCategory
    // return PerformanceCounterCategory.InstanceExists(instanceName,
    // PerformanceCounters.ShardManagementPerformanceCounterCategory);
  }

  private <T> RangeMapping markMappingOfflineAndUpdateShard(RangeShardMap<T> map,
      RangeMapping mapping, Shard newShard) {
    RangeMappingUpdate tempVar = new RangeMappingUpdate();
    tempVar.setStatus(MappingStatus.Offline);
    RangeMapping mappingOffline = map.updateMapping(mapping, tempVar);
    assert mappingOffline != null;

    RangeMappingUpdate tempVar2 = new RangeMappingUpdate();
    tempVar2.setShard(newShard);
    return map.updateMapping(mappingOffline, tempVar2);
  }

  private <T> PointMapping markMappingOfflineAndUpdateShard(ListShardMap<T> map,
      PointMapping mapping, Shard newShard) {
    PointMappingUpdate tempVar = new PointMappingUpdate();
    tempVar.setStatus(MappingStatus.Offline);
    PointMapping mappingOffline = map.updateMapping(mapping, tempVar);
    assert mappingOffline != null;

    PointMappingUpdate tempVar2 = new PointMappingUpdate();
    tempVar2.setShard(newShard);
    return map.updateMapping(mappingOffline, tempVar2);
  }
}
