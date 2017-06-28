package com.microsoft.azure.elasticdb.shard.unittests;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import static org.junit.Assert.assertEquals;

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.shard.base.MappingLockToken;
import com.microsoft.azure.elasticdb.shard.base.MappingStatus;
import com.microsoft.azure.elasticdb.shard.base.PointMapping;
import com.microsoft.azure.elasticdb.shard.base.PointMappingUpdate;
import com.microsoft.azure.elasticdb.shard.base.Range;
import com.microsoft.azure.elasticdb.shard.base.RangeMapping;
import com.microsoft.azure.elasticdb.shard.base.RangeMappingUpdate;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardCreationInfo;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.base.ShardStatus;
import com.microsoft.azure.elasticdb.shard.base.ShardUpdate;
import com.microsoft.azure.elasticdb.shard.category.ExcludeFromGatedCheckin;
import com.microsoft.azure.elasticdb.shard.map.ListShardMap;
import com.microsoft.azure.elasticdb.shard.map.RangeShardMap;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerCreateMode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerFactory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerLoadPolicy;
import com.microsoft.azure.elasticdb.shard.mapper.ConnectionOptions;
import com.microsoft.azure.elasticdb.shard.recovery.MappingLocation;
import com.microsoft.azure.elasticdb.shard.recovery.RecoveryManager;
import com.microsoft.azure.elasticdb.shard.recovery.RecoveryToken;
import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShardMapManagerLoadTests {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  /**
   * Query to kill connections for a particular database.
   */
  private static final String KILL_CONNECTIONS_FOR_DATABASE_QUERY =
      "declare @stmt varchar(max)" + "\r\n" + "set @stmt = ''" + "\r\n"
          + "SELECT @stmt = @stmt + 'Kill ' + CONVERT(VARCHAR, spid) + ';' " + "\r\n"
          + "FROM master..sysprocesses WHERE spid > 50 AND dbid = DB_ID('%1$s')" + "\r\n"
          + "exec(@stmt)";
  /**
   * Number of shards added to both list and range shard maps.
   */
  private static final int INITIAL_SHARD_COUNT = 6;
  /**
   * Lowest point on Integer range that can be mapped by unit tests.
   */
  private static final int MIN_MAPPING_POINT = -2000;
  /**
   * Highest point on Integer range that can be mapped by unit tests.
   */
  private static final int MAX_MAPPING_POINT = 2000;
  /**
   * Maximum size of a single range mapping.
   */
  private static final int MAX_RANGE_MAPPING_SIZE = 10;
  /**
   * Sharded databases to create for the test.
   */
  private static String[] shardDBs = new String[]{"shard1", "shard2", "shard3", "shard4",
      "shard5", "shard6", "shard7", "shard8", "shard9", "shard10"};
  /**
   * List shard map name.
   */
  private static String listShardMapName = "CustomersList";
  /**
   * Range shard map name.
   */
  private static String rangeShardMapName = "CustomersRange";
  /**
   * Queries to cleanup objects used for deadlock detection. These will not work against Azure SQL
   * DB, code just catches and ignores SqlException for these queries.
   */
  private static String[] deadlockDetectionCleanupQueries = new String[]{"use msdb",
      "drop event notification CaptureDeadlocks on server", "drop service DeadlockService",
      "drop queue DeadlockQueue", "use master"};
  /**
   * Queries to create objects for deadlock detection. These will not work against Azure SQL DB,
   * code just catches and ignores SqlException for these queries.
   */
  private static String[] deadlockDetectionSetupQueries = new String[]{"use msdb",
      "create queue DeadlockQueue", "create service DeadlockService on queue DeadlockQueue"
      + " ([http://schemas.microsoft.com/SQL/Notifications/PostEventNotification])",
      "create event notification CaptureDeadlocks on server with FAN_IN for DEADLOCK_GRAPH to"
          + " service 'DeadlockService', 'current database'", "use master"};
  /**
   * Query to collect deadlock graphs
   * This will not work against Azure SQL DB, code just catches and ignores SqlException.
   */
  private static String deadlockDetectionQuery = "SELECT CAST(message_body AS XML)"
      + " FROM msdb..DeadlockQueue";
  /**
   * Retry policy used for DDR in unit tests.
   */
  private static RetryPolicy retryPolicy;
  /**
   * Random number generator used to generate keys in unit test.
   */
  private Random random = new Random();

  /**
   * Initializes common state for tests in this class.
   */
  @BeforeClass
  public static void shardMapManagerLoadTestsInitialize() throws SQLException {
    // Clear all connection pools.
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

      // Create ShardMapManager database
      try (Statement stmt = conn.createStatement()) {
        String query = String.format(Globals.CREATE_DATABASE_QUERY,
            Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
        stmt.executeUpdate(query);
      }

      // Create shard databases
      for (int i = 0; i < ShardMapManagerLoadTests.shardDBs.length; i++) {
        try (Statement stmt = conn.createStatement()) {
          String query = String.format(Globals.DROP_DATABASE_QUERY,
              ShardMapManagerLoadTests.shardDBs[i]);
          stmt.executeUpdate(query);
        }

        try (Statement stmt = conn.createStatement()) {
          String query = String.format(Globals.CREATE_DATABASE_QUERY,
              ShardMapManagerLoadTests.shardDBs[i]);
          stmt.executeUpdate(query);
        }
      }

      // cleanup for deadlock monitoring
      for (String q : deadlockDetectionCleanupQueries) {
        try (Statement stmt = conn.createStatement()) {
          stmt.execute(q);
        }
      }

      // setup for deadlock monitoring
      for (String q : deadlockDetectionSetupQueries) {
        try (Statement stmt = conn.createStatement()) {
          stmt.execute(q);
        }
      }

      // Create the shard map manager.
      ShardMapManagerFactory.createSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING,
          ShardMapManagerCreateMode.ReplaceExisting);

      // Create list shard map.
      ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

      ListShardMap<Integer> lsm =
          smm.createListShardMap(ShardMapManagerLoadTests.listShardMapName, ShardKeyType.Int32);

      assert Objects.equals(ShardMapManagerLoadTests.listShardMapName, lsm.getName());

      // Create range shard map.
      RangeShardMap<Integer> rsm = smm.createRangeShardMap(
          ShardMapManagerLoadTests.rangeShardMapName, ShardKeyType.Int32);

      assert Objects.equals(ShardMapManagerLoadTests.rangeShardMapName, rsm.getName());

      // Add 'INITIAL_SHARD_COUNT' shards to list and range shard map.
      for (int i = 0; i < ShardMapManagerLoadTests.INITIAL_SHARD_COUNT; i++) {
        ShardCreationInfo si = new ShardCreationInfo(
            new ShardLocation(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING,
                ShardMapManagerLoadTests.shardDBs[i]), ShardStatus.Online);

        Shard shardList = lsm.createShard(si);
        assert shardList != null;

        Shard shardRange = rsm.createShard(si);
        assert shardRange != null;
      }

      // Initialize retry policy
      retryPolicy = new RetryPolicy(5, Duration.ofMillis(100), Duration.ofSeconds(5),
          Duration.ofMillis(100));

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
  public static void shardMapManagerLoadTestsCleanup() throws SQLException {
    // Detect inconsistencies for all shard locations in a shard map.
    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    RecoveryManager rm = new RecoveryManager(smm);

    boolean inconsistencyDetected = false;

    for (ShardLocation sl : smm.getDistinctShardLocations()) {
      List<RecoveryToken> gs = rm.detectMappingDifferences(sl);

      for (RecoveryToken g : gs) {
        Map<ShardRange, MappingLocation> kvps = rm.getMappingDifferences(g);
        if (kvps.keySet().size() > 0) {
          inconsistencyDetected = true;
          log.info("LSM at location {} is not consistent with GSM", sl);
        }
      }
    }

    // Clear all connection pools.
    boolean deadlocksDetected = false;

    // Check for deadlocks during the run, cleanup database and deadlock objects on successful run
    Connection conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

    // check for any deadlocks occurred during the run and cleanup deadlock monitoring objects
    try (Statement stmt = conn.createStatement()) {
      if (stmt.execute(deadlockDetectionQuery)) {
        ResultSet reader = stmt.getResultSet();
        if (reader.next()) {
          // some deadlocks occurred during the test, collect xml plan for these deadlocks
          deadlocksDetected = true;

          while (reader.next()) {
            log.info("Deadlock information");
            log.info(reader.getSQLXML(0).getString());
          }
        }
      }
    }

    // cleanup only if there are no inconsistencies and deadlocks during the run.
    if (!deadlocksDetected && !inconsistencyDetected) {
      for (String q : deadlockDetectionCleanupQueries) {
        try (Statement stmt = conn.createStatement()) {
          stmt.execute(q);
        }
      }

      // Drop shard databases
      for (int i = 0; i < ShardMapManagerLoadTests.shardDBs.length; i++) {
        try (Statement stmt = conn.createStatement()) {
          stmt.execute(String.format(Globals.DROP_DATABASE_QUERY,
              ShardMapManagerLoadTests.shardDBs[i]));
        }
      }

      // Drop shard map manager database
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(String.format(Globals.DROP_DATABASE_QUERY,
            Globals.SHARD_MAP_MANAGER_DATABASE_NAME));
      }
    }
  }

  /**
   * Helper function to do validation, this will be called using TFH retry policy.
   *
   * @param sm Shard map for a mapping to validate
   * @param key Key to lookup and validate
   */
  private static void validateImpl(ShardMap sm, int key) throws SQLException {
    try (Connection conn = sm.openConnectionForKey(key, Globals.SHARD_USER_CONN_STRING,
        ConnectionOptions.Validate)) {
      conn.close();
    } catch (SQLException e) {
      // Error Number 3980: The request failed to run because the batch is aborted, this can be
      // caused by abort signal sent from client, or another request is running in the same session,
      // which makes the session busy.
      // Error Number = 0, Class = 20, Message = The connection is broken and recovery is not
      // possible.  The connection is marked by the server as unrecoverable.  No attempt was made to
      // restore the connection.
      // Error Number = 0, Class = 11, Message = A severe error occurred on the current command.
      // The results, if any, should be discarded.
      switch (e.getErrorCode()) {
        case 0:
          /* TODO:
          if (e.Class() != 20 && e.Class != 11) {
            throw e;
          }*/
          break;

        case 3980:
          break;

        default:
          throw e;
      }
    }
  }

  /**
   * Get existing point mapping from a list shard map.
   *
   * @param lsm List shard map
   * @return Valid existing point mapping, null if no point mappings found in given shard map
   */
  private PointMapping getRandomPointMapping(ListShardMap<Integer> lsm) {
    // Get all point mappings without storing the results in the cache so that OpenConnection will
    // fetch mapping again.
    List<PointMapping> allMappings = lsm.getMappings();

    if (allMappings.size() == 0) {
      return null;
    }

    int index = random.nextInt(allMappings.size());
    return allMappings.get(index);
  }

  /**
   * Get existing point mapping from a list shard map.
   *
   * @param rsm Range shard map
   * @return Valid existing point mapping, null if no point mappings found in given shard map
   */
  private RangeMapping getRandomRangeMapping(RangeShardMap<Integer> rsm) {
    return getRandomRangeMapping(rsm, 1);
  }

  /**
   * Get existing range mapping from a range shard map.
   *
   * @param rsm Range shard map
   * @param minimumRangeSize Minimum size of range mapping, this can be used for AddRangeWithinRange
   * and RemoveRangeFromRange tests
   * @return Valid existing range mapping, null if no range mappings found in given shard map
   */
  private RangeMapping getRandomRangeMapping(RangeShardMap<Integer> rsm,
      int minimumRangeSize) {
    List<RangeMapping> allMappings = rsm.getMappings();

    ArrayList<RangeMapping> filteredList = new ArrayList<>(allMappings.stream().filter(m ->
        ((int) m.getRange().getHigh().getValue() - (int) m.getRange().getLow().getValue())
            >= minimumRangeSize)
        .collect(Collectors.toList()));

    if (filteredList.isEmpty()) {
      return null;
    }

    return filteredList.get(random.nextInt(filteredList.size()));
  }

  /**
   * Helper function to select a random shard for specified shard map.
   *
   * @param sm Shard map to get shard.
   * @return Shard from specified shard map.
   */
  private Shard getRandomOnlineShardFromShardMap(ShardMap sm) {
    List<Shard> shardList = sm.getShards().stream()
        .filter(s -> s.getStatus().equals(ShardStatus.Online))
        .collect(Collectors.toList());

    if (!shardList.isEmpty()) {
      return shardList.get(random.nextInt(shardList.size()));
    } else {
      return null;
    }
  }

  /**
   * Helper function to add a new shard to given shard map.
   */
  private void addShardToShardMap(ShardMap sm) {
    List<Shard> existingShards = sm.getShards();

    // get list of shard locations that are not already added to this shard map.
    List<String> existingShardNames = existingShards.stream()
        .map(s -> s.getLocation().getDatabase()).collect(Collectors.toList());
    List<String> availableLocationList = Arrays.stream(ShardMapManagerLoadTests.shardDBs)
        .filter(existingShardNames::contains).collect(Collectors.toList());

    if (!availableLocationList.isEmpty()) {
      ShardLocation sl = new ShardLocation(Globals.TEST_CONN_SERVER_NAME,
          availableLocationList.get(random.nextInt(availableLocationList.size())));

      log.info("Trying to add shard at location {} to shard map {}", sl, sm);

      ShardCreationInfo si = new ShardCreationInfo(sl, ShardStatus.Online);

      sm.createShard(si);
    }
  }

  /**
   * Add point mapping.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void loadTestAddPointMapping() {
    try {
      ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

      ListShardMap<Integer> lsm = smm.getListShardMap(
          ShardMapManagerLoadTests.listShardMapName);

      assert lsm != null;
      do {
        // Chose a random shard to add mapping.
        Shard s = getRandomOnlineShardFromShardMap(lsm);
        if (s == null) {
          continue;
        }

        // Create a random integer key for a new mapping and verify that its not already present
        // in this shard map.
        int key = randomNextInt(random, MIN_MAPPING_POINT, MAX_MAPPING_POINT);

        // choose different mapping if this one already exists.
        ReferenceObjectHelper<PointMapping> tempRefPExisting = new ReferenceObjectHelper<>(null);
        if (lsm.tryGetMappingForKey(key, tempRefPExisting)) {
          continue;
        }

        log.info("Trying to add point mapping for key {} to shard location {}", key,
            s.getLocation());

        PointMapping p1 = lsm.createPointMapping(key, s);

        assert p1 != null;

        // Validate mapping by trying to connect
        retryPolicy.executeAction(() -> {
          try {
            validateImpl(lsm, key);
          } catch (SQLException e) {
            log.info("SQLException caught: {}", e.getMessage());
          }
        });
      } while (false);
    } catch (ShardManagementException sme) {
      log.info("Exception caught: {}", sme.getMessage());
    } catch (InterruptedException ex) {
      log.info("Retry Logic Interrupted: {}", ex.getMessage());
    }
  }

  /**
   * Delete point mapping.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void loadTestDeletePointMapping() {
    try {
      ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

      ListShardMap<Integer> lsm = smm.getListShardMap(
          ShardMapManagerLoadTests.listShardMapName);
      assert lsm != null;

      PointMapping p1 = this.getRandomPointMapping(lsm);

      if (p1 != null) {
        log.info("Trying to delete point mapping for key {}", p1.getKey());

        PointMappingUpdate pu = new PointMappingUpdate();
        pu.setStatus(MappingStatus.Offline);

        PointMapping mappingToDelete = lsm.updateMapping(p1, pu);

        lsm.deleteMapping(mappingToDelete);
      }
    } catch (ShardManagementException sme) {
      log.info("Exception caught: {}", sme.getMessage());
    }
  }

  /**
   * DDR for a list shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void loadTestPointMappingDdr() {
    try {
      ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

      ListShardMap<Integer> lsm = smm.getListShardMap(
          ShardMapManagerLoadTests.listShardMapName);
      assert lsm != null;

      PointMapping p1 = this.getRandomPointMapping(lsm);

      if (p1 != null) {
        log.info("Trying to validate point mapping for key {}", p1.getKey());

        // Validate mapping by trying to connect
        retryPolicy.executeAction(() -> {
          try {
            validateImpl(lsm, (int) (p1.getKey().getValue()));
          } catch (SQLException e) {
            log.info("SQLException caught: {}", e.getMessage());
          }
        });
      }
    } catch (ShardManagementException sme) {
      log.info("Exception caught: {}", sme.getMessage());
    } catch (InterruptedException ex) {
      log.info("Retry Logic Interrupted: {}", ex.getMessage());
    }
  }

  /**
   * Add a shard to list shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void loadTestAddShardToListShardMap() {
    try {
      ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

      ListShardMap<Integer> lsm = smm.getListShardMap(
          ShardMapManagerLoadTests.listShardMapName);
      assert lsm != null;

      addShardToShardMap(lsm);
    } catch (ShardManagementException sme) {
      log.info("Exception caught: {}", sme.getMessage());
    }
  }

  /**
   * Mark all shards as online in list shard map. If remove shard operation fails, it will leave
   * shards in offline state, this function will mark all such shards as online.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void loadTestMarkAllShardsAsOnlineInListShardMap() {
    try {
      ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

      ListShardMap<Integer> lsm = smm.getListShardMap(
          ShardMapManagerLoadTests.listShardMapName);
      assert lsm != null;

      for (Shard s : lsm.getShards()) {
        if (s.getStatus().equals(ShardStatus.Offline)) {
          ShardUpdate tempVar = new ShardUpdate();
          tempVar.setStatus(ShardStatus.Online);
          lsm.updateShard(s, tempVar);
        }
      }
    } catch (ShardManagementException sme) {
      log.info("Exception caught: {}", sme.getMessage());
    }
  }

  /**
   * Remove a random shard from list shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void loadTestRemoveShardFromListShardMap() {
    try {
      ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

      ListShardMap<Integer> lsm = smm.getListShardMap(
          ShardMapManagerLoadTests.listShardMapName);
      assert lsm != null;

      List<Shard> existingShards = lsm.getShards();

      if (existingShards.isEmpty()) {
        return;
      }

      // If there is already a shard marked as offline, chose that one to delete.
      // This can happen if earlier remove operation was terminated for some reason
      // - ex. killing connections.
      Shard offlineShard = ListHelper
          .find(existingShards, e -> e.getStatus().equals(ShardStatus.Offline));

      if (offlineShard == null) {
        offlineShard = existingShards.get(random.nextInt(existingShards.size()));

        // First mark shard as offline so that other test threads will not add new mappings to it.
        ShardUpdate tempVar = new ShardUpdate();
        tempVar.setStatus(ShardStatus.Offline);
        offlineShard = lsm.updateShard(offlineShard, tempVar);
      }

      log.info("Trying to remove shard at location {}", offlineShard.getLocation());

      PointMappingUpdate pu = new PointMappingUpdate();
      pu.setStatus(MappingStatus.Offline);

      // Remove all mappings from this shard for given shard map.
      for (PointMapping p : lsm.getMappings(offlineShard)) {
        PointMapping mappingToDelete = lsm.updateMapping(p, pu);
        lsm.deleteMapping(mappingToDelete);
      }

      // Shard object is changed as mappings are removed, get it again.
      Shard deleteShard = lsm.getShard(offlineShard.getLocation());

      // now remove shard.
      lsm.deleteShard(deleteShard);

      log.info("Removed shard at location {} from shard map {}", deleteShard.getLocation(), lsm);
    } catch (ShardManagementException sme) {
      log.info("Exception caught: {}", sme.getMessage());
    }
  }

  /**
   * Add range mapping.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void loadTestAddRangeMapping() {
    try {
      ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

      RangeShardMap<Integer> rsm = smm.getRangeShardMap(
          ShardMapManagerLoadTests.rangeShardMapName);

      assert rsm != null;
      do {
        // Chose a random shard to add mapping.
        Shard s = getRandomOnlineShardFromShardMap(rsm);
        if (s == null) {
          continue;
        }

        // generate random range to add a new range mapping and verify that its not already mapped.
        int minKey = randomNextInt(random, MIN_MAPPING_POINT, MAX_MAPPING_POINT);
        int maxKey = minKey + randomNextInt(random, 1, MAX_RANGE_MAPPING_SIZE);
        maxKey = (maxKey <= MAX_MAPPING_POINT) ? maxKey : MAX_MAPPING_POINT;

        List<RangeMapping> existingMapping = rsm
            .getMappings(new Range(minKey, maxKey));
        if (existingMapping.size() > 0) {
          continue;
        }

        log.info("Trying to add range mapping for key range ({} - {}) to shard location {}", minKey,
            maxKey, s.getLocation());

        RangeMapping r1 = rsm.createRangeMapping(new Range(minKey, maxKey), s);

        assert r1 != null;

        // Validate mapping by trying to connect
        retryPolicy.executeAction(() -> {
          try {
            validateImpl(rsm, minKey);
          } catch (SQLException e) {
            log.info("SQLException caught: {}", e.getMessage());
          }
        });
      } while (false);
    } catch (ShardManagementException sme) {
      log.info("Exception caught: {}", sme.getMessage());
    } catch (InterruptedException ex) {
      log.info("Retry Logic Interrupted: {}", ex.getMessage());
    }
  }

  /**
   * Delete range mapping.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void loadTestDeleteRangeMapping() {
    try {
      ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

      RangeShardMap<Integer> rsm = smm.getRangeShardMap(
          ShardMapManagerLoadTests.rangeShardMapName);
      assert rsm != null;

      RangeMapping r1 = this.getRandomRangeMapping(rsm);

      if (r1 != null) {
        log.info("Trying to delete mapping for range with low value = {}", r1.getRange().getLow());

        RangeMappingUpdate ru = new RangeMappingUpdate();
        ru.setStatus(MappingStatus.Offline);

        RangeMapping mappingToDelete = rsm.updateMapping(r1, ru);

        rsm.deleteMapping(mappingToDelete);
      }
    } catch (ShardManagementException sme) {
      log.info("Exception caught: {}", sme.getMessage());
    }
  }

  /**
   * DDR for a range shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void loadTestRangeMappingDdr() {
    try {
      ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

      RangeShardMap<Integer> rsm = smm.getRangeShardMap(
          ShardMapManagerLoadTests.rangeShardMapName);
      assert rsm != null;

      RangeMapping r1 = this.getRandomRangeMapping(rsm);

      if (r1 != null) {
        int keyToValidate = randomNextInt(random, (int) (r1.getRange().getLow().getValue()),
            (int) (r1.getRange().getHigh().getValue()));

        log.info("Trying to validate mapping for key {}", keyToValidate);

        // Validate mapping by trying to connect
        retryPolicy.executeAction(() -> {
          try {
            validateImpl(rsm, keyToValidate);
          } catch (SQLException e) {
            log.info("SQLException caught: {}", e.getMessage());
          }
        });
      }
    } catch (ShardManagementException sme) {
      log.info("Exception caught: {}", sme.getMessage());
    } catch (InterruptedException ex) {
      log.info("Retry Logic Interrupted: {}", ex.getMessage());
    }
  }

  /**
   * Split range with locking.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void loadTestSplitRangeWithLock() {
    try {
      ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

      RangeShardMap<Integer> rsm = smm.getRangeShardMap(
          ShardMapManagerLoadTests.rangeShardMapName);
      assert rsm != null;

      RangeMapping r1 = this.getRandomRangeMapping(rsm, 2);

      if (r1 != null) {
        int splitPoint = randomNextInt(random, (int) (r1.getRange().getLow().getValue()) + 1,
            (int) (r1.getRange().getHigh().getValue()) - 1);

        log.info("Trying to split range mapping for key range ({} - {}) at {}",
            r1.getRange().getLow().getValue(), r1.getRange().getHigh().getValue(), splitPoint);

        // Lock the mapping
        MappingLockToken mappingLockToken = MappingLockToken.create();
        rsm.lockMapping(r1, mappingLockToken);

        List<RangeMapping> rangeMappings = rsm.splitMapping(r1, splitPoint, mappingLockToken);

        assert 2 == rangeMappings.size();

        for (RangeMapping r2 : rangeMappings) {
          assert r2 != null;
          assertEquals(String.format("LockOwnerId of mapping: %1$s does not match id in store!",
              r2), mappingLockToken, rsm.getMappingLockOwner(r2));

          // Unlock each mapping and verify
          rsm.unlockMapping(r2, mappingLockToken);
          assertEquals(String.format("Mapping: %1$s not unlocked as expected!",
              r2), MappingLockToken.NoLock, rsm.getMappingLockOwner(r2));
        }
      }
    } catch (ShardManagementException sme) {
      log.info("Exception caught: {}", sme.getMessage());
    }
  }

  /**
   * Split range without locking.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void loadTestSplitRangeNoLock() {
    try {
      ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

      RangeShardMap<Integer> rsm = smm.getRangeShardMap(
          ShardMapManagerLoadTests.rangeShardMapName);
      assert rsm != null;

      RangeMapping r1 = this.getRandomRangeMapping(rsm, 2);

      if (r1 != null) {
        int splitPoint = randomNextInt(random, (int) (r1.getRange().getLow().getValue()) + 1,
            (int) (r1.getRange().getHigh().getValue()) - 1);

        log.info("Trying to split range mapping for key range ({} - {}) at {}",
            r1.getRange().getLow().getValue(), r1.getRange().getHigh().getValue(), splitPoint);

        List<RangeMapping> rangeMappings = rsm.splitMapping(r1, splitPoint);
        assert 2 == rangeMappings.size();
      }
    } catch (ShardManagementException sme) {
      log.info("Exception caught: {}", sme.getMessage());
    }
  }

  /**
   * Merge ranges with locking.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void loadTestMergeRangesWithLock() {
    try {
      ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

      RangeShardMap<Integer> rsm = smm.getRangeShardMap(
          ShardMapManagerLoadTests.rangeShardMapName);
      assert rsm != null;

      List<RangeMapping> existingMappings = rsm
          .getMappings(new Range(MIN_MAPPING_POINT, MAX_MAPPING_POINT));

      //TODO:
      /*IQueryable<RangeMapping> qr = Queryable.AsQueryable(existingMappings);

      // Find pair of adjacent mappings.
      var test = from a in qr join b in qr on new {
        a.getRange().getHigh().getValue(), a.StoreMapping.StoreShard.Id, a.StoreMapping.Status
      } equals new {
        b.getRange().getLow().getValue(), b.StoreMapping.StoreShard.Id, b.StoreMapping.Status
      } select new {
        a, b
      } ;

      if (test.Count() > 0) {
        var t = test.First();

        log.info("Trying to merge range mapping for key range ({} - {}) and ({} - {})",
            t.a.getRange().getLow().getValue(), t.a.getRange().getHigh().getValue(),
            t.b.getRange().getLow().getValue(), t.b.getRange().getHigh().getValue());

        MappingLockToken mappingLockTokenLeft = MappingLockToken.create();
        rsm.lockMapping(t.a, mappingLockTokenLeft);

        MappingLockToken mappingLockTokenRight = MappingLockToken.create();
        rsm.lockMapping(t.b, mappingLockTokenLeft);

        RangeMapping rMerged = rsm
            .mergeMappings(t.a, t.b, mappingLockTokenLeft, mappingLockTokenRight);

        assert rMerged != null;

        MappingLockToken storeMappingLockToken = rsm.getMappingLockOwner(rMerged);
        assertEquals("Expected merged mapping lock id to equal left mapping id!",
            storeMappingLockToken, mappingLockTokenLeft);
        rsm.unlockMapping(rMerged, storeMappingLockToken);

        storeMappingLockToken = rsm.getMappingLockOwner(rMerged);
        assertEquals("Expected merged mapping lock id to equal default mapping id after unlock!",
            storeMappingLockToken, MappingLockToken.NoLock);
      }*/
    } catch (ShardManagementException sme) {
      log.info("Exception caught: {}", sme.getMessage());
    }
  }

  /**
   * Merge ranges without locking.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void loadTestMergeRangesNoLock() {
    try {
      ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

      RangeShardMap<Integer> rsm = smm.getRangeShardMap(
          ShardMapManagerLoadTests.rangeShardMapName);
      assert rsm != null;

      List<RangeMapping> existingMappings = rsm
          .getMappings(new Range(MIN_MAPPING_POINT, MAX_MAPPING_POINT));

      /*List<RangeMapping> qr = rsm.getMappings(existingMappings);

      // find pair of adjacent mappings.
        String test =  from a in qr join b in qr on new {
        a.getRange().getHigh().getValue(), a.StoreMapping.StoreShard.Id, a.StoreMapping.Status
      } equals new {
        b.getRange().getLow().getValue(), b.StoreMapping.StoreShard.Id, b.StoreMapping.Status
      } select new {
        a, b
      } ;

      if (test.c > 0) {
        var t = test.First();

        log.info("Trying to merge range mapping for key range ({} - {}) and ({} - {})",
            t.a.getRange().getLow().getValue(), t.a.getRange().getHigh().getValue(),
            t.b.getRange().getLow().getValue(), t.b.getRange().getHigh().getValue());

        RangeMapping rMerged = rsm.mergeMappings(t.a, t.b);
        assert rMerged != null;
      }*/
    } catch (ShardManagementException sme) {
      log.info("Exception caught: {}", sme.getMessage());
    }
  }

  /**
   * Add a shard to range shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void loadTestAddShardToRangeShardMap() {
    try {
      ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

      RangeShardMap<Integer> rsm = smm.getRangeShardMap(
          ShardMapManagerLoadTests.rangeShardMapName);
      assert rsm != null;

      addShardToShardMap(rsm);
    } catch (ShardManagementException sme) {
      log.info("Exception caught: {}", sme.getMessage());
    }
  }

  /**
   * Mark all shards as online in range shard map. If remove shard operation fails, it will leave
   * shards in offline state, this function will mark all such shards as online.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void loadTestMarkAllShardsAsOnlineInRangeShardMap() {
    try {
      ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

      RangeShardMap<Integer> rsm = smm.getRangeShardMap(
          ShardMapManagerLoadTests.rangeShardMapName);
      assert rsm != null;

      for (Shard s : rsm.getShards()) {
        if (s.getStatus().equals(ShardStatus.Offline)) {
          ShardUpdate tempVar = new ShardUpdate();
          tempVar.setStatus(ShardStatus.Online);
          rsm.updateShard(s, tempVar);
        }
      }
    } catch (ShardManagementException sme) {
      log.info("Exception caught: {}", sme.getMessage());
    }
  }

  /**
   * Remove a random shard from range shard map.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void loadTestRemoveShardFromRangeShardMap() {
    try {
      ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

      RangeShardMap<Integer> rsm = smm.getRangeShardMap(
          ShardMapManagerLoadTests.rangeShardMapName);
      assert rsm != null;

      List<Shard> existingShards = rsm.getShards();

      if (existingShards.isEmpty()) {
        return;
      }

      // If there is already a shard marked as offline, chose that one to delete.
      // This can happened if earlier remove operation was terminated for some reason
      // - ex. killing connections.
      Shard offlineShard = ListHelper
          .find(existingShards, e -> e.getStatus().equals(ShardStatus.Offline));

      if (offlineShard == null) {
        offlineShard = existingShards.get(random.nextInt(existingShards.size()));

        // First mark shard as offline so that other test threads will not add new mappings to it.
        ShardUpdate tempVar = new ShardUpdate();
        tempVar.setStatus(ShardStatus.Offline);
        offlineShard = rsm.updateShard(offlineShard, tempVar);
      }

      log.info("Trying to remove shard at location {}", offlineShard.getLocation());

      RangeMappingUpdate ru = new RangeMappingUpdate();
      ru.setStatus(MappingStatus.Offline);

      // Remove all mappings from this shard for given shard map.
      for (RangeMapping rm : rsm.getMappings(offlineShard)) {
        RangeMapping mappingToDelete = rsm.updateMapping(rm, ru);
        rsm.deleteMapping(mappingToDelete);
      }

      // get shard object again.
      Shard deleteShard = rsm.getShard(offlineShard.getLocation());

      // now remove shard.
      rsm.deleteShard(deleteShard);

      log.info("Removed shard at location {} from shard map {}", deleteShard.getLocation(), rsm);
    } catch (ShardManagementException sme) {
      log.info("Exception caught: {}", sme.getMessage());
    }
  }

  /**
   * Kill all connections for a random shard.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void loadTestKillLsmConnections() throws SQLException {
    String databaseName = null;
    try {
      ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(
          Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);
      do {
        List<ShardLocation> sl = smm.getDistinctShardLocations();
        if (sl.isEmpty()) {
          continue;
        }

        // Select a random database(shard) to kill connections
        databaseName = sl.get(random.nextInt(sl.size())).getDatabase();
      } while (false);
    } catch (ShardManagementException sme) {
      log.info("Exception caught: {}", sme.getMessage());
    }

    if (databaseName != null) {
      Connection conn;
      try {
        conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

        // kill all connections for given shard location
        try (Statement stmt = conn.createStatement()) {
          String query = String.format(KILL_CONNECTIONS_FOR_DATABASE_QUERY,
              Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
          stmt.execute(query);
        }
      } catch (SQLException e) {
        //  233: A transport-level error has occurred when receiving results from the server.
        //  (provider: Shared Memory Provider,
        //   error: 0 - No process is on the other end of the pipe.)
        // 6106: Process ID %d is not an active process ID.
        // 6107: Only user processes can be killed
        if ((e.getErrorCode() != 233) && (e.getErrorCode() != 6106) && (e.getErrorCode() != 6107)) {
          Assert.fail(String.format("error number %1$s with message %2$s", e.getErrorCode(),
              e.getMessage()));
        }
      }
    }
  }

  /**
   * Kill all connections to Shard Map Manager database.
   */
  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public final void loadTestKillGsmConnections() throws SQLException {
    // Clear all connection pools.
    Connection conn;
    try {
      conn = DriverManager.getConnection(Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING);

      // Create ShardMapManager database
      try (Statement stmt = conn.createStatement()) {
        String query = String.format(KILL_CONNECTIONS_FOR_DATABASE_QUERY,
            Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
        stmt.execute(query);
      }
    } catch (SQLException e) {
      //  233: A transport-level error has occurred when receiving results from the server.
      //  (provider: Shared Memory Provider,
      //   error: 0 - No process is on the other end of the pipe.)
      // 6106: Process ID %d is not an active process ID.
      // 6107: Only user processes can be killed
      if ((e.getErrorCode() != 233) && (e.getErrorCode() != 6106) && (e.getErrorCode() != 6107)) {
        Assert.fail(String.format("error number %1$s with message %2$s", e.getErrorCode(),
            e.getMessage()));
      }
    }
  }

  private int randomNextInt(Random random, int min, int max) {
    return random.nextInt(max + 1 - min) + min;
  }
}
