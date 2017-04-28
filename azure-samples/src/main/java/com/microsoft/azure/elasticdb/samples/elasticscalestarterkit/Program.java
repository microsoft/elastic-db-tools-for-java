package com.microsoft.azure.elasticdb.samples.elasticscalestarterkit;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.base.PointMapping;
import com.microsoft.azure.elasticdb.shard.base.Range;
import com.microsoft.azure.elasticdb.shard.base.RangeMapping;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.map.ListShardMap;
import com.microsoft.azure.elasticdb.shard.map.RangeShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.schema.ReferenceTableInfo;
import com.microsoft.azure.elasticdb.shard.schema.SchemaInfo;
import com.microsoft.azure.elasticdb.shard.schema.SchemaInfoCollection;
import com.microsoft.azure.elasticdb.shard.schema.ShardedTableInfo;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;

public class Program {

  // color for items that are expected to succeed
  private static final String EnabledColor = ConsoleColor.Green;
  // color for items that are expected to fail
  private static final String DisabledColor = ConsoleColor.DarkGray;

  ///#region Program control flow
  /**
   * The shard map manager, or null if it does not exist.
   * It is recommended that you keep only one shard map manager instance in
   * memory per AppDomain so that the mapping cache is not duplicated.
   */
  private static ShardMapManager s_shardMapManager;

  /**
   * Main execution method.
   *
   * @param args Command line arguments, if any.
   */
  public static void main(String[] args) {
    // Welcome screen
    System.out.println("***********************************************************");
    System.out.println("***    Welcome to Elastic Database Tools Starter Kit    ***");
    System.out.println("***********************************************************");
    System.out.println();
    // Verify that we can connect to the Sql Database that is specified in settings
    if (!SqlDatabaseUtils.tryConnectToSqlDatabase()) {
      // Connecting to the server failed - please update the settings

      // Give the user a chance to read the mesage
      System.out.println("Press ENTER to continue...");
      new Scanner(System.in).nextLine();

      // Exit
      return;
    }

    // Connection succeeded. Begin interactive loop
    menuLoop();
  }

  /**
   * Main program loop.
   */
  private static void menuLoop() {
    // Get the shard map manager, if it already exists.
    // It is recommended that you keep only one shard map manager instance in
    // memory per AppDomain so that the mapping cache is not duplicated.
    s_shardMapManager = ShardManagementUtils
        .tryGetShardMapManager(Configuration.getShardMapManagerServerName(),
            Configuration.getShardMapManagerDatabaseName());

    // Loop until the user chose "Exit".
    boolean continueLoop;
    do {
      printRangeShardMapState();
      printListShardMapState();
      System.out.println();

      printMenu();
      System.out.println();

      continueLoop = getMenuChoiceAndExecute();
      System.out.println();
    } while (continueLoop);
  }

  /**
   * Writes the range shard map's state to the console.
   */
  private static void printRangeShardMapState() {
    System.out.println("Current Range Shard Map state:");
    RangeShardMap<Integer> rangeShardMap = tryGetRangeShardMap();
    if (rangeShardMap == null) {
      return;
    }

    // Get all mappings, grouped by the shard that they are on.
    // We do this all in one go to minimise round trips.
    Map<Shard, List<RangeMapping>> mappingsGroupedByShard = rangeShardMap.GetMappings().stream()
        .collect(Collectors.groupingBy(RangeMapping::getShard));

    if (!mappingsGroupedByShard.isEmpty()) {
      // The shard map contains some shards, so for each shard (sorted by database name)
      // write out the mappings for that shard
      mappingsGroupedByShard.keySet().stream()
          .sorted(Comparator.comparing(shard -> shard.getLocation().getDatabase()))
          .forEach(shard -> {
            List<RangeMapping> mappingsOnThisShard = mappingsGroupedByShard.get(shard);

            if (mappingsOnThisShard != null && !mappingsOnThisShard.isEmpty()) {
              String mappingsString = mappingsOnThisShard.stream().map(m -> m.getValue().toString())
                  .collect(Collectors.joining(", "));
              ConsoleUtils
                  .writeInfo("\t%1$s contains key range %2$s", shard.getLocation().getDatabase(),
                      mappingsString);
            } else {
              ConsoleUtils
                  .writeInfo("\t%1$s contains no key ranges.", shard.getLocation().getDatabase());
            }
          });
    } else {
      ConsoleUtils.writeInfo("\tRange Shard Map contains no shards");
    }
  }

  /**
   * Writes the list shard map's state to the console.
   */
  private static void printListShardMapState() {
    System.out.println("Current List Shard Map state:");
    ListShardMap<Integer> listShardMap = tryGetListShardMap();
    if (listShardMap == null) {
      return;
    }

    // Get all mappings, grouped by the shard that they are on.
    // We do this all in one go to minimise round trips.
    Map<String, List<PointMapping>> mappingsGroupedByShard = listShardMap.GetMappings().stream()
        .collect(Collectors.groupingBy(map -> map.getShard().getLocation().getDatabase()));

    if (!mappingsGroupedByShard.isEmpty()) {
      // The shard map contains some shards, so for each shard (sorted by database name)
      // write out the mappings for that shard
      mappingsGroupedByShard.keySet().stream().sorted()
          .forEach(shard -> {
            List<PointMapping> mappingsOnThisShard = mappingsGroupedByShard.get(shard);

            if (mappingsOnThisShard != null && !mappingsOnThisShard.isEmpty()) {
              String mappingsString = mappingsOnThisShard.stream()
                  .map(m -> m.getValue().toString()).collect(Collectors.joining(", "));
              ConsoleUtils.writeInfo("\t%1$s contains key %2$s", shard, mappingsString);
            } else {
              ConsoleUtils.writeInfo("\t%1$s contains no keys.", shard);
            }
          });
    } else {
      ConsoleUtils.writeInfo("\tList Shard Map contains no shards");
    }
  }

  /**
   * Writes the program menu.
   */
  private static void printMenu() {
    String createSmmColor; // color for create shard map manger menu item
    String otherMenuItemColor; // color for other menu items
    if (s_shardMapManager == null) {
      createSmmColor = EnabledColor;
      otherMenuItemColor = DisabledColor;
    } else {
      createSmmColor = DisabledColor;
      otherMenuItemColor = EnabledColor;
    }

    ConsoleUtils.writeColor(createSmmColor, "1. Create shard map manager, and add a couple shards");
    ConsoleUtils.writeColor(otherMenuItemColor, "2. Add another shard");
    ConsoleUtils
        .writeColor(otherMenuItemColor, "3. Insert sample rows using Data-Dependent Routing");
    ConsoleUtils.writeColor(otherMenuItemColor, "4. Execute sample Multi-Shard Query");
    ConsoleUtils
        .writeColor(otherMenuItemColor, "5. Drop shard map manager database and all shards");
    ConsoleUtils.writeColor(EnabledColor, "6. Exit");
  }

  /**
   * Gets the user's chosen menu item and executes it.
   *
   * @return true if the program should continue executing.
   */
  private static boolean getMenuChoiceAndExecute() {
    while (true) {
      int inputValue = ConsoleUtils.readIntegerInput("Enter an option [1-6] and press ENTER: ");

      switch (inputValue) {
        case 1: // Create shard map manager
          System.out.println();
          createShardMapManagerAndShard();
          return true;
        case 2: // Add shard
          System.out.println();
          addShard();
          return true;
        case 3: // Data Dependent Routing
          System.out.println();
          dataDependentRouting();
          return true;
        case 4: // Multi-Shard Query
          System.out.println();
          multiShardQuery();
          return true;
        case 5: // Drop all
          System.out.println();
          dropAll();
          return true;
        case 6: // Exit
        default:
          return false;
      }
    }
  }

  ///#endregion

  ///#region Menu item implementations

  /**
   * Creates a shard map manager, creates a shard map, and creates a shard
   * with a mapping for the full range of 32-bit integers.
   */
  private static void createShardMapManagerAndShard() {
    if (s_shardMapManager != null) {
      ConsoleUtils.writeWarning("Shard Map Manager %s already exists in memory");
    }

    String shardMapManagerConnectionString;
    // Create shard map manager database
    if (!SqlDatabaseUtils.databaseExists(Configuration.getShardMapManagerServerName(),
        Configuration.getShardMapManagerDatabaseName())) {
      shardMapManagerConnectionString = SqlDatabaseUtils
          .createDatabase(Configuration.getShardMapManagerServerName(),
              Configuration.getShardMapManagerDatabaseName());
    } else {
      shardMapManagerConnectionString = Configuration
          .getConnectionString(Configuration.getShardMapManagerServerName(),
              Configuration.getShardMapManagerDatabaseName());
    }

    if (!StringUtilsLocal.isNullOrEmpty(shardMapManagerConnectionString)) {
      // Create shard map manager
      s_shardMapManager = ShardManagementUtils
          .createOrGetShardMapManager(shardMapManagerConnectionString);

      // Create shard map
      RangeShardMap<Integer> rangeShardMap = ShardManagementUtils
          .createOrGetRangeShardMap(s_shardMapManager,
              Configuration.getRangeShardMapName(),
              ShardKeyType.Int32);

      ListShardMap<Integer> listShardMap = ShardManagementUtils
          .createOrGetListShardMap(s_shardMapManager,
              Configuration.getListShardMapName(),
              ShardKeyType.Int32);

      // Create schema info so that the split-merge service can be used to move data
      // in sharded tables and reference tables.
      createSchemaInfo(rangeShardMap.getName());

      createSchemaInfo(listShardMap.getName());

      // If there are no shards, add two shards: one for [0,100) and one for [100,+inf)
      if (rangeShardMap.getShards().isEmpty()) {
        CreateShardSample.createShard(rangeShardMap, new Range(0, 100));
        CreateShardSample.createShard(rangeShardMap, new Range(100, 200));
      }

      if (listShardMap.getShards().isEmpty()) {
        ArrayList<Integer> list = new ArrayList<>();
        list.add(201);
        list.add(202);
        CreateShardSample.createShard(listShardMap, list);
        list = new ArrayList<>();
        list.add(203);
        list.add(204);
        CreateShardSample.createShard(listShardMap, list);
      }
    }
  }

  /**
   * Creates schema info for the schema defined in InitializeShard.sql.
   */
  private static void createSchemaInfo(String shardMapName) {
    // Create schema info
    SchemaInfo schemaInfo = new SchemaInfo();
    schemaInfo.Add(new ReferenceTableInfo("Regions"));
    schemaInfo.Add(new ReferenceTableInfo("Products"));
    schemaInfo.Add(new ShardedTableInfo("Customers", "CustomerId"));
    schemaInfo.Add(new ShardedTableInfo("Orders", "CustomerId"));

    SchemaInfoCollection schemaInfoCollection = s_shardMapManager.getSchemaInfoCollection();
    ReferenceObjectHelper<SchemaInfo> refSchemaInfo = new ReferenceObjectHelper<>(null);
    schemaInfoCollection.TryGet(shardMapName, refSchemaInfo);

    if (refSchemaInfo.argValue == null) {
      // Register it with the shard map manager for the given shard map name
      schemaInfoCollection.Add(shardMapName, schemaInfo);
    } else {
      ConsoleUtils.writeInfo("Schema Information already exists for " + shardMapName);
    }
  }


  private static void addShard() {
    int shardType = ConsoleUtils
        .readIntegerInput(String.format("1. Range Shard\r\n2. List Shard\r\n"
                + "Select the type of shard you want to create: "), 0,
            (input -> input == 1 || input == 2));

    if (shardType == 1) {
      addRangeShard();
    } else if (shardType == 2) {
      addListShard();
    }
  }

  /**
   * Executes the Data-Dependent Routing sample.
   */
  private static void dataDependentRouting() {
    RangeShardMap<Integer> rangeShardMap = tryGetRangeShardMap();
    if (rangeShardMap != null) {
      DataDependentRoutingSample.executeDataDependentRoutingQuery(rangeShardMap,
          Configuration.getCredentialsConnectionString());
    }

    ListShardMap<Integer> listShardMap = tryGetListShardMap();
    if (listShardMap != null) {
      DataDependentRoutingSample.executeDataDependentRoutingQuery(listShardMap,
          Configuration.getCredentialsConnectionString());
    }
  }

  /**
   * Executes the Multi-Shard Query sample.
   */
  private static void multiShardQuery() {
    RangeShardMap<Integer> rangeShardMap = tryGetRangeShardMap();
    if (rangeShardMap != null) {
      MultiShardQuerySample.executeMultiShardQuery(rangeShardMap,
          Configuration.getCredentialsConnectionString());
    }
  }

  /**
   * Drops all shards and the shard map manager database (if it exists).
   */
  private static void dropAll() {
    RangeShardMap<Integer> rangeShardMap = tryGetRangeShardMap();
    if (rangeShardMap != null) {
      // Drop shards
      for (Shard shard : rangeShardMap.getShards()) {
        SqlDatabaseUtils
            .dropDatabase(shard.getLocation().getDataSource(), shard.getLocation().getDatabase());
      }
    }

    ListShardMap<Integer> listShardMap = tryGetListShardMap();
    if (listShardMap != null) {
      // Drop shards
      for (Shard shard : listShardMap.getShards()) {
        SqlDatabaseUtils
            .dropDatabase(shard.getLocation().getDataSource(), shard.getLocation().getDatabase());
      }
    }

    if (SqlDatabaseUtils.databaseExists(Configuration.getShardMapManagerServerName(),
        Configuration.getShardMapManagerDatabaseName())) {
      // Drop shard map manager database
      SqlDatabaseUtils.dropDatabase(Configuration.getShardMapManagerServerName(),
          Configuration.getShardMapManagerDatabaseName());
    }

    // Since we just dropped the shard map manager database, this shardMapManager reference is now
    // non-functional. So set it to null so that the program knows the shard map manager is gone.
    s_shardMapManager = null;
  }

  ///#endregion

  ///#region Shard map helper methods

  /**
   * Reads the user's choice of a split point, and creates a new shard with a mapping for the
   * resulting range.
   */
  private static void addRangeShard() {
    RangeShardMap<Integer> rangeShardMap = tryGetRangeShardMap();
    if (rangeShardMap != null) {
      // Here we assume that the ranges start at 0, are contiguous,
      // and are bounded (i.e. there is no range where HighIsMax == true)
      int currentMaxHighKey = rangeShardMap.GetMappings().stream()
          .mapToInt(m -> (Integer) m.getValue().getHigh())
          .max()
          .orElse(0);
      int defaultNewHighKey = currentMaxHighKey + 100;

      ConsoleUtils.writeInfo("A new range with low key %1$s will be mapped to the new shard."
          + "\r\n", currentMaxHighKey);
      int newHighKey = ConsoleUtils.readIntegerInput(
          String.format("Enter the high key for the new range [default %1$s]: ", defaultNewHighKey),
          defaultNewHighKey, input -> input > currentMaxHighKey);

      Range range = new Range(currentMaxHighKey, newHighKey);

      ConsoleUtils.writeInfo("");
      ConsoleUtils.writeInfo("Creating shard for range %1$s" + "\r\n", range);
      CreateShardSample.createShard(rangeShardMap, range);
    }
  }

  /**
   * Reads the user's choice of a split point, and creates a new shard with a mapping for the
   * resulting range.
   */
  private static void addListShard() {
    ListShardMap<Integer> listShardMap = tryGetListShardMap();
    if (listShardMap != null) {
      // Here we assume that the point start at 0, are contiguous, and are bounded
      List<Integer> currentKeys = listShardMap.GetMappings().stream()
          .map(m -> (Integer) m.getValue()).sorted().collect(Collectors.toList());

      ArrayList<Integer> newKeys = new ArrayList<>();
      int newKey = 0;
      ConsoleUtils.writeInfo("");
      do {
        newKey = ConsoleUtils.readIntegerInput("Enter the points to be mapped to the new shard."
            + " To stop press enter: ", 0, input -> !currentKeys.contains(input));
        if (newKey > 0) {
          newKeys.add(newKey);
        }
      } while (newKeys.size() > 0 && newKey != 0);

      ConsoleUtils.writeInfo("");
      if (newKeys.size() > 0) {
        ConsoleUtils.writeInfo("Creating shard for given list of points");
        CreateShardSample.createShard(listShardMap, newKeys);
      } else {
        ConsoleUtils.writeInfo("No new points to map.");
      }
    }
  }

  /**
   * Gets the range shard map, if it exists. If it doesn't exist, writes out the reason and returns
   * null.
   */
  private static RangeShardMap<Integer> tryGetRangeShardMap() {
    if (s_shardMapManager == null) {
      ConsoleUtils.writeWarning("Shard Map Manager has not yet been created");
      return null;
    }

    RangeShardMap<Integer> rangeShardMap = s_shardMapManager
        .tryGetRangeShardMap(Configuration.getRangeShardMapName());

    if (rangeShardMap == null) {
      ConsoleUtils.writeWarning(
          "Shard Map Manager has been created, but the Shard Map has not been created");
      return null;
    }

    return rangeShardMap;
  }

  /**
   * Gets the list shard map, if it exists. If it doesn't exist, writes out the reason and returns
   * null.
   */
  private static ListShardMap<Integer> tryGetListShardMap() {
    if (s_shardMapManager == null) {
      ConsoleUtils.writeWarning("Shard Map Manager has not yet been created");
      return null;
    }

    ListShardMap<Integer> listShardMap = s_shardMapManager
        .tryGetListShardMap(Configuration.getListShardMapName());

    if (listShardMap == null) {
      ConsoleUtils.writeWarning(
          "Shard Map Manager has been created, but the Shard Map has not been created");
      return null;
    }

    return listShardMap;
  }

  ///#endregion
}
