package com.microsoft.azure.elasticdb.samples.elasticscalestarterkit;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.*;
import com.microsoft.azure.elasticdb.shard.map.ListShardMap;
import com.microsoft.azure.elasticdb.shard.map.RangeShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.schema.ReferenceTableInfo;
import com.microsoft.azure.elasticdb.shard.schema.SchemaInfo;
import com.microsoft.azure.elasticdb.shard.schema.ShardedTableInfo;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;

import java.util.*;
import java.util.stream.Collectors;

public class Program {
    private static final String EnabledColor = ConsoleColor.Green; // color for items that are expected to succeed
    private static final String DisabledColor = ConsoleColor.DarkGray; // color for items that are expected to fail

    ///#region Program control flow
    /**
     * The shard map manager, or null if it does not exist.
     * It is recommended that you keep only one shard map manager instance in
     * memory per AppDomain so that the mapping cache is not duplicated.
     */
    private static ShardMapManager s_shardMapManager;

    public static void main(String[] args) {
        // Welcome screen
        System.out.println("***********************************************************");
        System.out.println("***    Welcome to Elastic Database Tools Starter Kit    ***");
        System.out.println("***********************************************************");
        System.out.println();
        // Verify that we can connect to the Sql Database that is specified in settings
        if (!SqlDatabaseUtils.TryConnectToSqlDatabase()) {
            // Connecting to the server failed - please update the settings

            // Give the user a chance to read the mesage
            System.out.println("Press ENTER to continue...");
            new Scanner(System.in).nextLine();

            // Exit
            return;
        }

        // Connection succeeded. Begin interactive loop
        MenuLoop();
    }

    /**
     * Main program loop.
     */
    private static void MenuLoop() {
        // Get the shard map manager, if it already exists.
        // It is recommended that you keep only one shard map manager instance in
        // memory per AppDomain so that the mapping cache is not duplicated.
        s_shardMapManager = ShardManagementUtils.TryGetShardMapManager(Configuration.getShardMapManagerServerName(), Configuration.getShardMapManagerDatabaseName());

        // Loop until the user chose "Exit".
        boolean continueLoop;
        do {
            PrintRangeShardMapState();
            PrintListShardMapState();
            System.out.println();

            PrintMenu();
            System.out.println();

            continueLoop = GetMenuChoiceAndExecute();
            System.out.println();
        } while (continueLoop);
    }

    /**
     * Writes the range shard map's state to the console.
     */
    private static void PrintRangeShardMapState() {
        System.out.println("Current Range Shard Map state:");
        RangeShardMap<Integer> rangeShardMap = TryGetRangeShardMap();
        if (rangeShardMap == null) {
            return;
        }

        // Get all mappings, grouped by the shard that they are on. We do this all in one go to minimise round trips.
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
                            String mappingsString = mappingsOnThisShard.stream().map(m -> m.getValue().toString()).collect(Collectors.joining(", "));
                            ConsoleUtils.WriteInfo("\t%1$s contains key range %2$s", shard.getLocation().getDatabase(), mappingsString);
                        } else {
                            ConsoleUtils.WriteInfo("\t%1$s contains no key ranges.", shard.getLocation().getDatabase());
                        }
                    });
        } else {
            ConsoleUtils.WriteInfo("\tRange Shard Map contains no shards");
        }
    }

    /**
     * Writes the list shard map's state to the console.
     */
    private static void PrintListShardMapState() {
        System.out.println("Current List Shard Map state:");
        ListShardMap<Integer> listShardMap = TryGetListShardMap();
        if (listShardMap == null) {
            return;
        }

        // Get all mappings, grouped by the shard that they are on. We do this all in one go to minimise round trips.
        Map<Shard, List<PointMapping>> mappingsGroupedByShard = listShardMap.GetMappings().stream()
                .collect(Collectors.groupingBy(PointMapping::getShard));

        if (!mappingsGroupedByShard.isEmpty()) {
            // The shard map contains some shards, so for each shard (sorted by database name)
            // write out the mappings for that shard
            mappingsGroupedByShard.keySet().stream()
                    .sorted(Comparator.comparing(shard -> shard.getLocation().getDatabase()))
                    .forEach(shard -> {
                        List<PointMapping> mappingsOnThisShard = mappingsGroupedByShard.get(shard);

                        if (mappingsOnThisShard != null && !mappingsOnThisShard.isEmpty()) {
                            String mappingsString = mappingsOnThisShard.stream().map(m -> m.getValue().toString()).collect(Collectors.joining(", "));
                            ConsoleUtils.WriteInfo("\t%1$s contains key %2$s", shard.getLocation().getDatabase(), mappingsString);
                        } else {
                            ConsoleUtils.WriteInfo("\t%1$s contains no keys.", shard.getLocation().getDatabase());
                        }
                    });
        } else {
            ConsoleUtils.WriteInfo("\tList Shard Map contains no shards");
        }
    }

    /**
     * Writes the program menu.
     */
    private static void PrintMenu() {
        String createSmmColor; // color for create shard map manger menu item
        String otherMenuItemColor; // color for other menu items
        if (s_shardMapManager == null) {
            createSmmColor = EnabledColor;
            otherMenuItemColor = DisabledColor;
        } else {
            createSmmColor = DisabledColor;
            otherMenuItemColor = EnabledColor;
        }

        ConsoleUtils.WriteColor(createSmmColor, "1. Create shard map manager, and add a couple shards");
        ConsoleUtils.WriteColor(otherMenuItemColor, "2. Add another shard");
        ConsoleUtils.WriteColor(otherMenuItemColor, "3. Insert sample rows using Data-Dependent Routing");
        ConsoleUtils.WriteColor(otherMenuItemColor, "4. Execute sample Multi-Shard Query");
        ConsoleUtils.WriteColor(otherMenuItemColor, "5. Drop shard map manager database and all shards");
        ConsoleUtils.WriteColor(EnabledColor, "6. Exit");
    }

    /**
     * Gets the user's chosen menu item and executes it.
     *
     * @return true if the program should continue executing.
     */
    private static boolean GetMenuChoiceAndExecute() {
        while (true) {
            int inputValue = ConsoleUtils.ReadIntegerInput("Enter an option [1-6] and press ENTER: ");

            switch (inputValue) {
                case 1: // Create shard map manager
                    System.out.println();
                    CreateShardMapManagerAndShard();
                    return true;
                case 2: // Add shard
                    System.out.println();
                    AddShard();
                    return true;
                case 3: // Data Dependent Routing
                    System.out.println();
                    DataDepdendentRouting();
                    return true;
                case 4: // Multi-Shard Query
                    System.out.println();
                    MultiShardQuery();
                    return true;
                case 5: // Drop all
                    System.out.println();
                    DropAll();
                    return true;
                case 6: // Exit
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
    private static void CreateShardMapManagerAndShard() {
        if (s_shardMapManager != null) {
            ConsoleUtils.WriteWarning("Shard Map shardMapManager already exists in memory");
        }

        String shardMapManagerConnectionString;
        // Create shard map manager database
        if (!SqlDatabaseUtils.DatabaseExists(Configuration.getShardMapManagerServerName(), Configuration.getShardMapManagerDatabaseName())) {
            shardMapManagerConnectionString = SqlDatabaseUtils.CreateDatabase(Configuration.getShardMapManagerServerName(), Configuration.getShardMapManagerDatabaseName());
        } else {
            shardMapManagerConnectionString = Configuration.GetConnectionString(Configuration.getShardMapManagerServerName(), Configuration.getShardMapManagerDatabaseName());
        }

        if (!StringUtilsLocal.isNullOrEmpty(shardMapManagerConnectionString)) {
            // Create shard map manager
            s_shardMapManager = ShardManagementUtils.CreateOrGetShardMapManager(shardMapManagerConnectionString);

            // Create shard map
            RangeShardMap<Integer> rangeShardMap = ShardManagementUtils.CreateOrGetRangeShardMap(s_shardMapManager
                , Configuration.getRangeShardMapName()
                , ShardKeyType.Int32);

            ListShardMap<Integer> listShardMap = ShardManagementUtils.CreateOrGetListShardMap(s_shardMapManager
                , Configuration.getListShardMapName()
                , ShardKeyType.Int32);

            // Create schema info so that the split-merge service can be used to move data in sharded tables
            // and reference tables.
            CreateSchemaInfo(rangeShardMap.getName());

            CreateSchemaInfo(listShardMap.getName());

            // If there are no shards, add two shards: one for [0,100) and one for [100,+inf)
            if (rangeShardMap.GetShards().isEmpty()) {
                CreateShardSample.CreateShard(rangeShardMap, new Range(0, 100));
                CreateShardSample.CreateShard(rangeShardMap, new Range(100, 200));
            }

            if (listShardMap.GetShards().isEmpty()) {
                ArrayList<Integer> list = new ArrayList<>();
                list.add(201);
                list.add(203);
                list.add(205);
                list.add(207);
                list.add(209);
                CreateShardSample.CreateShard(listShardMap, list);
                list = new ArrayList<>();
                list.add(202);
                list.add(204);
                list.add(206);
                list.add(208);
                list.add(210);
                CreateShardSample.CreateShard(listShardMap, list);
            }
        }
    }

    /**
     * Creates schema info for the schema defined in InitializeShard.sql.
     */
    private static void CreateSchemaInfo(String shardMapName) {
        // Create schema info
        SchemaInfo schemaInfo = new SchemaInfo();
        schemaInfo.Add(new ReferenceTableInfo("Regions"));
        schemaInfo.Add(new ReferenceTableInfo("Products"));
        schemaInfo.Add(new ShardedTableInfo("Customers", "CustomerId"));
        schemaInfo.Add(new ShardedTableInfo("Orders", "CustomerId"));

        // Register it with the shard map manager for the given shard map name
        s_shardMapManager.GetSchemaInfoCollection().Add(shardMapName, schemaInfo);
    }

    /**
     * Reads the user's choice of a split point, and creates a new shard with a mapping for the resulting range.
     */
    private static void AddShard() {
        RangeShardMap<Integer> rangeShardMap = TryGetRangeShardMap();
        if (rangeShardMap != null) {
            // Here we assume that the ranges start at 0, are contiguous,
            // and are bounded (i.e. there is no range where HighIsMax == true)
            int currentMaxHighKey = rangeShardMap.GetMappings().stream()
                    .mapToInt(m -> (Integer)m.getValue().getHigh())
                    .max()
                    .orElse(0);
            int defaultNewHighKey = currentMaxHighKey + 100;

            ConsoleUtils.WriteInfo("A new range with low key %1$s will be mapped to the new shard." + "\r\n", currentMaxHighKey);
            int newHighKey = ConsoleUtils.ReadIntegerInput(String.format("Enter the high key for the new range [default %1$s]: ", defaultNewHighKey), defaultNewHighKey, input -> input > currentMaxHighKey);

            Range range = new Range(currentMaxHighKey, newHighKey);

            ConsoleUtils.WriteInfo("");
            ConsoleUtils.WriteInfo("Creating shard for range %1$s" + "\r\n", range);
            CreateShardSample.CreateShard(rangeShardMap, range);
        }
    }

    /**
     * Executes the Data-Dependent Routing sample.
     */
    private static void DataDepdendentRouting() {
        RangeShardMap<Integer> rangeShardMap = TryGetRangeShardMap();
        if (rangeShardMap != null) {
            DataDependentRoutingSample.ExecuteDataDependentRoutingQuery(rangeShardMap, Configuration.GetCredentialsConnectionString());
        }
    }

    /**
     * Executes the Multi-Shard Query sample.
     */
    private static void MultiShardQuery() {
        RangeShardMap<Integer> rangeShardMap = TryGetRangeShardMap();
        if (rangeShardMap != null) {
            MultiShardQuerySample.ExecuteMultiShardQuery(rangeShardMap, Configuration.GetCredentialsConnectionString());
        }
    }

    /**
     * Drops all shards and the shard map manager database (if it exists).
     */
    private static void DropAll() {
        RangeShardMap<Integer> rangeShardMap = TryGetRangeShardMap();
        if (rangeShardMap != null) {
            // Drop shards
            for (Shard shard : rangeShardMap.GetShards()) {
                SqlDatabaseUtils.DropDatabase(shard.getLocation().getDataSource(), shard.getLocation().getDatabase());
            }
        }

        ListShardMap<Integer> listShardMap = TryGetListShardMap();
        if (listShardMap != null) {
            // Drop shards
            for (Shard shard : listShardMap.GetShards()) {
                SqlDatabaseUtils.DropDatabase(shard.getLocation().getDataSource(), shard.getLocation().getDatabase());
            }
        }

        if (SqlDatabaseUtils.DatabaseExists(Configuration.getShardMapManagerServerName(), Configuration.getShardMapManagerDatabaseName())) {
            // Drop shard map manager database
            SqlDatabaseUtils.DropDatabase(Configuration.getShardMapManagerServerName(), Configuration.getShardMapManagerDatabaseName());
        }

        // Since we just dropped the shard map manager database, this shardMapManager reference is now non-functional.
        // So set it to null so that the program knows that the shard map manager is gone.
        s_shardMapManager = null;
    }

    ///#endregion

    ///#region Shard map helper methods

    /**
     * Gets the range shard map, if it exists. If it doesn't exist, writes out the reason and returns null.
     */
    private static RangeShardMap<Integer> TryGetRangeShardMap() {
        if (s_shardMapManager == null) {
            ConsoleUtils.WriteWarning("Shard Map shardMapManager has not yet been created");
            return null;
        }

        RangeShardMap<Integer> rangeShardMap = s_shardMapManager.TryGetRangeShardMap(Configuration.getRangeShardMapName());

        if (rangeShardMap == null) {
            ConsoleUtils.WriteWarning("Shard Map shardMapManager has been created, but the Shard Map has not been created");
            return null;
        }

        return rangeShardMap;
    }

    /**
     * Gets the list shard map, if it exists. If it doesn't exist, writes out the reason and returns null.
     */
    private static ListShardMap<Integer> TryGetListShardMap() {
        if (s_shardMapManager == null) {
            ConsoleUtils.WriteWarning("Shard Map Manager has not yet been created");
            return null;
        }

        ListShardMap<Integer> listShardMap = s_shardMapManager.TryGetListShardMap(Configuration.getListShardMapName());

        if (listShardMap == null) {
            ConsoleUtils.WriteWarning("Shard Map Manager has been created, but the Shard Map has not been created");
            return null;
        }

        return listShardMap;
    }

    ///#endregion
}
