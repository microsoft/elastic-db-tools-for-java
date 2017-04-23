package com.microsoft.azure.elasticdb.samples.elasticscalestarterkit;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.base.*;
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
import java.util.stream.Collectors;

//TODO: To be removed after Demo
public class ProgramPhase1 {
    private static final String EnabledColor = ConsoleColor.Green; // color for items that are expected to succeed
    private static final String DisabledColor = ConsoleColor.DarkGray; // color for items that are expected to fail

    /**
     * The shard map manager, or null if it does not exist.
     * It is recommended that you keep only one shard map manager instance in
     * memory per AppDomain so that the mapping cache is not duplicated.
     */
    private static ShardMapManager s_shardMapManager;
    private static boolean enableSecondOption;
    private static String createColor = DisabledColor;
    private static String optionOneColor = DisabledColor;
    private static String otherOptionColor = DisabledColor;

    public static void main(String[] args) {
        // Welcome screen
        System.out.println("***********************************************************");
        System.out.println("***    Welcome to Elastic Database Tools Starter Kit    ***");
        System.out.println("******************    Phase 1 - Demo    *******************");
        System.out.println("***********************************************************");
        System.out.println();

        MenuLoop();
    }

    /**
     * Main program loop.
     */
    private static void MenuLoop() {
        // Loop until the user chose "Exit".
        boolean continueLoop;
        do {
            PrintMenu();
            System.out.println();

            continueLoop = GetMenuChoiceAndExecute();
            System.out.println();
        } while (continueLoop);
    }

    /**
     * Writes the program menu.
     */
    private static void PrintMenu() {
        createColor = DisabledColor;
        optionOneColor = DisabledColor;
        otherOptionColor = DisabledColor;
        if (enableSecondOption) {
            optionOneColor = EnabledColor;
            if (s_shardMapManager != null) {
                otherOptionColor = EnabledColor;
            } else {
                createColor = EnabledColor;
            }
        }

        ConsoleUtils.WriteColor(EnabledColor, "1. Connect to Azure Portal");
        ConsoleUtils.WriteColor(optionOneColor, "2. Get Shard Map Manager");
        ConsoleUtils.WriteColor(otherOptionColor, "3. Get Range Shards and Mappings");
        ConsoleUtils.WriteColor(otherOptionColor, "4. Get List Shards and Mappings");
        ConsoleUtils.WriteColor(otherOptionColor, "5. Add Shard");
        ConsoleUtils.WriteColor(otherOptionColor, "6. Drop Shard Map Manager Database and All Shards");
        ConsoleUtils.WriteColor(createColor, "7. Create Shard Map Manager and Add couple of Shards");
        ConsoleUtils.WriteColor(EnabledColor, "8. Exit");
    }

    /**
     * Gets the user's chosen menu item and executes it.
     *
     * @return true if the program should continue executing.
     */
    private static boolean GetMenuChoiceAndExecute() {
        while (true) {
            int inputValue = ConsoleUtils.ReadIntegerInput("Enter an option [1-8] and press ENTER: ");

            switch (inputValue) {
                case 1:
                    System.out.println();
                    // Verify that we can connect to the Sql Database that is specified in settings
                    enableSecondOption = SqlDatabaseUtils.TryConnectToSqlDatabase();
                    return true;
                case 2:
                    System.out.println();
                    String shardMapManagerDatabaseName = Configuration.getShardMapManagerDatabaseName();
                    String shardMapManagerServerName = Configuration.getShardMapManagerServerName();
                    if (optionOneColor.equals(EnabledColor)) {
                        s_shardMapManager = ShardManagementUtils.TryGetShardMapManager(shardMapManagerServerName, shardMapManagerDatabaseName);
                    } else if (enableSecondOption) {
                        ConsoleUtils.WriteInfo("%s reloaded successfully...", shardMapManagerDatabaseName);
                    }
                    return true;
                case 3:
                    System.out.println();
                    if (otherOptionColor.equals(EnabledColor)) {
                        PrintRangeShardMapState();
                    }
                    return true;
                case 4:
                    System.out.println();
                    if (otherOptionColor.equals(EnabledColor)) {
                        PrintListShardMapState();
                    }
                    return true;
                case 5:
                    System.out.println();
                    if (otherOptionColor.equals(EnabledColor)) {
                        AddShard();
                    }
                    return true;
                case 6: // Drop all
                    System.out.println();
                    if (otherOptionColor.equals(EnabledColor)) {
                        DropAll();
                        enableSecondOption = false;
                        s_shardMapManager = null;
                    }
                    return true;
                case 7: // Create SMM and Shards
                    System.out.println();
                    if (createColor.equals(EnabledColor)) {
                        CreateShardMapManagerAndShard();
                        enableSecondOption = true;
                        createColor = EnabledColor;
                    }
                    return true;
                case 8: // Exit
                    return false;
            }
        }
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
                            ConsoleUtils.WriteInfo("\t%1$s contains key %2$s", shard, mappingsString);
                        } else {
                            ConsoleUtils.WriteInfo("\t%1$s contains no keys.", shard);
                        }
                    });
        } else {
            ConsoleUtils.WriteInfo("\tList Shard Map contains no shards");
        }
    }

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
                list.add(202);
                CreateShardSample.CreateShard(listShardMap, list);
                list = new ArrayList<>();
                list.add(203);
                list.add(204);
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

        SchemaInfoCollection schemaInfoCollection = s_shardMapManager.GetSchemaInfoCollection();
        ReferenceObjectHelper<SchemaInfo> ref_schemaInfo = new ReferenceObjectHelper<>(null);
        schemaInfoCollection.TryGet(shardMapName, ref_schemaInfo);

        if (ref_schemaInfo.argValue == null) {
            // Register it with the shard map manager for the given shard map name
            schemaInfoCollection.Add(shardMapName, schemaInfo);
        } else {
            ConsoleUtils.WriteInfo("Schema Information already exists for " + shardMapName);
        }
    }

    /**
     * Reads user's choice and decide which type of shard to add
     */
    private static void AddShard() {
        int shardType = ConsoleUtils.ReadIntegerInput(String.format("1. Range Shard\r\n2. List Shard\r\n"
                + "Select the type of shard you want to create: "), 0, (input -> input == 1 || input == 2));

        if (shardType == 1) {
            AddRangeShard();
        } else if (shardType == 2) {
            AddListShard();
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

    /**
     * Reads the user's choice of a split point, and creates a new shard with a mapping for the resulting range.
     */
    private static void AddRangeShard() {
        RangeShardMap<Integer> rangeShardMap = TryGetRangeShardMap();
        if (rangeShardMap != null) {
            // Here we assume that the ranges start at 0, are contiguous,
            // and are bounded (i.e. there is no range where HighIsMax == true)
            int currentMaxHighKey = rangeShardMap.GetMappings().stream()
                    .mapToInt(m -> (Integer) m.getValue().getHigh())
                    .max()
                    .orElse(0);
            int defaultNewHighKey = currentMaxHighKey + 100;

            ConsoleUtils.WriteInfo("A new range with low key %1$s will be mapped to the new shard."
                    + "\r\n", currentMaxHighKey);
            int newHighKey = ConsoleUtils.ReadIntegerInput(String.format("Enter the high key for the new range [default %1$s]: ", defaultNewHighKey), defaultNewHighKey, input -> input > currentMaxHighKey);

            Range range = new Range(currentMaxHighKey, newHighKey);

            ConsoleUtils.WriteInfo("");
            ConsoleUtils.WriteInfo("Creating shard for range %1$s" + "\r\n", range);
            CreateShardSample.CreateShard(rangeShardMap, range);
        }
    }

    /**
     * Reads the user's choice of a split point, and creates a new shard with a mapping for the resulting range.
     */
    private static void AddListShard() {
        ListShardMap<Integer> listShardMap = TryGetListShardMap();
        if (listShardMap != null) {
            // Here we assume that the point start at 0, are contiguous, and are bounded
            List<Integer> currentKeys = listShardMap.GetMappings().stream()
                    .map(m -> (Integer) m.getValue()).sorted().collect(Collectors.toList());

            ArrayList<Integer> newKeys = new ArrayList<>();
            int newKey = 0;
            ConsoleUtils.WriteInfo("");
            do {
                newKey = ConsoleUtils.ReadIntegerInput("Enter the points to be mapped to the new shard."
                        + " To stop press enter: ", 0, input -> !currentKeys.contains(input));
                if (newKey > 0) {
                    newKeys.add(newKey);
                }
            } while (newKeys.size() > 0 && newKey != 0);

            ConsoleUtils.WriteInfo("");
            if (newKeys.size() > 0) {
                ConsoleUtils.WriteInfo("Creating shard for given list of points");
                CreateShardSample.CreateShard(listShardMap, newKeys);
            } else {
                ConsoleUtils.WriteInfo("No new points to map.");
            }
        }
    }

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
}
