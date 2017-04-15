package com.microsoft.azure.elasticdb.samples.elasticscalestarterkit;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.PointMapping;
import com.microsoft.azure.elasticdb.shard.base.RangeMapping;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.map.ListShardMap;
import com.microsoft.azure.elasticdb.shard.map.RangeShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;

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
        optionOneColor = DisabledColor;
        otherOptionColor = DisabledColor;
        if (enableSecondOption) {
            optionOneColor = EnabledColor;
            if (s_shardMapManager != null) {
                otherOptionColor = EnabledColor;
            }
        }

        ConsoleUtils.WriteColor(EnabledColor, "1. Connect to Azure Portal");
        ConsoleUtils.WriteColor(optionOneColor, "2. Get Shard Map Manager");
        ConsoleUtils.WriteColor(otherOptionColor, "3. Get Range Shards and Mappings");
        ConsoleUtils.WriteColor(otherOptionColor, "4. Get List Shards and Mappings");
//        ConsoleUtils.WriteColor(otherOptionColor, "5. Drop Shard Map Manager Database and All Shards");
        ConsoleUtils.WriteColor(EnabledColor, "5. Exit");
    }

    /**
     * Gets the user's chosen menu item and executes it.
     *
     * @return true if the program should continue executing.
     */
    private static boolean GetMenuChoiceAndExecute() {
        while (true) {
            int inputValue = ConsoleUtils.ReadIntegerInput("Enter an option [1-5] and press ENTER: ");

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
                case 5: // Drop all
                    System.out.println();
//                    if (otherOptionColor.equals(EnabledColor)) {
//                        DropAll();
//                        enableSecondOption = false;
//                        s_shardMapManager = null;
//                    }
                    return true;
                case 6: // Exit
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
        Map<Shard, List<RangeMapping<Integer>>> mappingsGroupedByShard = rangeShardMap.GetMappings().stream()
                .collect(Collectors.groupingBy(RangeMapping::getShard));

        if (!mappingsGroupedByShard.isEmpty()) {
            // The shard map contains some shards, so for each shard (sorted by database name)
            // write out the mappings for that shard
            mappingsGroupedByShard.keySet().stream()
                    .sorted(Comparator.comparing(shard -> shard.getLocation().getDatabase()))
                    .forEach(shard -> {
                        List<RangeMapping<Integer>> mappingsOnThisShard = mappingsGroupedByShard.get(shard);

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
        Map<Shard, List<PointMapping<Integer>>> mappingsGroupedByShard = listShardMap.GetMappings().stream()
                .collect(Collectors.groupingBy(PointMapping::getShard));

        if (!mappingsGroupedByShard.isEmpty()) {
            // The shard map contains some shards, so for each shard (sorted by database name)
            // write out the mappings for that shard
            mappingsGroupedByShard.keySet().stream()
                    .sorted(Comparator.comparing(shard -> shard.getLocation().getDatabase()))
                    .forEach(shard -> {
                        List<PointMapping<Integer>> mappingsOnThisShard = mappingsGroupedByShard.get(shard);

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
