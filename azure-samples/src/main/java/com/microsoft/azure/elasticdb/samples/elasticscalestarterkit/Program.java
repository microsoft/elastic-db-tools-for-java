package com.microsoft.azure.elasticdb.samples.elasticscalestarterkit;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.base.RangeMapping;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.map.RangeShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;

import java.util.List;
import java.util.Scanner;

public class Program {
    private static final ConsoleColor EnabledColor = ConsoleColor.White; // color for items that are expected to succeed
    private static final ConsoleColor DisabledColor = ConsoleColor.DarkGray; // color for items that are expected to fail

    ///#region Program control flow
    /**
     * The shard map manager, or null if it does not exist.
     * It is recommended that you keep only one shard map manager instance in
     * memory per AppDomain so that the mapping cache is not duplicated.
     */
    private static ShardMapManager s_shardMapManager;

    public static void main() {
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
            PrintShardMapState();
            System.out.println();

            PrintMenu();
            System.out.println();

            continueLoop = GetMenuChoiceAndExecute();
            System.out.println();
        } while (continueLoop);
    }

    /**
     * Writes the shard map's state to the console.
     */
    private static void PrintShardMapState() {
        System.out.println("Current Shard Map state:");
        RangeShardMap<Integer> shardMap = TryGetShardMap();
        if (shardMap == null) {
            return;
        }

        // Get all shards
        List<Shard> allShards = shardMap.GetShards();

        // Get all mappings, grouped by the shard that they are on. We do this all in one go to minimise round trips.
        ILookup<Shard, RangeMapping<Integer>> mappingsGroupedByShard = shardMap.GetMappings().ToLookup(m -> m.Shard);

        if (allShards.Any()) {
            // The shard map contains some shards, so for each shard (sorted by database name)
            // write out the mappings for that shard
            // TODO: Convert the below LINQ queries to java
            for (Shard shard : shardMap.GetShards().OrderBy(s -> s.Location.Database)) {
                List<RangeMapping<Integer>> mappingsOnThisShard = mappingsGroupedByShard.getItem(shard);

                if (mappingsOnThisShard.Any()) {
                    String mappingsString = StringUtilsLocal.join(", ", mappingsOnThisShard.Select(m -> m.Value));
                    System.out.printf("\t%1$s contains key range %2$s" + "\r\n", shard.Location.Database, mappingsString);
                } else {
                    System.out.printf("\t%1$s contains no key ranges." + "\r\n", shard.Location.Database);
                }
            }
        } else {
            System.out.println("\tShard Map contains no shards");
        }
    }

    /**
     * Writes the program menu.
     */
    private static void PrintMenu() {
        ConsoleColor createSmmColor; // color for create shard map manger menu item
        ConsoleColor otherMenuItemColor; // color for other menu items
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
            ConsoleUtils.WriteWarning("Shard Map shardMapManager already exists");
            return;
        }

        // Create shard map manager database
        if (!SqlDatabaseUtils.DatabaseExists(Configuration.getShardMapManagerServerName(), Configuration.getShardMapManagerDatabaseName())) {
            SqlDatabaseUtils.CreateDatabase(Configuration.getShardMapManagerServerName(), Configuration.getShardMapManagerDatabaseName());
        }

        // Create shard map manager
        String shardMapManagerConnectionString = Configuration.GetConnectionString(Configuration.getShardMapManagerServerName(), Configuration.getShardMapManagerDatabaseName());

        s_shardMapManager = ShardManagementUtils.CreateOrGetShardMapManager(shardMapManagerConnectionString);

        // Create shard map
        RangeShardMap<Integer> shardMap = ShardManagementUtils.<Integer>CreateOrGetRangeShardMap(s_shardMapManager, Configuration.getShardMapName());

        // Create schema info so that the split-merge service can be used to move data in sharded tables
        // and reference tables.
        CreateSchemaInfo(shardMap.Name);

        // If there are no shards, add two shards: one for [0,100) and one for [100,+inf)
        if (!shardMap.GetShards().Any()) {
            CreateShardSample.CreateShard(shardMap, new Range<Integer>(0, 100));
            CreateShardSample.CreateShard(shardMap, new Range<Integer>(100, 200));
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
        RangeShardMap<Integer> shardMap = TryGetShardMap();
        if (shardMap != null) {
            // Here we assume that the ranges start at 0, are contiguous,
            // and are bounded (i.e. there is no range where HighIsMax == true)
            int currentMaxHighKey = shardMap.GetMappings().Max(m -> m.Value.High);
            int defaultNewHighKey = currentMaxHighKey + 100;

            System.out.printf("A new range with low key %1$s will be mapped to the new shard." + "\r\n", currentMaxHighKey);
            int newHighKey = ConsoleUtils.ReadIntegerInput(String.format("Enter the high key for the new range [default %1$s]: ", defaultNewHighKey), defaultNewHighKey, input -> input > currentMaxHighKey);

            Range<Integer> range = new Range<Integer>(currentMaxHighKey, newHighKey);

            System.out.println();
            System.out.printf("Creating shard for range %1$s" + "\r\n", range);
            CreateShardSample.CreateShard(shardMap, range);
        }
    }

    /**
     * Executes the Data-Dependent Routing sample.
     */
    private static void DataDepdendentRouting() {
        RangeShardMap<Integer> shardMap = TryGetShardMap();
        if (shardMap != null) {
            DataDependentRoutingSample.ExecuteDataDependentRoutingQuery(shardMap, Configuration.GetCredentialsConnectionString());
        }
    }

    /**
     * Executes the Multi-Shard Query sample.
     */
    private static void MultiShardQuery() {
        RangeShardMap<Integer> shardMap = TryGetShardMap();
        if (shardMap != null) {
            MultiShardQuerySample.ExecuteMultiShardQuery(shardMap, Configuration.GetCredentialsConnectionString());
        }
    }

    /**
     * Drops all shards and the shard map manager database (if it exists).
     */
    private static void DropAll() {
        RangeShardMap<Integer> shardMap = TryGetShardMap();
        if (shardMap != null) {
            // Drop shards
            for (Shard shard : shardMap.GetShards()) {
                SqlDatabaseUtils.DropDatabase(shard.Location.DataSource, shard.Location.Database);
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
     * Gets the shard map, if it exists. If it doesn't exist, writes out the reason and returns null.
     */
    private static RangeShardMap<Integer> TryGetShardMap() {
        if (s_shardMapManager == null) {
            ConsoleUtils.WriteWarning("Shard Map shardMapManager has not yet been created");
            return null;
        }

        RangeShardMap<Integer> shardMap = null;
        ReferenceObjectHelper<RangeShardMap<Integer>> tempRef_shardMap = new ReferenceObjectHelper<RangeShardMap<Integer>>(shardMap);
        boolean mapExists = s_shardMapManager.TryGetRangeShardMap(Configuration.getShardMapName(), tempRef_shardMap);
        shardMap = tempRef_shardMap.argValue;

        if (!mapExists) {
            ConsoleUtils.WriteWarning("Shard Map shardMapManager has been created, but the Shard Map has not been created");
            return null;
        }

        return shardMap;
    }

    ///#endregion
}
