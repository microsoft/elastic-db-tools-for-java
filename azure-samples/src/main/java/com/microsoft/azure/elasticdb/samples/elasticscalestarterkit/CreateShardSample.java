package com.microsoft.azure.elasticdb.samples.elasticscalestarterkit;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.*;
import com.microsoft.azure.elasticdb.shard.map.ListShardMap;
import com.microsoft.azure.elasticdb.shard.map.RangeShardMap;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class CreateShardSample {
    /**
     * Script file that will be executed to initialize a shard.
     */
    private static final String InitializeShardScriptFile = "InitializeShard.sql";
    /**
     * Format to use for creating range shard name. {0} is the number of shards that have already been created.
     */
    private static final String RangeShardNameFormat = "ElasticScaleStarterKit_RangeShard%s";
    /**
     * Format to use for creating list shard name. {0} is the number of shards that have already been created.
     */
    private static final String ListShardNameFormat = "ElasticScaleStarterKit_ListShard%s";

    /**
     * Creates a new shard (or uses an existing empty shard), adds it to the shard map,
     * and assigns it the specified range if possible.
     */
    public static void CreateShard(RangeShardMap<Integer> shardMap, Range<Integer> rangeForNewShard) {
        // Create a new shard, or get an existing empty shard (if a previous create partially succeeded).
        Shard shard = CreateOrGetEmptyShard(shardMap);

        // Create a mapping to that shard.
        RangeMapping<Integer> mappingForNewShard = shardMap.CreateRangeMapping(rangeForNewShard, shard);
        ConsoleUtils.WriteInfo("Mapped range %s to shard %s", mappingForNewShard.getValue().toString(), shard.getLocation().getDatabase());
    }

    /**
     * Creates a new shard (or uses an existing empty shard), adds it to the shard map,
     * and assigns it the specified range if possible.
     */
    public static void CreateShard(ListShardMap<Integer> shardMap, ArrayList<Integer> pointsForNewShard) {
        // Create a new shard, or get an existing empty shard (if a previous create partially succeeded).
        Shard shard = CreateOrGetEmptyShard(shardMap);

        // Create a mapping to that shard.
        for (int point : pointsForNewShard) {
            PointMapping<Integer> mappingForNewShard = shardMap.CreatePointMapping(point, shard);
            ConsoleUtils.WriteInfo("Mapped point %s to shard %s", mappingForNewShard.getValue().toString(), shard.getLocation().getDatabase());
        }
    }

    /**
     * Creates a new shard, or gets an existing empty shard (i.e. a shard that has no mappings).
     * The reason why an empty shard might exist is that it was created and initialized but we
     * failed to create a mapping to it.
     */
    private static Shard CreateOrGetEmptyShard(RangeShardMap<Integer> shardMap) {
        // Get an empty shard if one already exists, otherwise create a new one
        Shard shard = FindEmptyShard(shardMap);
        if (shard == null) {
            // No empty shard exists, so create one

            // Choose the shard name
            final String databaseName = String.format(RangeShardNameFormat, shardMap.GetShards().size());
            final String shardMapManagerServerName = Configuration.getShardMapManagerServerName();

            // Only create the database if it doesn't already exist. It might already exist if
            if (!SqlDatabaseUtils.DatabaseExists(shardMapManagerServerName, databaseName)) {
            // we tried to create it previously but hit a transient fault.
                SqlDatabaseUtils.CreateDatabase(shardMapManagerServerName, databaseName);
            }

            // Create schema and populate reference data on that database
            // The initialize script must be idempotent, in case it was already run on this database
            // and we failed to add it to the shard map previously
            SqlDatabaseUtils.ExecuteSqlScript(shardMapManagerServerName, databaseName, InitializeShardScriptFile);

            // Add it to the shard map
            ShardLocation shardLocation = new ShardLocation(shardMapManagerServerName, databaseName);
            shard = ShardManagementUtils.CreateOrGetShard(shardMap, shardLocation);
        }

        return shard;
    }

    /**
     * Creates a new shard, or gets an existing empty shard (i.e. a shard that has no mappings).
     * The reason why an empty shard might exist is that it was created and initialized but we
     * failed to create a mapping to it.
     */
    private static Shard CreateOrGetEmptyShard(ListShardMap<Integer> shardMap) {
        // Get an empty shard if one already exists, otherwise create a new one
        Shard shard = FindEmptyShard(shardMap);
        if (shard == null) {
            // No empty shard exists, so create one

            // Choose the shard name
            final String databaseName = String.format(ListShardNameFormat, shardMap.GetShards().size());
            final String shardMapManagerServerName = Configuration.getShardMapManagerServerName();

            // Only create the database if it doesn't already exist. It might already exist if
            if (!SqlDatabaseUtils.DatabaseExists(shardMapManagerServerName, databaseName)) {
            // we tried to create it previously but hit a transient fault.
                SqlDatabaseUtils.CreateDatabase(shardMapManagerServerName, databaseName);
            }

            // Create schema and populate reference data on that database
            // The initialize script must be idempotent, in case it was already run on this database
            // and we failed to add it to the shard map previously
            SqlDatabaseUtils.ExecuteSqlScript(shardMapManagerServerName, databaseName, InitializeShardScriptFile);

            // Add it to the shard map
            ShardLocation shardLocation = new ShardLocation(shardMapManagerServerName, databaseName);
            shard = ShardManagementUtils.CreateOrGetShard(shardMap, shardLocation);
        }

        return shard;
    }

    /**
     * Finds an existing empty shard, or returns null if none exist.
     */
    private static Shard FindEmptyShard(RangeShardMap<Integer> shardMap) {
        // Get all shards in the shard map
        List<Shard> allShards = shardMap.GetShards();

        // Get all mappings in the shard map
        List<RangeMapping<Integer>> allMappings = shardMap.GetMappings();

        // Determine which shards have mappings
        List<Shard> shardsWithMappings = allMappings.stream().map(RangeMapping::getShard).collect(Collectors.toCollection(ArrayList::new));

        // Get the first shard (ordered by name) that has no mappings, if it exists
        return allShards.stream()
                .sorted(Comparator.comparing(shard -> shard.getLocation().getDatabase()))
                .filter(s -> !shardsWithMappings.contains(s))
                .findFirst().orElse(null);
    }

    /**
     * Finds an existing empty shard, or returns null if none exist.
     */
    private static Shard FindEmptyShard(ListShardMap<Integer> shardMap) {
        // Get all shards in the shard map
        List<Shard> allShards = shardMap.GetShards();

        // Get all mappings in the shard map
        List<PointMapping<Integer>> allMappings = shardMap.GetMappings();

        // Determine which shards have mappings
        List<Shard> shardsWithMappings = allMappings.stream().map(PointMapping::getShard).collect(Collectors.toCollection(ArrayList::new));

        // Get the first shard (ordered by name) that has no mappings, if it exists
        return allShards.stream()
                .sorted(Comparator.comparing(shard -> shard.getLocation().getDatabase()))
                .filter(s -> !shardsWithMappings.contains(s))
                .findFirst().orElse(null);
    }
}