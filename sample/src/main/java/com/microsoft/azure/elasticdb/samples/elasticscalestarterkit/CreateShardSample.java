package com.microsoft.azure.elasticdb.samples.elasticscalestarterkit;

/*
 * Copyright (c) Microsoft. All rights reserved. Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

import com.google.common.collect.Iterables;
import com.microsoft.azure.elasticdb.shard.base.PointMapping;
import com.microsoft.azure.elasticdb.shard.base.Range;
import com.microsoft.azure.elasticdb.shard.base.RangeMapping;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.SqlProtocol;
import com.microsoft.azure.elasticdb.shard.map.ListShardMap;
import com.microsoft.azure.elasticdb.shard.map.RangeShardMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

class CreateShardSample {

    /**
     * Get the following properties from resource file: INITIAL_SHARD_SCRIPT - Script file that will be executed to initialize a shard.
     * RANGE_SHARD_NAME_FORMAT - Format to use for creating range shard name. LIST_SHARD_NAME_FORMAT - Format to use for creating list shard name.
     */
    private static Properties properties = Configuration.loadProperties();

    /**
     * Creates a new shard (or uses an existing empty shard), adds it to the shard map, and assigns it the specified range if possible.
     */
    static void createShard(RangeShardMap<Integer> shardMap,
            Range rangeForNewShard) {
        // Create a new shard, or get an existing empty shard (if a previous create partially succeeded)
        Shard shard = createOrGetEmptyShard(shardMap);

        // Create a mapping to that shard.
        RangeMapping mappingForNewShard = shardMap.createRangeMapping(rangeForNewShard, shard);
        ConsoleUtils.writeInfo("Mapped range %s to shard %s", mappingForNewShard.getValue().toString(), shard.getLocation().getDatabase());
    }

    /**
     * Creates a new shard (or uses an existing empty shard), adds it to the shard map, and assigns it the specified range if possible.
     */
    static void createShard(ListShardMap<Integer> shardMap,
            ArrayList<Integer> pointsForNewShard) {
        // Create a new shard, or get an existing empty shard (if a previous create partially succeeded)
        Shard shard = createOrGetEmptyShard(shardMap);

        // Create a mapping to that shard.
        for (int point : pointsForNewShard) {
            PointMapping mappingForNewShard = shardMap.createPointMapping(point, shard);
            ConsoleUtils.writeInfo("Mapped point %s to shard %s", mappingForNewShard.getKey().toString(), shard.getLocation().getDatabase());
        }
    }

    /**
     * Creates a new shard, or gets an existing empty shard (i.e. a shard that has no mappings). The reason why an empty shard might exist is that it
     * was created and initialized but we failed to create a mapping to it.
     */
    private static Shard createOrGetEmptyShard(RangeShardMap<Integer> shardMap) {
        // Get an empty shard if one already exists, otherwise create a new one
        Shard shard = findEmptyShard(shardMap);
        if (shard == null) {
            // No empty shard exists, so create one

            // Choose the shard name
            final String databaseName = String.format(properties.getProperty("RANGE_SHARD_NAME_FORMAT"), shardMap.getShards().size());
            final String shardMapManagerServerName = Configuration.getShardMapManagerServerName();

            // Only create the database if it doesn't already exist. It might already exist if
            if (!SqlDatabaseUtils.databaseExists(shardMapManagerServerName, databaseName)) {
                // we tried to create it previously but hit a transient fault.
                SqlDatabaseUtils.createDatabase(shardMapManagerServerName, databaseName);
            }

            // Create schema and populate reference data on that database
            // The initialize script must be idempotent, in case it was already run on this database
            // and we failed to add it to the shard map previously
            SqlDatabaseUtils.executeSqlScript(shardMapManagerServerName, databaseName, properties.getProperty("INITIAL_SHARD_SCRIPT"));

            // Add it to the shard map
            ShardLocation shardLocation = new ShardLocation(shardMapManagerServerName, databaseName, SqlProtocol.Tcp, 1433);
            shard = ShardManagementUtils.createOrGetShard(shardMap, shardLocation);
        }

        return shard;
    }

    /**
     * Creates a new shard, or gets an existing empty shard (i.e. a shard that has no mappings). The reason why an empty shard might exist is that it
     * was created and initialized but we failed to create a mapping to it.
     */
    private static Shard createOrGetEmptyShard(ListShardMap<Integer> shardMap) {
        // Get an empty shard if one already exists, otherwise create a new one
        Shard shard = findEmptyShard(shardMap);
        if (shard == null) {
            // No empty shard exists, so create one

            // Choose the shard name
            final String databaseName = String.format(properties.getProperty("LIST_SHARD_NAME_FORMAT"), shardMap.getShards().size());
            final String shardMapManagerServerName = Configuration.getShardMapManagerServerName();

            // Only create the database if it doesn't already exist. It might already exist if
            if (!SqlDatabaseUtils.databaseExists(shardMapManagerServerName, databaseName)) {
                // we tried to create it previously but hit a transient fault.
                SqlDatabaseUtils.createDatabase(shardMapManagerServerName, databaseName);
            }

            // Create schema and populate reference data on that database
            // The initialize script must be idempotent, in case it was already run on this database
            // and we failed to add it to the shard map previously
            SqlDatabaseUtils.executeSqlScript(shardMapManagerServerName, databaseName, properties.getProperty("INITIAL_SHARD_SCRIPT"));

            // Add it to the shard map
            ShardLocation shardLocation = new ShardLocation(shardMapManagerServerName, databaseName, SqlProtocol.Tcp, 1433);
            shard = ShardManagementUtils.createOrGetShard(shardMap, shardLocation);
        }

        return shard;
    }

    /**
     * Finds an existing empty shard, or returns null if none exist.
     */
    private static Shard findEmptyShard(RangeShardMap<Integer> shardMap) {
        // Get all shards in the shard map (ordered by name)
        List<Shard> allShards = shardMap.getShards().stream().sorted(Comparator.comparing(shard -> shard.getLocation().getDatabase()))
                .collect(Collectors.toList());

        // Get all mappings in the shard map
        List<RangeMapping> allMappings = shardMap.getMappings();

        // Determine which shards have mappings
        Set<UUID> shardsIdsWithMappings = allMappings.stream().map(RangeMapping::getShard).map(Shard::getId).collect(Collectors.toSet());

        // Remove all the shards that has mappings
        allShards.removeIf(shard -> shardsIdsWithMappings.contains(shard.getId()));

        // Get the first shard, if it exists
        return Iterables.getFirst(allShards, null);
    }

    /**
     * Finds an existing empty shard, or returns null if none exist.
     */
    private static Shard findEmptyShard(ListShardMap<Integer> shardMap) {
        // Get all shards in the shard map (ordered by name)
        List<Shard> allShards = shardMap.getShards().stream().sorted(Comparator.comparing(shard -> shard.getLocation().getDatabase()))
                .collect(Collectors.toList());

        // Get all mappings in the shard map
        List<PointMapping> allMappings = shardMap.getMappings();

        // Determine which shards have mappings
        Set<UUID> shardsIdsWithMappings = allMappings.stream().map(PointMapping::getShard).map(Shard::getId).collect(Collectors.toSet());

        // Remove all the shards that has mappings
        allShards.removeIf(shard -> shardsIdsWithMappings.contains(shard.getId()));

        // Get the first shard, if it exists
        return Iterables.getFirst(allShards, null);
    }
}