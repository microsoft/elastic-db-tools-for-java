package com.microsoft.azure.elasticdb.samples.elasticscalestarterkit;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.Range;
import com.microsoft.azure.elasticdb.shard.base.RangeMapping;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.map.RangeShardMap;

import java.util.List;

public class CreateShardSample {
    /**
     * Script file that will be executed to initialize a shard.
     */
    private static final String InitializeShardScriptFile = "InitializeShard.sql";
    /**
     * Format to use for creating shard name. {0} is the number of shards that have already been created.
     */
    private static final String ShardNameFormat = "ElasticScaleStarterKit_Shard{0}";

    /**
     * Creates a new shard (or uses an existing empty shard), adds it to the shard map,
     * and assigns it the specified range if possible.
     */
    public static void CreateShard(RangeShardMap<Integer> shardMap, Range<Integer> rangeForNewShard) {
        // Create a new shard, or get an existing empty shard (if a previous create partially succeeded).
        Shard shard = CreateOrGetEmptyShard(shardMap);

        // Create a mapping to that shard.
        RangeMapping<Integer> mappingForNewShard = shardMap.CreateRangeMapping(rangeForNewShard, shard);
        ConsoleUtils.WriteInfo("Mapped range {0} to shard {1}", mappingForNewShard.getValue(), shard.getLocation().getDatabase());
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
            String databaseName = String.format(ShardNameFormat, shardMap.GetShards().size());

            // Only create the database if it doesn't already exist. It might already exist if
            // we tried to create it previously but hit a transient fault.
            if (!SqlDatabaseUtils.DatabaseExists(Configuration.getShardMapManagerServerName(), databaseName)) {
                SqlDatabaseUtils.CreateDatabase(Configuration.getShardMapManagerServerName(), databaseName);
            }

            // Create schema and populate reference data on that database
            // The initialize script must be idempotent, in case it was already run on this database
            // and we failed to add it to the shard map previously
            SqlDatabaseUtils.ExecuteSqlScript(Configuration.getShardMapManagerServerName(), databaseName, InitializeShardScriptFile);

            // Add it to the shard map
            ShardLocation shardLocation = new ShardLocation(Configuration.getShardMapManagerServerName(), databaseName);
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
        //TODO: List<RangeMapping<Integer>> allMappings = shardMap.GetMappings();

        // Determine which shards have mappings
        //TODO: HashSet<Shard> shardsWithMappings = new HashSet<Shard>(allMappings.Select(m -> m.Shard));

        // Get the first shard (ordered by name) that has no mappings, if it exists
        //TODO: Convert below LINQ queries to Java
        return allShards.get(0); //TODO: .OrderBy(s -> s.Location.Database).FirstOrDefault(s -> !shardsWithMappings.contains(s));
    }
}