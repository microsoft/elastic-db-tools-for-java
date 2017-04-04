package com.microsoft.azure.elasticdb.samples.elasticscalestarterkit;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.map.RangeShardMap;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerFactory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerLoadPolicy;

public final class ShardManagementUtils {
    /**
     * Tries to get the ShardMapManager that is stored in the specified database.
     */
    public static ShardMapManager TryGetShardMapManager(String shardMapManagerServerName, String shardMapManagerDatabaseName) {
        String shardMapManagerConnectionString = Configuration.GetConnectionString(Configuration.getShardMapManagerServerName(), Configuration.getShardMapManagerDatabaseName());

        if (!SqlDatabaseUtils.DatabaseExists(shardMapManagerServerName, shardMapManagerDatabaseName)) {
            // Shard Map Manager database has not yet been created
            return null;
        }

        ShardMapManager shardMapManager = null;
        ReferenceObjectHelper<ShardMapManager> tempRef_shardMapManager = new ReferenceObjectHelper<ShardMapManager>(shardMapManager);
        boolean smmExists = ShardMapManagerFactory.TryGetSqlShardMapManager(shardMapManagerConnectionString, ShardMapManagerLoadPolicy.Lazy, tempRef_shardMapManager);
        shardMapManager = tempRef_shardMapManager.argValue;

        if (!smmExists) {
            // Shard Map Manager database exists, but Shard Map Manager has not been created
            return null;
        }

        return shardMapManager;
    }

    /**
     * Creates a shard map manager in the database specified by the given connection string.
     */
    public static ShardMapManager CreateOrGetShardMapManager(String shardMapManagerConnectionString) {
        // Get shard map manager database connection string
        // Try to get a reference to the Shard Map Manager in the Shard Map Manager database. If it doesn't already exist, then create it.
        ShardMapManager shardMapManager = null;
        ReferenceObjectHelper<ShardMapManager> tempRef_shardMapManager = new ReferenceObjectHelper<ShardMapManager>(shardMapManager);
        boolean shardMapManagerExists = ShardMapManagerFactory.TryGetSqlShardMapManager(shardMapManagerConnectionString, ShardMapManagerLoadPolicy.Lazy, tempRef_shardMapManager);
        shardMapManager = tempRef_shardMapManager.argValue;

        if (shardMapManagerExists) {
            ConsoleUtils.WriteInfo("Shard Map Manager already exists");
        } else {
            // The Shard Map Manager does not exist, so create it
            shardMapManager = ShardMapManagerFactory.CreateSqlShardMapManager(shardMapManagerConnectionString);
            ConsoleUtils.WriteInfo("Created Shard Map Manager");
        }

        return shardMapManager;
    }

    /**
     * Creates a new Range Shard Map with the specified name, or gets the Range Shard Map if it already exists.
     */
    public static <T> RangeShardMap<T> CreateOrGetRangeShardMap(ShardMapManager shardMapManager, String shardMapName) {
        // Try to get a reference to the Shard Map.
        RangeShardMap<T> shardMap = null;
        ReferenceObjectHelper<RangeShardMap<T>> tempRef_shardMap = new ReferenceObjectHelper<RangeShardMap<T>>(shardMap);
        boolean shardMapExists = shardMapManager.TryGetRangeShardMap(shardMapName, tempRef_shardMap);
        shardMap = tempRef_shardMap.argValue;

        if (shardMapExists) {
            ConsoleUtils.WriteInfo("Shard Map {0} already exists", shardMap.getName());
        } else {
            // The Shard Map does not exist, so create it
            shardMap = shardMapManager.<T>CreateRangeShardMap(shardMapName);
            ConsoleUtils.WriteInfo("Created Shard Map {0}", shardMap.getName());
        }

        return shardMap;
    }

    /**
     * Adds Shards to the Shard Map, or returns them if they have already been added.
     */
    public static Shard CreateOrGetShard(ShardMap shardMap, ShardLocation shardLocation) {
        // Try to get a reference to the Shard
        Shard shard = null;
        ReferenceObjectHelper<Shard> tempRef_shard = new ReferenceObjectHelper<Shard>(shard);
        boolean shardExists = shardMap.TryGetShard(shardLocation, tempRef_shard);
        shard = tempRef_shard.argValue;

        if (shardExists) {
            ConsoleUtils.WriteInfo("Shard {0} has already been added to the Shard Map", shardLocation.getDatabase());
        } else {
            // The Shard Map does not exist, so create it
            shard = shardMap.CreateShard(shardLocation);
            ConsoleUtils.WriteInfo("Added shard {0} to the Shard Map", shardLocation.getDatabase());
        }

        return shard;
    }
}
