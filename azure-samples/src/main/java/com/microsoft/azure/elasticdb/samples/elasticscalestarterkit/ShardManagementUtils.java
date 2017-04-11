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
        String shardMapManagerConnectionString = Configuration.GetConnectionString(shardMapManagerServerName, shardMapManagerDatabaseName);

        ConsoleUtils.WriteInfo("Checking if Shard Map Manager by name %s already exists...", shardMapManagerDatabaseName);
        if (!SqlDatabaseUtils.DatabaseExists(shardMapManagerConnectionString, shardMapManagerDatabaseName)) {
            // Shard Map shardMapManager database has not yet been created
            ConsoleUtils.WriteInfo("%s does not exist...", shardMapManagerDatabaseName);
            return null;
        }
        ConsoleUtils.WriteInfo("Shard Map Manager exists... Trying to get %s...", shardMapManagerDatabaseName);

        ShardMapManager shardMapManager = null;
        ReferenceObjectHelper<ShardMapManager> tempRef_shardMapManager = new ReferenceObjectHelper<>(shardMapManager);
        boolean smmExists = ShardMapManagerFactory.TryGetSqlShardMapManager(shardMapManagerConnectionString, ShardMapManagerLoadPolicy.Lazy, tempRef_shardMapManager);
        shardMapManager = tempRef_shardMapManager.argValue;

        if (!smmExists) {
            // Shard Map shardMapManager database exists, but Shard Map shardMapManager has not been created
            return null;
        }

        return shardMapManager;
    }

    /**
     * Creates a shard map manager in the database specified by the given connection string.
     */
    public static ShardMapManager CreateOrGetShardMapManager(String shardMapManagerConnectionString) {
        // Get shard map manager database connection string
        // Try to get a reference to the Shard Map shardMapManager in the Shard Map shardMapManager database. If it doesn't already exist, then create it.
        ShardMapManager shardMapManager = null;
        ReferenceObjectHelper<ShardMapManager> tempRef_shardMapManager = new ReferenceObjectHelper<ShardMapManager>(shardMapManager);
        boolean shardMapManagerExists = ShardMapManagerFactory.TryGetSqlShardMapManager(shardMapManagerConnectionString, ShardMapManagerLoadPolicy.Lazy, tempRef_shardMapManager);
        shardMapManager = tempRef_shardMapManager.argValue;

        if (shardMapManagerExists) {
            ConsoleUtils.WriteInfo("Shard Map shardMapManager already exists");
        } else {
            // The Shard Map shardMapManager does not exist, so create it
            shardMapManager = ShardMapManagerFactory.CreateSqlShardMapManager(shardMapManagerConnectionString);
            ConsoleUtils.WriteInfo("Created Shard Map shardMapManager");
        }

        return shardMapManager;
    }

    /**
     * Creates a new Range Shard Map with the specified name, or gets the Range Shard Map if it already exists.
     */
    public static <T> RangeShardMap<T> CreateOrGetRangeShardMap(ShardMapManager shardMapManager, String shardMapName) {
        // Try to get a reference to the Shard Map.
        RangeShardMap<T> shardMap = shardMapManager.TryGetRangeShardMap(shardMapName);

        if (shardMap != null) {
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
