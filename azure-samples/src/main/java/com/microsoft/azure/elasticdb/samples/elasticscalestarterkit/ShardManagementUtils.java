package com.microsoft.azure.elasticdb.samples.elasticscalestarterkit;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.map.ListShardMap;
import com.microsoft.azure.elasticdb.shard.map.RangeShardMap;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerFactory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerLoadPolicy;

final class ShardManagementUtils {

  /**
   * Tries to get the ShardMapManager that is stored in the specified database.
   */
  static ShardMapManager tryGetShardMapManager(String shardMapManagerServerName,
      String shardMapManagerDatabaseName) {
    ConsoleUtils.writeInfo("Checking if Shard Map Manager by name %s already exists...",
        shardMapManagerDatabaseName);
    if (!SqlDatabaseUtils.databaseExists(shardMapManagerServerName, shardMapManagerDatabaseName)) {
      // Shard Map Manager database has not yet been created
      ConsoleUtils.writeInfo("%s does not exist...", shardMapManagerDatabaseName);
      return null;
    }
    ConsoleUtils
        .writeInfo("Shard Map Manager exists... Trying to get %s...", shardMapManagerDatabaseName);

    String shardMapManagerConnectionString = Configuration
        .getConnectionString(shardMapManagerServerName, shardMapManagerDatabaseName);

    ShardMapManager shardMapManager = null;
    ReferenceObjectHelper<ShardMapManager> refShardMapManager = new ReferenceObjectHelper<>(
        shardMapManager);
    boolean smmExists = ShardMapManagerFactory
        .tryGetSqlShardMapManager(shardMapManagerConnectionString, ShardMapManagerLoadPolicy.Lazy,
            refShardMapManager);
    shardMapManager = refShardMapManager.argValue;

    if (!smmExists) {
      // Shard Map Manager database exists, but Shard Map Manager has not been created
      ConsoleUtils.writeInfo("Unable to get %s...", shardMapManagerDatabaseName);
      return null;
    }

    ConsoleUtils.writeInfo("Loading %s successful...", shardMapManagerDatabaseName);
    return shardMapManager;
  }

  /**
   * Creates a shard map manager in the database specified by the given connection string.
   */
  static ShardMapManager createOrGetShardMapManager(String shardMapManagerConnectionString) {
    // Get shard map manager database connection string
    // Try to get a reference to the Shard Map Manager in the shardMapManager database.
    // If it doesn't already exist, then create it.
    ShardMapManager shardMapManager = null;
    ReferenceObjectHelper<ShardMapManager> refShardMapManager =
        new ReferenceObjectHelper<ShardMapManager>(shardMapManager);
    boolean shardMapManagerExists = ShardMapManagerFactory
        .tryGetSqlShardMapManager(shardMapManagerConnectionString, ShardMapManagerLoadPolicy.Lazy,
            refShardMapManager);
    shardMapManager = refShardMapManager.argValue;

    if (shardMapManagerExists) {
      ConsoleUtils.writeInfo("Shard Map %s already exists", shardMapManager);
    } else {
      // The Shard Map Manager does not exist, so create it
      shardMapManager = ShardMapManagerFactory
          .createSqlShardMapManager(shardMapManagerConnectionString);
      ConsoleUtils.writeInfo("Created Shard Map %s", shardMapManager);
    }

    return shardMapManager;
  }

  /**
   * Creates a new Range Shard Map with the specified name, or gets the Range Shard Map if it
   * already exists.
   */
  static <T> RangeShardMap<T> createOrGetRangeShardMap(ShardMapManager shardMapManager,
      String shardMapName, ShardKeyType keyType) {
    // Try to get a reference to the Shard Map.
    RangeShardMap<T> shardMap = shardMapManager.tryGetRangeShardMap(shardMapName);

    if (shardMap != null) {
      ConsoleUtils.writeInfo("Shard Map %1$s already exists", shardMap.getName());
    } else {
      // The Shard Map does not exist, so create it
      try {
        shardMap = shardMapManager.createRangeShardMap(shardMapName, keyType);
      } catch (Exception e) {
        e.printStackTrace();
      }
      ConsoleUtils.writeInfo("Created Shard Map %1$s", shardMap.getName());
    }

    return shardMap;
  }

  /**
   * Creates a new Range Shard Map with the specified name, or gets the Range Shard Map if it
   * already exists.
   */
  static <T> ListShardMap<T> createOrGetListShardMap(ShardMapManager shardMapManager,
      String shardMapName, ShardKeyType keyType) {
    // Try to get a reference to the Shard Map.
    ListShardMap<T> shardMap = shardMapManager.tryGetListShardMap(shardMapName);

    if (shardMap != null) {
      ConsoleUtils.writeInfo("Shard Map %1$s already exists", shardMap.getName());
    } else {
      // The Shard Map does not exist, so create it
      try {
        shardMap = shardMapManager.createListShardMap(shardMapName, keyType);
      } catch (Exception e) {
        e.printStackTrace();
      }
      ConsoleUtils.writeInfo("Created Shard Map %1$s", shardMap.getName());
    }

    return shardMap;
  }

  /**
   * Adds Shards to the Shard Map, or returns them if they have already been added.
   */
  static Shard createOrGetShard(ShardMap shardMap, ShardLocation shardLocation) {
    // Try to get a reference to the Shard
    Shard shard = null;
    ReferenceObjectHelper<Shard> refShard = new ReferenceObjectHelper<Shard>(shard);
    boolean shardExists = shardMap.tryGetShard(shardLocation, refShard);
    shard = refShard.argValue;

    if (shardExists) {
      ConsoleUtils.writeInfo("Shard %1$s has already been added to the Shard Map",
          shardLocation.getDatabase());
    } else {
      // The Shard Map does not exist, so create it
      shard = shardMap.createShard(shardLocation);
      ConsoleUtils.writeInfo("Added shard %1$s to the Shard Map", shardLocation.getDatabase());
    }

    return shard;
  }
}
