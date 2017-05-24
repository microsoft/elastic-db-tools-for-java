package com.microsoft.azure.elasticdb.shardmapscalability;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import java.util.List;

public abstract class ShardMapOperations<T> {

  private ShardMap shardMap;

  public final ShardMap getShardMap() {
    return shardMap;
  }

  protected final void setShardMap(ShardMap value) {
    shardMap = value;
  }

  public final Shard AddShard(ShardLocation shardLocation) {
    // Add shards to the shard map, if they aren't already there
    List<Shard> existingShards = getShardMap().getShards();
    Shard shard = existingShards.stream().filter(s -> s.getLocation().equals(shardLocation))
        .findFirst().orElse(null);
    if (shard != null) {
      System.out.printf("Shard %1$s has already been added to the Shard Map" + "\r\n",
          shardLocation.getDataSource());
    } else {
      shard = getShardMap().createShard(shardLocation);
      System.out.printf("Added shard %1$s to the Shard Map" + "\r\n",
          shard.getLocation().getDataSource());
    }
    return shard;
  }

  public final int GetCurrentMappingCount() {
    return GetCurrentMappingCountInternal();
  }

  protected abstract int GetCurrentMappingCountInternal();

  public final void CreateMapping(T key, Shard shard) {
    try {
      CreateMappingInternal(key, shard);
    } catch (RuntimeException e) {
      System.out.printf("Error while creating mapping for key: %1$s" + "\r\n", key);
      throw e;
    }
  }

  protected abstract void CreateMappingInternal(T key, Shard shard);

  public abstract void LookupMapping(T key);
}