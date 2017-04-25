package com.microsoft.azure.elasticdb.shard.recovery;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;

/**
 * Class for mapping differences.
 */
public class MappingDifference {

  /**
   * Type of mapping difference. Either List or Range.
   */
  private MappingDifferenceType Type;
  /**
   * Location where the mappings that differ exist.
   */
  private MappingLocation Location;
  /**
   * ShardMap which has the consistency violation.
   */
  private StoreShardMap ShardMap;
  /**
   * Mapping found in shard map.
   */
  private StoreMapping MappingForShardMap;
  /**
   * Mapping found in shard.
   */
  private StoreMapping MappingForShard;

  public MappingDifference(MappingDifferenceType type, MappingLocation location,
      StoreShardMap shardMap, StoreMapping mappingForShardMap, StoreMapping mappingForShard) {
    this.setType(type);
    this.setLocation(location);
    this.setShardMap(shardMap);
    this.setMappingForShardMap(mappingForShardMap);
    this.setMappingForShard(mappingForShard);
  }

  public final MappingDifferenceType getType() {
    return Type;
  }

  private void setType(MappingDifferenceType value) {
    Type = value;
  }

  public final MappingLocation getLocation() {
    return Location;
  }

  private void setLocation(MappingLocation value) {
    Location = value;
  }

  public final StoreShardMap getShardMap() {
    return ShardMap;
  }

  private void setShardMap(StoreShardMap value) {
    ShardMap = value;
  }

  public final StoreMapping getMappingForShardMap() {
    return MappingForShardMap;
  }

  private void setMappingForShardMap(StoreMapping value) {
    MappingForShardMap = value;
  }

  public final StoreMapping getMappingForShard() {
    return MappingForShard;
  }

  private void setMappingForShard(StoreMapping value) {
    MappingForShard = value;
  }
}