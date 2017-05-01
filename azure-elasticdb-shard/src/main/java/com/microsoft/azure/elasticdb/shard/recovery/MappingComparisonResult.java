package com.microsoft.azure.elasticdb.shard.recovery;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;

/**
 * Result of comparison b/w the given range mappings.
 */
public class MappingComparisonResult {

  /**
   * Shard map to which mappings belong.
   */
  private StoreShardMap shardMap;
  /**
   * Current range.
   */
  private ShardRange range;
  /**
   * Location of the mapping.
   */
  private MappingLocation mappingLocation;
  /**
   * Mappings corresponding to current range in GSM.
   */
  private StoreMapping shardMapManagerMapping;
  /**
   * Mapping corresponding to current range in LSM.
   */
  private StoreMapping shardMapping;

  /**
   * Instantiates a new instance of range mapping comparison result.
   *
   * @param ssm Store representation of shard map.
   * @param range Range being considered.
   * @param mappingLocation Location of mapping.
   * @param gsmMapping Storage representation of GSM mapping.
   * @param lsmMapping Storange representation of LSM mapping.
   */
  public MappingComparisonResult(StoreShardMap ssm, ShardRange range,
      MappingLocation mappingLocation, StoreMapping gsmMapping, StoreMapping lsmMapping) {
    this.setShardMap(ssm);
    this.setRange(range);
    this.setMappingLocation(mappingLocation);
    this.setShardMapManagerMapping(gsmMapping);
    this.setShardMapping(lsmMapping);
  }

  public final StoreShardMap getShardMap() {
    return shardMap;
  }

  public final void setShardMap(StoreShardMap value) {
    shardMap = value;
  }

  public final ShardRange getRange() {
    return range;
  }

  public final void setRange(ShardRange value) {
    range = value;
  }

  public final MappingLocation getMappingLocation() {
    return mappingLocation;
  }

  public final void setMappingLocation(MappingLocation value) {
    mappingLocation = value;
  }

  public final StoreMapping getShardMapManagerMapping() {
    return shardMapManagerMapping;
  }

  public final void setShardMapManagerMapping(StoreMapping value) {
    shardMapManagerMapping = value;
  }

  public final StoreMapping getShardMapping() {
    return shardMapping;
  }

  public final void setShardMapping(StoreMapping value) {
    shardMapping = value;
  }
}