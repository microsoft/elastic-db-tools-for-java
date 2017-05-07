package com.microsoft.azure.elasticdb.shard.cache;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;

/**
 * Cached representation of shard map.
 */
public class CacheShardMap {

  /**
   * Storage representation of shard map.
   */
  private StoreShardMap storeShardMap;
  /**
   * Mapper object. Exists only for List/Range/Hash shard maps.
   */
  private CacheMapper mapper;
  /**
   * Performance counter instance for this shard map.
   */
  private PerfCounterInstance perfCounters;

  /**
   * Constructs the cached shard map.
   *
   * @param ssm Storage representation of shard map.
   */
  public CacheShardMap(StoreShardMap ssm) {
    storeShardMap = ssm;

    switch (ssm.getMapType()) {
      case List:
        mapper = new CacheListMapper(ssm.getKeyType());
        break;
      case Range:
        mapper = new CacheRangeMapper(ssm.getKeyType());
        break;
      default:
        throw new RuntimeException("Unknown shardMapType:" + ssm.getMapType());
    }

    this.perfCounters = new PerfCounterInstance(ssm.getName());
  }

  public final StoreShardMap getStoreShardMap() {
    return storeShardMap;
  }

  public final CacheMapper getMapper() {
    return mapper;
  }

  /**
   * Transfers the child cache objects to current instance from the source instance.
   * Useful for mantaining the cache even in case of refreshes to shard map objects.
   *
   * @param source Source cached shard map to copy child objects from.
   */
  public final void transferStateFrom(CacheShardMap source) {
    mapper = source.getMapper();
  }

  /**
   * Increment value of performance counter by 1.
   *
   * @param name Name of performance counter to increment.
   */
  public final void incrementPerformanceCounter(PerformanceCounterName name) {
    this.perfCounters.incrementCounter(name);
  }

  /**
   * Set raw value of performance counter.
   *
   * @param name Performance counter to update.
   * @param value Raw value for the counter. This method is always called from CacheStore inside
   * csm.GetWriteLockScope() so we do not have to worry about multithreaded access here.
   */
  public final void setPerformanceCounter(PerformanceCounterName name, long value) {
    this.perfCounters.setCounter(name, value);
  }
}