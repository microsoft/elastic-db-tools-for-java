package com.microsoft.azure.elasticdb.shard.cache;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;

/**
 * Cached representation of shard map.
 */
public class CacheShardMap extends CacheObject {

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
    super();
    this.setStoreShardMap(ssm);

    switch (ssm.getMapType()) {
      case List:
        this.setMapper(new CacheListMapper(ssm.getKeyType()));
        break;
      case Range:
        this.setMapper(new CacheRangeMapper(ssm.getKeyType()));
        break;
      default:
        break;
    }

    this.perfCounters = new PerfCounterInstance(ssm.getName());
  }

  public final StoreShardMap getStoreShardMap() {
    return storeShardMap;
  }

  public final void setStoreShardMap(StoreShardMap value) {
    storeShardMap = value;
  }

  public final CacheMapper getMapper() {
    return mapper;
  }

  public final void setMapper(CacheMapper value) {
    mapper = value;
  }

  /**
   * Transfers the child cache objects to current instance from the source instance.
   * Useful for mantaining the cache even in case of refreshes to shard map objects.
   *
   * @param source Source cached shard map to copy child objects from.
   */
  public final void transferStateFrom(CacheShardMap source) {
    this.setMapper(source.getMapper());
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

  /**
   * Protected vitual member of the dispose pattern.
   *
   * @param disposing Call came from Dispose.
   */
  @Override
  protected void dispose(boolean disposing) {
    //TODO: this.perfCounters.Dispose();
    super.dispose(disposing);
  }
}