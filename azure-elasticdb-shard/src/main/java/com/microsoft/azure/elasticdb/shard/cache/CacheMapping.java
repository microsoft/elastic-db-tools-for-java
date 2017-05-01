package com.microsoft.azure.elasticdb.shard.cache;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Cached representation of a single mapping.
 */
public class CacheMapping implements ICacheStoreMapping {

  /**
   * Time to live for the cache entry.
   */
  private long timeToLiveMilliseconds;
  /**
   * Storage representation of the mapping.
   */
  private StoreMapping mapping;
  /**
   * Mapping entry creation time.
   */
  private long creationTime;

  /**
   * Constructs cached representation of a mapping object.
   *
   * @param storeMapping Storage representation of mapping.
   */
  public CacheMapping(StoreMapping storeMapping) {
    this(storeMapping, 0);
  }

  /**
   * Constructs cached representation of a mapping object.
   *
   * @param storeMapping Storage representation of mapping.
   * @param timeToLiveMilliseconds Mapping expiration time.
   */
  public CacheMapping(StoreMapping storeMapping, long timeToLiveMilliseconds) {
    this.setMapping(storeMapping);
    this.setCreationTime(LocalDateTime.now().getLong(ChronoField.MILLI_OF_SECOND));
    this.setTimeToLiveMilliseconds(timeToLiveMilliseconds);
  }

  public final StoreMapping getMapping() {
    return mapping;
  }

  private void setMapping(StoreMapping value) {
    mapping = value;
  }

  public final long getCreationTime() {
    return creationTime;
  }

  private void setCreationTime(long value) {
    creationTime = value;
  }

  /**
   * Mapping entry expiration time.
   */
  public final long getTimeToLiveMilliseconds() {
    return timeToLiveMilliseconds;
  }

  private void setTimeToLiveMilliseconds(long value) {
    timeToLiveMilliseconds = value;
  }

  /**
   * Resets the mapping entry expiration time to 0.
   */
  public final void resetTimeToLive() {
    timeToLiveMilliseconds = new AtomicLong(0L).get();
  }

  /**
   * Whether TimeToLiveMilliseconds have elapsed since the CreationTime.
   *
   * @return True if they have
   */
  public final boolean hasTimeToLiveExpired() {
    return (System.nanoTime() - creationTime) >= timeToLiveMilliseconds;
  }
}