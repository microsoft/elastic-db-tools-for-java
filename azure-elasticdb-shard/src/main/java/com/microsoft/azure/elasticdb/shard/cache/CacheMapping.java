package com.microsoft.azure.elasticdb.shard.cache;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;

/**
 * Cached representation of a single mapping.
 */
public class CacheMapping implements ICacheStoreMapping {

  /**
   * Time to live for the cache entry.
   */
  private long _timeToLiveMilliseconds;
  /**
   * Storage representation of the mapping.
   */
  private StoreMapping Mapping;
  /**
   * Mapping entry creation time.
   */
  private long CreationTime;

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
    //TODO: this.setCreationTime(TimerUtils.GetTimestamp());
    this.setTimeToLiveMilliseconds(timeToLiveMilliseconds);
  }

  public final StoreMapping getMapping() {
    return Mapping;
  }

  private void setMapping(StoreMapping value) {
    Mapping = value;
  }

  public final long getCreationTime() {
    return CreationTime;
  }

  private void setCreationTime(long value) {
    CreationTime = value;
  }

  /**
   * Mapping entry expiration time.
   */
  public final long getTimeToLiveMilliseconds() {
    return _timeToLiveMilliseconds;
  }

  private void setTimeToLiveMilliseconds(long value) {
    _timeToLiveMilliseconds = value;
  }

  /**
   * Resets the mapping entry expiration time to 0.
   */
  public final void ResetTimeToLive() {
    ReferenceObjectHelper<Long> tempRef__timeToLiveMilliseconds = new ReferenceObjectHelper<Long>(
        _timeToLiveMilliseconds);
    //TODO:
    //Interlocked.CompareExchange(tempRef__timeToLiveMilliseconds, 0L, _timeToLiveMilliseconds);
    //_timeToLiveMilliseconds = tempRef__timeToLiveMilliseconds.argValue;
  }

  /**
   * Whether TimeToLiveMilliseconds have elapsed
   * since the CreationTime
   *
   * @return True if they have
   */
  public final boolean HasTimeToLiveExpired() {
    return false; //TODO: TimerUtils.ElapsedMillisecondsSince(this.getCreationTime()) >= this.getTimeToLiveMilliseconds();
  }
}