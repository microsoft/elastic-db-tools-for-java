package com.microsoft.azure.elasticdb.shard.stubs;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.cache.ICacheStoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.stubhelper.Action0Param;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func0Param;

/**
 * Stub type of ICacheStoreMapping.
 */
public class StubICacheStoreMapping implements ICacheStoreMapping {

  /**
   * Sets the stub of ICacheStoreMapping.get_CreationTime()
   */
  public Func0Param<Long> creationTimeGet;
  /**
   * Sets the stub of ICacheStoreMapping.HasTimeToLiveExpired()
   */
  public Func0Param<Boolean> hasTimeToLiveExpired;
  /**
   * Sets the stub of ICacheStoreMapping.get_Mapping()
   */
  public Func0Param<StoreMapping> mappingGet;
  /**
   * Sets the stub of ICacheStoreMapping.ResetTimeToLive()
   */
  public Action0Param resetTimeToLive;
  /**
   * Sets the stub of ICacheStoreMapping.get_TimeToLiveMilliseconds()
   */
  public Func0Param<Long> timeToLiveMillisecondsGet;

  private IStubBehavior instanceBehavior;

  /**
   * Gets or sets the instance behavior.
   */
  public final IStubBehavior getInstanceBehavior() {
    return StubBehaviors.getValueOrCurrent(this.instanceBehavior);
  }

  public final void setInstanceBehavior(IStubBehavior value) {
    this.instanceBehavior = value;
  }

  /**
   * Stub to get Creation Time.
   */
  public final long getCreationTime() {
    Func0Param<Long> func1 = () -> creationTimeGet.invoke();
    if (func1 != null) {
      return func1.invoke();
    }
    return this.getInstanceBehavior().<StubICacheStoreMapping, Long>result(this,
        "ICacheStoreMapping.get_CreationTime");
  }

  /**
   * Stub to get Mapping.
   */
  public StoreMapping getMapping() {
    Func0Param<StoreMapping> func1 = () -> mappingGet.invoke();
    if (func1 != null) {
      return func1.invoke();
    }
    return this.getInstanceBehavior().result(this,
        "ICacheStoreMapping.get_Mapping");
  }

  /**
   * Stub to get Cache's Time to live in milliseconds.
   */
  public long getTimeToLiveMilliseconds() {
    Func0Param<Long> func1 = () -> timeToLiveMillisecondsGet.invoke();
    if (func1 != null) {
      return func1.invoke();
    }
    return this.getInstanceBehavior().<StubICacheStoreMapping, Long>result(this,
        "ICacheStoreMapping.get_TimeToLiveMilliseconds");
  }

  @Override
  public final boolean hasTimeToLiveExpired() {
    Func0Param<Boolean> func1 = () -> hasTimeToLiveExpired.invoke();
    if (func1 != null) {
      return func1.invoke();
    }
    return this.getInstanceBehavior().<StubICacheStoreMapping, Boolean>result(this,
        "ICacheStoreMapping.HasTimeToLiveExpired");
  }

  @Override
  public final void resetTimeToLive() {
    Action0Param action1 = () -> resetTimeToLive.invoke();
    if (action1 != null) {
      action1.invoke();
    } else {
      this.getInstanceBehavior().voidResult(this, "ICacheStoreMapping.ResetTimeToLive");
    }
  }
}
