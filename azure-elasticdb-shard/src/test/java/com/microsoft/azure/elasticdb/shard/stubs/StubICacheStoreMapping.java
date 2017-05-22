package com.microsoft.azure.elasticdb.shard.stubs;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.cache.ICacheStoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.stubhelper.Action0Param;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func0Param;

/**
 * Stub type of Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ICacheStoreMapping
 */
public class StubICacheStoreMapping implements ICacheStoreMapping {

  /**
   * Sets the stub of ICacheStoreMapping.get_CreationTime()
   */
  public Func0Param<Long> CreationTimeGet;
  /**
   * Sets the stub of ICacheStoreMapping.HasTimeToLiveExpired()
   */
  public Func0Param<Boolean> HasTimeToLiveExpired;
  /**
   * Sets the stub of ICacheStoreMapping.get_Mapping()
   */
  public Func0Param<StoreMapping> MappingGet;
  /**
   * Sets the stub of ICacheStoreMapping.ResetTimeToLive()
   */
  public Action0Param ResetTimeToLive;
  /**
   * Sets the stub of ICacheStoreMapping.get_TimeToLiveMilliseconds()
   */
  public Func0Param<Long> TimeToLiveMillisecondsGet;

  private IStubBehavior ___instanceBehavior;

  /**
   * Gets or sets the instance behavior.
   */
  public final IStubBehavior getInstanceBehavior() {
    return StubBehaviors.GetValueOrCurrent(this.___instanceBehavior);
  }

  public final void setInstanceBehavior(IStubBehavior value) {
    this.___instanceBehavior = value;
  }

  public long getCreationTime() {
    Func0Param<Long> func1 = () -> CreationTimeGet.invoke();
    if (func1 != null) {
      return func1.invoke();
    }
    return this.getInstanceBehavior().<StubICacheStoreMapping, Long>Result(this,
        "Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ICacheStoreMapping.get_CreationTime");
  }

  public StoreMapping getMapping() {
    Func0Param<StoreMapping> func1 = () -> MappingGet.invoke();
    if (func1 != null) {
      return func1.invoke();
    }
    return this.getInstanceBehavior().Result(this,
        "Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ICacheStoreMapping.get_Mapping");
  }

  public long getTimeToLiveMilliseconds() {
    Func0Param<Long> func1 = () -> TimeToLiveMillisecondsGet.invoke();
    if (func1 != null) {
      return func1.invoke();
    }
    return this.getInstanceBehavior().<StubICacheStoreMapping, Long>Result(this,
        "Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ICacheStoreMapping.get_TimeToLiveMilliseconds");
  }

  @Override
  public void resetTimeToLive() {

  }

  @Override
  public boolean hasTimeToLiveExpired() {
    return false;
  }

  public final boolean HasTimeToLiveExpired() {
    Func0Param<Boolean> func1 = () -> HasTimeToLiveExpired.invoke();
    if (func1 != null) {
      return func1.invoke();
    }
    return this.getInstanceBehavior().<StubICacheStoreMapping, Boolean>Result(this,
        "Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ICacheStoreMapping.HasTimeToLiveExpired");
  }

  public final void ResetTimeToLive() {
    Action0Param action1 = this::ResetTimeToLive;
    if (action1 != null) {
      action1.invoke();
    } else {
      this.getInstanceBehavior().VoidResult(this,
          "Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ICacheStoreMapping.ResetTimeToLive");
    }
  }
}
