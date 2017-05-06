package com.microsoft.azure.elasticdb.shard.stubs;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.cache.CacheStore;
import com.microsoft.azure.elasticdb.shard.cache.CacheStoreMappingUpdatePolicy;
import com.microsoft.azure.elasticdb.shard.cache.ICacheStoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.stubhelper.Action0Param;
import com.microsoft.azure.elasticdb.shard.stubhelper.Action1Param;
import com.microsoft.azure.elasticdb.shard.stubhelper.Action2Param;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func1Param;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func2Param;

/**
 * Stub type of Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.CacheStore
 */
public class StubCacheStore extends CacheStore {

  /**
   * Sets the stub of CacheStore.addOrUpdateMapping(StoreMapping mapping,
   * CacheStoreMappingUpdatePolicy policy)
   */
  public Action2Param<StoreMapping, CacheStoreMappingUpdatePolicy> AddOrUpdateMappingIStoreMappingCacheStoreMappingUpdatePolicy;
  /**
   * Sets the stub of CacheStore.addOrUpdateShardMap(StoreShardMap shardMap)
   */
  public Action1Param<StoreShardMap> AddOrUpdateShardMapIStoreShardMap;
  /**
   * Sets the stub of CacheStore.clear()
   */
  public Action0Param Clear01;
  /**
   * Sets the stub of CacheStore.deleteMapping(StoreMapping mapping)
   */
  public Action1Param<StoreMapping> DeleteMappingIStoreMapping;
  /**
   * Sets the stub of CacheStore.deleteShardMap(StoreShardMap shardMap)
   */
  public Action1Param<StoreShardMap> DeleteShardMapIStoreShardMap;
  /**
   * Sets the stub of CacheStore.dispose(Boolean disposing)
   */
  public Action1Param<Boolean> DisposeBoolean;
  /**
   * Sets the stub of CacheStore.lookupMappingByKey(StoreShardMap shardMap, ShardKey key)
   */
  public Func2Param<StoreShardMap, ShardKey, ICacheStoreMapping> LookupMappingByKeyIStoreShardMapShardKey;
  /**
   * Sets the stub of CacheStore.lookupShardMapByName(String shardMapName)
   */
  public Func1Param<String, StoreShardMap> LookupShardMapByNameString;
  private boolean ___callBase;
  private IStubBehavior ___instanceBehavior;

  /**
   * Initializes a new instance
   */
  public StubCacheStore() {
    this.InitializeStub();
  }

  /**
   * Gets or sets a value that indicates if the base method should be called instead of the fallback
   * behavior
   */
  public final boolean getCallBase() {
    return this.___callBase;
  }

  public final void setCallBase(boolean value) {
    this.___callBase = value;
  }

  /**
   * Gets or sets the instance behavior.
   */
  public final IStubBehavior getInstanceBehavior() {
    return StubBehaviors.GetValueOrCurrent(this.___instanceBehavior);
  }

  public final void setInstanceBehavior(IStubBehavior value) {
    this.___instanceBehavior = value;
  }

  /**
   * Sets the stub of CacheStore.addOrUpdateMapping(StoreMapping mapping,
   * CacheStoreMappingUpdatePolicy policy)
   */
  @Override
  public void addOrUpdateMapping(StoreMapping mapping, CacheStoreMappingUpdatePolicy policy) {
    Action2Param<StoreMapping, CacheStoreMappingUpdatePolicy> action1 = (StoreMapping arg1, CacheStoreMappingUpdatePolicy arg2) -> AddOrUpdateMappingIStoreMappingCacheStoreMappingUpdatePolicy
        .invoke(arg1, arg2);
    if (action1 != null) {
      action1.invoke(mapping, policy);
    } else if (this.___callBase) {
      super.addOrUpdateMapping(mapping, policy);
    } else {
      this.getInstanceBehavior().VoidResult(this, "addOrUpdateMapping");
    }
  }

  /**
   * Sets the stub of CacheStore.addOrUpdateShardMap(StoreShardMap shardMap)
   */
  @Override
  public void addOrUpdateShardMap(StoreShardMap shardMap) {
    Action1Param<StoreShardMap> action1;
    if(this.AddOrUpdateShardMapIStoreShardMap == null){
     action1 = null;
    }
    else{
      action1 = (StoreShardMap obj) -> this.AddOrUpdateShardMapIStoreShardMap.invoke(obj);
    }
    if (action1 != null) {
      action1.invoke(shardMap);
    } else if (this.___callBase) {
      super.addOrUpdateShardMap(shardMap);
    } else {
      this.getInstanceBehavior().VoidResult(this, "addOrUpdateShardMap");
    }
  }

  /**
   * Sets the stub of CacheStore.clear()
   */
  @Override
  public void clear() {
    Action0Param action1 = () -> Clear01.invoke();
    if (action1 != null) {
      action1.invoke();
    } else if (this.___callBase) {
      super.clear();
    } else {
      this.getInstanceBehavior().VoidResult(this, "clear");
    }
  }

  /**
   * Sets the stub of CacheStore.deleteMapping(StoreMapping mapping)
   */
  @Override
  public void deleteMapping(StoreMapping mapping) {
    Action1Param<StoreMapping> action1 = (StoreMapping obj) -> DeleteMappingIStoreMapping
        .invoke(obj);
    if (action1 != null) {
      action1.invoke(mapping);
    } else if (this.___callBase) {
      super.deleteMapping(mapping);
    } else {
      this.getInstanceBehavior().VoidResult(this, "deleteMapping");
    }
  }

  /**
   * Sets the stub of CacheStore.deleteShardMap(StoreShardMap shardMap)
   */
  @Override
  public void deleteShardMap(StoreShardMap shardMap) {
    Action1Param<StoreShardMap> action1;
    if(this.DeleteShardMapIStoreShardMap == null){
      action1 = null;
    }else{
      action1 = (StoreShardMap obj) -> DeleteShardMapIStoreShardMap.invoke(obj);
    }
    if (action1 != null) {
      action1.invoke(shardMap);
    } else if (this.___callBase) {
      super.deleteShardMap(shardMap);
    } else {
      this.getInstanceBehavior().VoidResult(this, "deleteShardMap");
    }
  }

  /**
   * Initializes a new instance of type StubCacheStore
   */
  private void InitializeStub() {
  }

  /**
   * Sets the stub of CacheStore.lookupMappingByKey(StoreShardMap shardMap, ShardKey key)
   */
  @Override
  public ICacheStoreMapping lookupMappingByKey(StoreShardMap shardMap, ShardKey key) {
    Func2Param<StoreShardMap, ShardKey, ICacheStoreMapping> func1 = (StoreShardMap arg1, ShardKey arg2) -> LookupMappingByKeyIStoreShardMapShardKey
        .invoke(arg1, arg2);
    if (func1 != null) {
      return func1.invoke(shardMap, key);
    }
    if (this.___callBase) {
      return super.lookupMappingByKey(shardMap, key);
    }
    return this.getInstanceBehavior().Result(this,
        "lookupMappingByKey");
  }

  /**
   * Sets the stub of CacheStore.lookupShardMapByName(String shardMapName)
   */
  @Override
  public StoreShardMap lookupShardMapByName(String shardMapName) {
    Func1Param<String, StoreShardMap> func1;
    if(this.LookupShardMapByNameString == null){
      func1 = null;
    }else{
      func1 =(String arg) -> LookupShardMapByNameString.invoke(arg);
    }
    if (func1 != null) {
      return func1.invoke(shardMapName);
    }
    if (this.___callBase) {
      return super.lookupShardMapByName(shardMapName);
    }
    return this.getInstanceBehavior().Result(this,
        "lookupShardMapByName");
  }
}
