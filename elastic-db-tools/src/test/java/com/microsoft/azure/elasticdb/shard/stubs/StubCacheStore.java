package com.microsoft.azure.elasticdb.shard.stubs;

/*
 * Copyright (c) Microsoft. All rights reserved. Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

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
 * Stub type of CacheStore.
 */
public class StubCacheStore extends CacheStore {

    /**
     * Sets the stub of CacheStore.addOrUpdateMapping(StoreMapping mapping, CacheStoreMappingUpdatePolicy policy)
     */
    public Action2Param<StoreMapping, CacheStoreMappingUpdatePolicy> addOrUpdateMappingIStoreMappingCacheStoreMappingUpdatePolicy;
    /**
     * Sets the stub of CacheStore.addOrUpdateShardMap(StoreShardMap shardMap)
     */
    public Action1Param<StoreShardMap> addOrUpdateShardMapIStoreShardMap;
    /**
     * Sets the stub of CacheStore.clear()
     */
    public Action0Param clear01;
    /**
     * Sets the stub of CacheStore.deleteMapping(StoreMapping mapping)
     */
    public Action1Param<StoreMapping> deleteMappingIStoreMapping;
    /**
     * Sets the stub of CacheStore.deleteShardMap(StoreShardMap shardMap)
     */
    public Action1Param<StoreShardMap> deleteShardMapIStoreShardMap;
    /**
     * Sets the stub of CacheStore.dispose(Boolean disposing)
     */
    public Action1Param<Boolean> disposeBoolean;
    /**
     * Sets the stub of CacheStore.lookupMappingByKey(StoreShardMap shardMap, ShardKey key)
     */
    public Func2Param<StoreShardMap, ShardKey, ICacheStoreMapping> lookupMappingByKeyIStoreShardMapShardKey;
    /**
     * Sets the stub of CacheStore.lookupShardMapByName(String shardMapName)
     */
    public Func1Param<String, StoreShardMap> lookupShardMapByNameString;

    private boolean callBase;
    private IStubBehavior instanceBehavior;

    /**
     * Initializes a new instance.
     */
    public StubCacheStore() {
        this.initializeStub();
    }

    /**
     * Gets or sets a value that indicates if the base method should be called instead of the fallback behavior.
     */
    public final boolean getCallBase() {
        return this.callBase;
    }

    public final void setCallBase(boolean value) {
        this.callBase = value;
    }

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
     * Sets the stub of CacheStore.addOrUpdateMapping(StoreMapping mapping, CacheStoreMappingUpdatePolicy policy)
     */
    @Override
    public void addOrUpdateMapping(StoreMapping mapping,
            CacheStoreMappingUpdatePolicy policy) {
        Action2Param<StoreMapping, CacheStoreMappingUpdatePolicy> action1 = (StoreMapping arg1,
                CacheStoreMappingUpdatePolicy arg2) -> addOrUpdateMappingIStoreMappingCacheStoreMappingUpdatePolicy.invoke(arg1, arg2);
        if (addOrUpdateMappingIStoreMappingCacheStoreMappingUpdatePolicy != null) {
            action1.invoke(mapping, policy);
        }
        else if (this.callBase) {
            super.addOrUpdateMapping(mapping, policy);
        }
        else {
            this.getInstanceBehavior().voidResult(this, "addOrUpdateMapping");
        }
    }

    /**
     * Sets the stub of CacheStore.addOrUpdateShardMap(StoreShardMap shardMap)
     */
    @Override
    public void addOrUpdateShardMap(StoreShardMap shardMap) {
        Action1Param<StoreShardMap> action1 = (StoreShardMap obj) -> this.addOrUpdateShardMapIStoreShardMap.invoke(obj);
        if (addOrUpdateShardMapIStoreShardMap != null) {
            action1.invoke(shardMap);
        }
        else if (this.callBase) {
            super.addOrUpdateShardMap(shardMap);
        }
        else {
            this.getInstanceBehavior().voidResult(this, "addOrUpdateShardMap");
        }
    }

    /**
     * Sets the stub of CacheStore.clear()
     */
    @Override
    public void clear() {
        Action0Param action1 = () -> clear01.invoke();
        if (clear01 != null) {
            action1.invoke();
        }
        else if (this.callBase) {
            super.clear();
        }
        else {
            this.getInstanceBehavior().voidResult(this, "clear");
        }
    }

    /**
     * Sets the stub of CacheStore.deleteMapping(StoreMapping mapping)
     */
    @Override
    public void deleteMapping(StoreMapping mapping) {
        Action1Param<StoreMapping> action1 = (StoreMapping obj) -> deleteMappingIStoreMapping.invoke(obj);
        if (deleteMappingIStoreMapping != null) {
            action1.invoke(mapping);
        }
        else if (this.callBase) {
            super.deleteMapping(mapping);
        }
        else {
            this.getInstanceBehavior().voidResult(this, "deleteMapping");
        }
    }

    /**
     * Sets the stub of CacheStore.deleteShardMap(StoreShardMap shardMap)
     */
    @Override
    public void deleteShardMap(StoreShardMap shardMap) {
        Action1Param<StoreShardMap> action1 = (StoreShardMap obj) -> deleteShardMapIStoreShardMap.invoke(obj);
        if (deleteShardMapIStoreShardMap != null) {
            action1.invoke(shardMap);
        }
        else if (this.callBase) {
            super.deleteShardMap(shardMap);
        }
        else {
            this.getInstanceBehavior().voidResult(this, "deleteShardMap");
        }
    }

    /**
     * Initializes a new instance of type StubCacheStore.
     */
    private void initializeStub() {
    }

    /**
     * Sets the stub of CacheStore.lookupMappingByKey(StoreShardMap shardMap, ShardKey key)
     */
    @Override
    public ICacheStoreMapping lookupMappingByKey(StoreShardMap shardMap,
            ShardKey key) {
        Func2Param<StoreShardMap, ShardKey, ICacheStoreMapping> func1 = (StoreShardMap arg1,
                ShardKey arg2) -> lookupMappingByKeyIStoreShardMapShardKey.invoke(arg1, arg2);
        if (lookupMappingByKeyIStoreShardMapShardKey != null) {
            return func1.invoke(shardMap, key);
        }
        if (this.callBase) {
            return super.lookupMappingByKey(shardMap, key);
        }
        return this.getInstanceBehavior().result(this, "lookupMappingByKey");
    }

    /**
     * Sets the stub of CacheStore.lookupShardMapByName(String shardMapName)
     */
    @Override
    public StoreShardMap lookupShardMapByName(String shardMapName) {
        Func1Param<String, StoreShardMap> func1 = (String arg) -> lookupShardMapByNameString.invoke(arg);
        if (lookupShardMapByNameString != null) {
            return func1.invoke(shardMapName);
        }
        if (this.callBase) {
            return super.lookupShardMapByName(shardMapName);
        }
        return this.getInstanceBehavior().result(this, "lookupShardMapByName");
    }
}
