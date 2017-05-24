package com.microsoft.azure.elasticdb.shard.decorators;

import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.cache.CacheStoreMappingUpdatePolicy;
import com.microsoft.azure.elasticdb.shard.cache.ICacheStore;
import com.microsoft.azure.elasticdb.shard.cache.ICacheStoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;

public class CountingCacheStore extends CacheStoreDecorator {

  private int addShardMapCount;
  private int deleteShardMapCount;
  private int lookupShardMapCount;
  private int lookupShardMapHitCount;
  private int lookupShardMapMissCount;
  private int addMappingCount;
  private int deleteMappingCount;
  private int lookupMappingCount;
  private int lookupMappingHitCount;
  private int lookupMappingMissCount;

  public CountingCacheStore(ICacheStore inner) {
    super(inner);
    this.resetCounters();
  }

  public int getAddShardMapCount() {
    return this.addShardMapCount;
  }

  public void setAddShardMapCount(int value) {
    this.addShardMapCount = value;
  }

  public int getDeleteShardMapCount() {
    return this.deleteShardMapCount;
  }

  public void setDeleteShardMapCount(int value) {
    this.deleteShardMapCount = value;
  }

  public int getLookupShardMapCount() {
    return this.lookupShardMapCount;
  }

  public void setLookupShardMapCount(int value) {
    this.lookupShardMapCount = value;
  }

  public int getLookupShardMapHitCount() {
    return this.lookupShardMapHitCount;
  }

  public void setLookupShardMapHitCount(int value) {
    this.lookupShardMapHitCount = value;
  }

  public int getLookupShardMapMissCount() {
    return this.lookupShardMapMissCount;
  }

  public void setLookupShardMapMissCount(int value) {
    this.lookupShardMapMissCount = value;
  }

  public int getAddMappingCount() {
    return this.addMappingCount;
  }

  public void setAddMappingCount(int value) {
    this.addMappingCount = value;
  }

  public int getDeleteMappingCount() {
    return this.deleteMappingCount;
  }

  public void setDeleteMappingCount(int value) {
    this.deleteMappingCount = value;
  }

  public int getLookupMappingCount() {
    return this.lookupMappingCount;
  }

  public void setLookupMappingCount(int value) {
    this.lookupMappingCount = value;
  }

  public int getLookupMappingHitCount() {
    return this.lookupMappingHitCount;
  }

  public void setLookupMappingHitCount(int value) {
    this.lookupMappingHitCount = value;
  }

  public int getLookupMappingMissCount() {
    return this.lookupMappingMissCount;
  }

  public void setLookupMappingMissCount(int value) {
    this.lookupMappingMissCount = value;
  }

  /**
   * Reset all counter to 0 (Zero).
   */
  public void resetCounters() {
    this.setAddShardMapCount(0);
    this.setDeleteShardMapCount(0);
    this.setLookupShardMapCount(0);
    this.setLookupShardMapHitCount(0);
    this.setLookupShardMapMissCount(0);

    this.setAddMappingCount(0);
    this.setDeleteMappingCount(0);
    this.setLookupMappingCount(0);
    this.setLookupMappingHitCount(0);
    this.setLookupMappingMissCount(0);

  }

  @Override
  public void addOrUpdateShardMap(StoreShardMap shardMap) {
    this.setAddShardMapCount(this.getAddShardMapCount() + 1);
    super.addOrUpdateShardMap(shardMap);
  }

  @Override
  public void deleteShardMap(StoreShardMap shardMap) {
    this.setDeleteShardMapCount(this.getDeleteShardMapCount() + 1);
    super.deleteShardMap(shardMap);
  }

  @Override
  public StoreShardMap lookupShardMapByName(String shardMapName) {
    this.setLookupShardMapCount(this.getLookupShardMapCount() + 1);
    StoreShardMap result = super.lookupShardMapByName(shardMapName);
    if (result == null) {
      this.setLookupShardMapMissCount(this.getLookupShardMapMissCount() + 1);
    } else {
      this.setLookupShardMapHitCount(this.getLookupShardMapHitCount() + 1);
    }
    return result;

  }

  @Override
  public void addOrUpdateMapping(StoreMapping mapping, CacheStoreMappingUpdatePolicy policy) {
    this.setAddMappingCount(this.getAddMappingCount() + 1);
    super.addOrUpdateMapping(mapping, policy);
  }

  @Override
  public void deleteMapping(StoreMapping mapping) {
    this.setDeleteMappingCount(this.getDeleteMappingCount() + 1);
    super.deleteMapping(mapping);
  }

  @Override
  public ICacheStoreMapping lookupMappingByKey(StoreShardMap shardMap, ShardKey key) {
    this.setLookupMappingCount(this.getLookupMappingCount() + 1);
    ICacheStoreMapping result = super.lookupMappingByKey(shardMap, key);
    if (result == null) {
      this.setLookupMappingMissCount(this.getLookupMappingMissCount() + 1);
    } else {
      this.setLookupMappingHitCount(this.getLookupMappingHitCount() + 1);
    }
    return result;
  }
}
