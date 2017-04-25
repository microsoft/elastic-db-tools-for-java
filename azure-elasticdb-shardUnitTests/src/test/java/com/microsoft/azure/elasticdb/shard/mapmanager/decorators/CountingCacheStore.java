package com.microsoft.azure.elasticdb.shard.mapmanager.decorators;

import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.cache.CacheStoreMappingUpdatePolicy;
import com.microsoft.azure.elasticdb.shard.cache.ICacheStore;
import com.microsoft.azure.elasticdb.shard.cache.ICacheStoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;

public class CountingCacheStore extends CacheStoreDecorator {

  private int AddShardMapCount;
  private int DeleteShardMapCount;
  private int LookupShardMapCount;
  private int LookupShardMapHitCount;
  private int LookupShardMapMissCount;
  private int AddMappingCount;
  private int DeleteMappingCount;
  private int LookupMappingCount;
  private int LookupMappingHitCount;
  private int LookupMappingMissCount;

  public CountingCacheStore(ICacheStore inner) {
    super(inner);
    this.ResetCounters();
  }

  public int getAddShardMapCount() {
    return this.AddShardMapCount;
  }

  public void setAddShardMapCount(int AddShardMapCount) {
    this.AddShardMapCount = AddShardMapCount;
  }

  public int getDeleteShardMapCount() {
    return this.DeleteShardMapCount;
  }

  public void setDeleteShardMapCount(int DeleteShardMapCount) {
    this.DeleteShardMapCount = DeleteShardMapCount;
  }

  public int getLookupShardMapCount() {
    return this.LookupShardMapCount;
  }

  public void setLookupShardMapCount(int LookupShardMapCount) {
    this.LookupShardMapCount = LookupShardMapCount;
  }

  public int getLookupShardMapHitCount() {
    return this.LookupShardMapHitCount;
  }

  public void setLookupShardMapHitCount(int LookupShardMapHitCount) {
    this.LookupShardMapHitCount = LookupShardMapHitCount;
  }

  public int getLookupShardMapMissCount() {
    return this.LookupShardMapMissCount;
  }

  public void setLookupShardMapMissCount(int LookupShardMapMissCount) {
    this.LookupShardMapMissCount = LookupShardMapMissCount;
  }

  public int getAddMappingCount() {
    return this.AddMappingCount;
  }

  public void setAddMappingCount(int AddMappingCount) {
    this.AddMappingCount = AddMappingCount;
  }

  public int getDeleteMappingCount() {
    return this.DeleteMappingCount;
  }

  public void setDeleteMappingCount(int DeleteMappingCount) {
    this.DeleteMappingCount = DeleteMappingCount;
  }

  public int getLookupMappingCount() {
    return this.LookupMappingCount;
  }

  public void setLookupMappingCount(int LookupMappingCount) {
    this.LookupMappingCount = LookupMappingCount;
  }

  public int getLookupMappingHitCount() {
    return this.LookupMappingHitCount;
  }

  public void setLookupMappingHitCount(int LookupMappingHitCount) {
    this.LookupMappingHitCount = LookupMappingHitCount;
  }

  public int getLookupMappingMissCount() {
    return this.LookupMappingMissCount;
  }

  public void setLookupMappingMissCount(int LookupMappingMissCount) {
    this.LookupMappingMissCount = LookupMappingMissCount;
  }

  private void ResetCounters() {
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
  public void AddOrUpdateShardMap(StoreShardMap shardMap) {
    this.setAddMappingCount(this.getAddMappingCount() + 1);
    super.AddOrUpdateShardMap(shardMap);
  }

  @Override
  public void DeleteShardMap(StoreShardMap shardMap) {
    this.setDeleteShardMapCount(this.getDeleteShardMapCount() + 1);
    super.DeleteShardMap(shardMap);
  }

  @Override
  public StoreShardMap LookupShardMapByName(String shardMapName) {
    this.setLookupShardMapCount(this.getLookupShardMapCount() + 1);
    StoreShardMap result = super.LookupShardMapByName(shardMapName);
    if (result == null) {
      this.setLookupShardMapMissCount(this.getLookupShardMapMissCount() + 1);
    } else {
      this.setLookupShardMapHitCount(this.getLookupShardMapHitCount() + 1);
    }
    return result;

  }

  @Override
  public void AddOrUpdateMapping(StoreMapping mapping, CacheStoreMappingUpdatePolicy policy) {
    this.setAddMappingCount(this.getAddMappingCount() + 1);
    super.AddOrUpdateMapping(mapping, policy);
  }

  @Override
  public void DeleteMapping(StoreMapping mapping) {
    this.setDeleteMappingCount(this.getDeleteMappingCount() + 1);
    super.DeleteMapping(mapping);
  }

  @Override
  public ICacheStoreMapping LookupMappingByKey(StoreShardMap shardMap, ShardKey key) {
    this.setLookupMappingCount(this.getLookupMappingCount() + 1);
    ICacheStoreMapping result = super.LookupMappingByKey(shardMap, key);
    if (result == null) {
      this.setLookupMappingMissCount(this.getLookupMappingMissCount() + 1);
    } else {
      this.setLookupMappingHitCount(this.getLookupMappingHitCount() + 1);
    }
    return result;
  }

}
