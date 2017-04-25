package com.microsoft.azure.elasticdb.shard.mapmanager.decorators;

import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.cache.CacheStoreMappingUpdatePolicy;
import com.microsoft.azure.elasticdb.shard.cache.ICacheStore;
import com.microsoft.azure.elasticdb.shard.cache.ICacheStoreMapping;
import com.microsoft.azure.elasticdb.shard.cache.PerformanceCounterName;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import java.io.IOException;

class CacheStoreDecorator implements ICacheStore {

  protected ICacheStore inner;

  public CacheStoreDecorator(ICacheStore inner) {
    this.inner = inner;
  }

  @Override
  public void close() throws IOException {
    this.inner.close();
  }

  @Override
  public void AddOrUpdateShardMap(StoreShardMap shardMap) {
    this.inner.AddOrUpdateShardMap(shardMap);

  }

  @Override
  public void DeleteShardMap(StoreShardMap shardMap) {
    this.inner.DeleteShardMap(shardMap);

  }

  @Override
  public StoreShardMap LookupShardMapByName(String shardMapName) {
    return this.inner.LookupShardMapByName(shardMapName);
  }

  @Override
  public void AddOrUpdateMapping(StoreMapping mapping, CacheStoreMappingUpdatePolicy policy) {
    this.inner.AddOrUpdateMapping(mapping, policy);
  }

  @Override
  public void DeleteMapping(StoreMapping mapping) {
    this.inner.DeleteMapping(mapping);
  }

  @Override
  public ICacheStoreMapping LookupMappingByKey(StoreShardMap shardMap, ShardKey key) {
    return this.inner.LookupMappingByKey(shardMap, key);
  }

  @Override
  public void IncrementPerformanceCounter(StoreShardMap shardMap, PerformanceCounterName name) {
    this.inner.IncrementPerformanceCounter(shardMap, name);

  }

  @Override
  public void Clear() {
    this.inner.Clear();
  }

}
