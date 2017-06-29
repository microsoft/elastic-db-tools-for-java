package com.microsoft.azure.elasticdb.shard.decorators;

import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.cache.CacheStoreMappingUpdatePolicy;
import com.microsoft.azure.elasticdb.shard.cache.ICacheStore;
import com.microsoft.azure.elasticdb.shard.cache.ICacheStoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import java.util.List;

class CacheStoreDecorator implements ICacheStore {

  protected ICacheStore inner;

  public CacheStoreDecorator(ICacheStore inner) {
    this.inner = inner;
  }

  @Override
  public void addOrUpdateShardMap(StoreShardMap shardMap) {
    this.inner.addOrUpdateShardMap(shardMap);
  }

  @Override
  public void deleteShardMap(StoreShardMap shardMap) {
    this.inner.deleteShardMap(shardMap);
  }

  @Override
  public StoreShardMap lookupShardMapByName(String shardMapName) {
    return this.inner.lookupShardMapByName(shardMapName);
  }

  @Override
  public void addOrUpdateMapping(StoreMapping mapping, CacheStoreMappingUpdatePolicy policy) {
    this.inner.addOrUpdateMapping(mapping, policy);
  }

  @Override
  public void deleteMapping(StoreMapping mapping) {
    this.inner.deleteMapping(mapping);
  }

  @Override
  public ICacheStoreMapping lookupMappingByKey(StoreShardMap shardMap, ShardKey key) {
    return this.inner.lookupMappingByKey(shardMap, key);
  }

  @Override
  public List<ICacheStoreMapping> lookupMappingsForRange(StoreShardMap shardMap, ShardRange range) {
    return this.inner.lookupMappingsForRange(shardMap, range);
  }

  @Override
  public void clear() {
    this.inner.clear();
  }

}
