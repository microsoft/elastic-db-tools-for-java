package com.microsoft.azure.elasticdb.shard.cache;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client side cache store.
 */
public class CacheStore implements ICacheStore {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Contained shard maps. Look up to be done by name.
   */
  private Map<String, CacheShardMap> shardMapsByName;

  /**
   * Contained shard maps. Lookup to be done by Id.
   */
  private Map<UUID, CacheShardMap> shardMapsById;

  /**
   * Constructs an instance of client side cache object.
   */
  public CacheStore() {
    shardMapsByName = new ConcurrentHashMap<>();
    shardMapsById = new ConcurrentHashMap<>();
  }

  /**
   * Invoked for refreshing shard map in cache from store.
   *
   * @param ssm Storage representation of shard map.
   */
  public void addOrUpdateShardMap(StoreShardMap ssm) {
    CacheShardMap csm = new CacheShardMap(ssm);
    CacheShardMap csmOldByName = shardMapsByName.get(ssm.getName());
    CacheShardMap csmOldById = shardMapsById.get(ssm.getId());

    if (csmOldByName != null) {
      shardMapsByName.remove(ssm.getName());
    }
    if (csmOldById != null) {
      shardMapsById.remove(ssm.getId());
    }
    // Both should be found or none should be found.
    assert (csmOldByName == null && csmOldById == null) || (csmOldByName != null
        && csmOldById != null);

    // Both should point to same cached copy.
    assert csmOldByName == csmOldById;

    if (csmOldByName != null) {
      csm.transferStateFrom(csmOldByName);
    }

    shardMapsByName.put(ssm.getName(), csm);
    shardMapsById.put(ssm.getId(), csm);
  }

  /**
   * Invoked for deleting shard map in cache because it no longer exists in store.
   *
   * @param shardMap Storage representation of shard map.
   */
  public void deleteShardMap(StoreShardMap shardMap) {
    shardMapsByName.remove(shardMap.getName());
    shardMapsById.remove(shardMap.getId());
  }

  /**
   * Looks up a given shard map in cache based on it's name.
   *
   * @param shardMapName Name of shard map.
   * @return The shard being searched.
   */
  public StoreShardMap lookupShardMapByName(String shardMapName) {
    CacheShardMap csm = shardMapsByName.get(shardMapName);
    log.info("Cache {}; ShardMap: {}", csm == null ? "miss" : "hit", shardMapName);
    return (csm != null) ? csm.getStoreShardMap() : null;
  }

  /**
   * Invoked for refreshing mapping in cache from store.
   *
   * @param mapping Storage representation of mapping.
   * @param policy Policy to use for preexisting cache entries during update.
   */
  public void addOrUpdateMapping(StoreMapping mapping, CacheStoreMappingUpdatePolicy policy) {
    CacheShardMap csm = shardMapsById.get(mapping.getShardMapId());
    if (csm == null) {
      return;
    }
    // Mapper by itself is thread-safe using ConcurrentHashMap and ConcurrentSkipListMap.
    csm.getMapper().addOrUpdate(mapping, policy);

    log.info("Cache Add/Update mapping complete. Mapping Id: {}", mapping.getId());
  }

  /**
   * Invoked for deleting mapping in cache because it no longer exists in store.
   *
   * @param mapping Storage representation of mapping.
   */
  public void deleteMapping(StoreMapping mapping) {
    CacheShardMap csm = shardMapsById.get(mapping.getShardMapId());
    if (csm == null) {
      return;
    }
    csm.getMapper().remove(mapping);

    log.info("Cache delete mapping complete. Mapping Id: {}", mapping.getId());
  }

  /**
   * Looks up a given key in given shard map.
   *
   * @param shardMap Storage representation of shard map.
   * @param key Key value.
   * @return Mapping corresponding to <paramref name="key"/> or null.
   */
  public ICacheStoreMapping lookupMappingByKey(StoreShardMap shardMap, ShardKey key) {
    CacheShardMap csm = shardMapsById.get(shardMap.getId());
    if (csm == null) {
      return null;
    }

    return csm.getMapper().lookupByKey(key);
  }

  /**
   * Looks up a given range in given shard map.
   *
   * @param shardMap Storage representation of shard map.
   * @param range Optional range value, if null, we cover everything.
   * @return Mapping corresponding to <paramref name="key"/> or null.
   */
  public List<ICacheStoreMapping> lookupMappingsForRange(StoreShardMap shardMap, ShardRange range) {
    CacheShardMap csm = shardMapsById.get(shardMap.getId());
    if (csm == null) {
      return null;
    }

    ReferenceObjectHelper<List<StoreMapping>> tempRefSmDummy = new ReferenceObjectHelper<>(null);

    return csm.getMapper().lookupByRange(range, tempRefSmDummy);
  }

  /**
   * Clears the cache.
   */
  public void clear() {
    shardMapsByName.clear();
    shardMapsById.clear();
  }
}