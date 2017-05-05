package com.microsoft.azure.elasticdb.shard.cache;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cached representation of collection of mappings within shard map.
 * The items consist of a single point values.
 */
public class CacheListMapper extends CacheMapper {

  /**
   * Mappings organized by Key.
   */
  private Map<ShardKey, CacheMapping> mappingsByKey;

  /**
   * Constructs the mapper, notes the key type for lookups.
   *
   * @param keyType Key type.
   */
  public CacheListMapper(ShardKeyType keyType) {
    super(keyType);
    // Use concurrentHashMap as it locks at key level instead of entire map for better performance.
    mappingsByKey = new ConcurrentHashMap<>();
  }

  /**
   * Add or update a mapping in cache.
   *
   * @param sm Storage mapping object.
   * @param policy Policy to use for preexisting cache entries during update.
   */
  @Override
  public void addOrUpdate(StoreMapping sm, CacheStoreMappingUpdatePolicy policy) {
    // Make key out of mapping key.
    ShardKey key = ShardKey.fromRawValue(this.getKeyType(), sm.getMinValue());

    CacheMapping cm = null;

    // We need to update TTL and update entry if:
    // a) We are in update TTL mode
    // b) Mapping exists and same as the one we already have
    // c) Entry is beyond the TTL limit
    if (policy == CacheStoreMappingUpdatePolicy.UpdateTimeToLive
        && mappingsByKey.containsKey(key) && cm.getMapping().getId() == sm.getId()) {
      cm = new CacheMapping(sm, CacheMapper.calculateNewTimeToLiveMilliseconds(cm));
    } else {
      cm = new CacheMapping(sm);
    }
    // Remove existing entry.
    this.remove(sm);

    // Add the entry to lookup table by Key.
    mappingsByKey.put(key, cm);
  }

  /**
   * Remove a mapping object from cache.
   *
   * @param sm Storage maping object.
   */
  @Override
  public void remove(StoreMapping sm) {
    // Make key value out of mapping key.
    ShardKey key = ShardKey.fromRawValue(this.getKeyType(), sm.getMinValue());

    // Remove existing entry.
    if (mappingsByKey.containsKey(key)) {
      mappingsByKey.remove(key);
    }
  }

  /**
   * Looks up a mapping by key.
   *
   * @param key Key value.
   * @return Mapping object which has the key value.
   */
  @Override
  public ICacheStoreMapping lookupByKey(ShardKey key) {
    return mappingsByKey.get(key);
  }

  /**
   * Get number of point mappings cached in this mapper.
   *
   * @return Number of cached point mappings.
   */
  @Override
  public long getMappingsCount() {
    return mappingsByKey.size();
  }

  /**
   * Clears all the mappings in the lookup by Id table as well
   * as lookup by key table.
   */
  @Override
  protected void clear() {
    mappingsByKey.clear();
  }
}
