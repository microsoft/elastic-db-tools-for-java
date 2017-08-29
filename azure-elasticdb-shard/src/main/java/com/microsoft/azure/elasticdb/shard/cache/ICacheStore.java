package com.microsoft.azure.elasticdb.shard.cache;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import java.util.List;

/**
 * Representation of client side cache.
 */
public interface ICacheStore {

  /**
   * Invoked for refreshing shard map in cache from store.
   *
   * @param shardMap Storage representation of shard map.
   */
  void addOrUpdateShardMap(StoreShardMap shardMap);

  /**
   * Invoked for deleting shard map in cache because it no longer exists in store.
   *
   * @param shardMap Storage representation of shard map.
   */
  void deleteShardMap(StoreShardMap shardMap);

  /**
   * Looks up a given shard map in cache based on it's name.
   *
   * @param shardMapName Name of shard map.
   * @return The shard being searched.
   */
  StoreShardMap lookupShardMapByName(String shardMapName);

  /**
   * Invoked for refreshing mapping in cache from store.
   *
   * @param mapping Storage representation of mapping.
   * @param policy Policy to use for preexisting cache entries during update.
   */
  void addOrUpdateMapping(StoreMapping mapping, CacheStoreMappingUpdatePolicy policy);

  /**
   * Invoked for deleting mapping in cache because it no longer exists in store.
   *
   * @param mapping Storage representation of mapping.
   */
  void deleteMapping(StoreMapping mapping);

  /**
   * Looks up a given key in given shard map.
   *
   * @param shardMap Storage representation of shard map.
   * @param key Key value.
   * @return Mapping corresponding to <paramref name="key"/> or null.
   */
  ICacheStoreMapping lookupMappingByKey(StoreShardMap shardMap, ShardKey key);

  /**
   * Looks up a given range in given shard map.
   *
   * @param shardMap Storage representation of shard map.
   * @param range Optional range value, if null, we cover everything.
   * @return Mapping corresponding to <paramref name="key"/> or null.
   */
  List<ICacheStoreMapping> lookupMappingsForRange(StoreShardMap shardMap, ShardRange range);

  /**
   * Clears the cache.
   */
  void clear();
}
