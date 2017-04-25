package com.microsoft.azure.elasticdb.shard.cache;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;

/**
 * Representation of client side cache.
 */
public interface ICacheStore extends java.io.Closeable {

  /**
   * Invoked for refreshing shard map in cache from store.
   *
   * @param shardMap Storage representation of shard map.
   */
  void AddOrUpdateShardMap(StoreShardMap shardMap);

  /**
   * Invoked for deleting shard map in cache becase it no longer exists in store.
   *
   * @param shardMap Storage representation of shard map.
   */
  void DeleteShardMap(StoreShardMap shardMap);

  /**
   * Looks up a given shard map in cache based on it's name.
   *
   * @param shardMapName Name of shard map.
   * @return The shard being searched.
   */
  StoreShardMap LookupShardMapByName(String shardMapName);

  /**
   * Invoked for refreshing mapping in cache from store.
   *
   * @param mapping Storage representation of mapping.
   * @param policy Policy to use for preexisting cache entries during update.
   */
  void AddOrUpdateMapping(StoreMapping mapping, CacheStoreMappingUpdatePolicy policy);

  /**
   * Invoked for deleting mapping in cache becase it no longer exists in store.
   *
   * @param mapping Storage representation of mapping.
   */
  void DeleteMapping(StoreMapping mapping);

  /**
   * Looks up a given key in given shard map.
   *
   * @param shardMap Storage representation of shard map.
   * @param key Key value.
   * @return Mapping corresponding to <paramref name="key"/> or null.
   */
  ICacheStoreMapping LookupMappingByKey(StoreShardMap shardMap, ShardKey key);

  /**
   * Increment specified perf counter.
   *
   * @param shardMap Storage representation of shard map.
   * @param name Performance counter to increment.s
   */
  void IncrementPerformanceCounter(StoreShardMap shardMap, PerformanceCounterName name);

  /**
   * Clears the cache.
   */
  void Clear();
}
