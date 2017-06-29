package com.microsoft.azure.elasticdb.shard.cache;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;

/**
 * Cached representation of shard map.
 */
public class CacheShardMap {

  /**
   * Storage representation of shard map.
   */
  private StoreShardMap storeShardMap;
  /**
   * Mapper object. Exists only for List/Range/Hash shard maps.
   */
  private CacheMapper mapper;

  /**
   * Constructs the cached shard map.
   *
   * @param ssm Storage representation of shard map.
   */
  public CacheShardMap(StoreShardMap ssm) {
    storeShardMap = ssm;

    switch (ssm.getMapType()) {
      case List:
        mapper = new CacheListMapper(ssm.getKeyType());
        break;
      case Range:
        mapper = new CacheRangeMapper(ssm.getKeyType());
        break;
      default:
        throw new RuntimeException("Unknown shardMapType:" + ssm.getMapType());
    }
  }

  public final StoreShardMap getStoreShardMap() {
    return storeShardMap;
  }

  public final CacheMapper getMapper() {
    return mapper;
  }

  /**
   * Transfers the child cache objects to current instance from the source instance.
   * Useful for maintaining the cache even in case of refreshes to shard map objects.
   *
   * @param source Source cached shard map to copy child objects from.
   */
  public final void transferStateFrom(CacheShardMap source) {
    mapper = source.getMapper();
  }
}