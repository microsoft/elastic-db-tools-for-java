package com.microsoft.azure.elasticdb.shard.cache;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import java.util.Comparator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Cached representation of shard map manager.
 */
public class CacheRoot extends CacheObject {

  /**
   * Contained shard maps. Look up to be done by name.
   */
  private SortedMap<String, CacheShardMap> _shardMapsByName;

  /**
   * Contained shard maps. Lookup to be done by Id.
   */
  private SortedMap<UUID, CacheShardMap> _shardMapsById;

  /**
   * Constructs the cached shard map manager.
   */
  public CacheRoot() {
    super();
    _shardMapsByName = new TreeMap<>(Comparator.comparing(s -> s, String.CASE_INSENSITIVE_ORDER));
    _shardMapsById = new TreeMap<>();
  }

  /**
   * Adds a shard map to the cache given storage representation.
   *
   * @param ssm Storage representation of shard map.
   * @return Cached shard map object.
   */
  public final CacheShardMap AddOrUpdate(StoreShardMap ssm) {
    CacheShardMap csm = new CacheShardMap(ssm);
    CacheShardMap csmOldByName = _shardMapsByName.get(ssm.getName());
    CacheShardMap csmOldById = _shardMapsById.get(ssm.getId());

    if (csmOldByName != null) {
      _shardMapsByName.remove(ssm.getName());
    }
    if (csmOldById != null) {
      _shardMapsById.remove(ssm.getId());
    }
    // Both should be found or none should be found.
    assert (csmOldByName == null && csmOldById == null) || (csmOldByName != null
        && csmOldById != null);

    // Both should point to same cached copy.
    assert csmOldByName == csmOldById;

    if (csmOldByName != null) {
      csm.TransferStateFrom(csmOldByName);
      // Dispose off the old cached shard map
      csmOldByName.close();
    }

    _shardMapsByName.put(ssm.getName(), csm);
    _shardMapsById.put(ssm.getId(), csm);
    return csm;
  }

  /**
   * Removes shard map from cache given the name.
   *
   * @param ssm Storage representation of shard map.
   */
  public final void Remove(StoreShardMap ssm) {
    CacheShardMap csmNameByName = _shardMapsByName.get(ssm.getName());
    if (csmNameByName != null) {
      _shardMapsByName.remove(ssm.getName());
      // Dispose off the cached map
      csmNameByName.close();
    }

    CacheShardMap csmById = _shardMapsById.get(ssm.getId());
    if (csmById != null) {
      _shardMapsById.remove(ssm.getId());
      csmById.close(); //TODO: this would have already closed above. Do we need to close again?
    }
  }

  /**
   * Finds shard map in cache given the name.
   *
   * @param name Name of shard map.
   * @return Cached shard map object.
   */
  public final StoreShardMap LookupByName(String name) {
    CacheShardMap csm = _shardMapsByName.get(name);
    return (csm != null) ? csm.getStoreShardMap() : null;
  }

  /**
   * Finds shard map in cache given the name.
   *
   * @param shardMapId Id of shard map.
   * @return Cached shard map object.
   */
  public final CacheShardMap LookupById(UUID shardMapId) {
    return _shardMapsById.get(shardMapId);
  }

  /**
   * Clears the cache of shard maps.
   */
  public final void Clear() {
    _shardMapsByName.values().forEach(v -> v.close());
    _shardMapsById.values().forEach(v -> v.close());

    _shardMapsByName.clear();
    _shardMapsById.clear();
  }
}
