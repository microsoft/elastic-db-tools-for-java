package com.microsoft.azure.elasticdb.shard.cache;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.store.IStoreShardMap;

import java.io.IOException;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Cached representation of shard map manager.
 */
public class CacheRoot extends CacheObject {
    /**
     * Contained shard maps. Look up to be done by name.
     */
    private TreeMap<String, CacheShardMap> _shardMapsByName;

    /**
     * Contained shard maps. Lookup to be done by Id.
     */
    private TreeMap<UUID, CacheShardMap> _shardMapsById;

    /**
     * Constructs the cached shard map manager.
     */
    public CacheRoot() {
        super();
        //_shardMapsByName = new TreeMap<String, CacheShardMap>(StringComparer.OrdinalIgnoreCase);
        _shardMapsById = new TreeMap<UUID, CacheShardMap>();
    }

    /**
     * Adds a shard map to the cache given storage representation.
     *
     * @param ssm Storage representation of shard map.
     * @return Cached shard map object.
     */
    public final CacheShardMap AddOrUpdate(IStoreShardMap ssm) {
        CacheShardMap csm = new CacheShardMap(ssm);
        CacheShardMap csmOldByName = null;
        CacheShardMap csmOldById = null;

        //TODO: Implement TRY GET VALUE

        ReferenceObjectHelper<CacheShardMap> tempRef_csmOldByName = new ReferenceObjectHelper<CacheShardMap>(csmOldByName);
        /*if (_shardMapsByName.TryGetValue(ssm.getName(), tempRef_csmOldByName)) {
            csmOldByName = tempRef_csmOldByName.argValue;
            _shardMapsByName.remove(ssm.getName());
        } else {
            csmOldByName = tempRef_csmOldByName.argValue;
        }*/

        ReferenceObjectHelper<CacheShardMap> tempRef_csmOldById = new ReferenceObjectHelper<CacheShardMap>(csmOldById);
        /*if (_shardMapsById.TryGetValue(ssm.getId(), tempRef_csmOldById)) {
            csmOldById = tempRef_csmOldById.argValue;
            _shardMapsById.remove(ssm.getId());
        } else {
            csmOldById = tempRef_csmOldById.argValue;
        }*/
        // Both should be found or none should be found.
        assert (csmOldByName == null && csmOldById == null) || (csmOldByName != null && csmOldById != null);

        // Both should point to same cached copy.
        assert csmOldByName == csmOldById;

        if (csmOldByName != null) {
            csm.TransferStateFrom(csmOldByName);

            // Dispose off the old cached shard map
            try {
                csmOldByName.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
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
    public final void Remove(IStoreShardMap ssm) {
        if (_shardMapsByName.containsKey(ssm.getName())) {
            CacheShardMap csm = _shardMapsByName.get(ssm.getName());
            _shardMapsByName.remove(ssm.getName());

            // Dispose off the cached map
            if (csm != null) {
                try {
                    csm.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        if (_shardMapsById.containsKey(ssm.getId())) {
            CacheShardMap csm = _shardMapsById.get(ssm.getId());
            _shardMapsById.remove(ssm.getId());

            // Dispose off the cached map
            if (csm != null) {
                try {
                    csm.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Finds shard map in cache given the name.
     *
     * @param name     Name of shard map.
     * @param shardMap The found shard map object.
     * @return Cached shard map object.
     */
    public final CacheShardMap LookupByName(String name, ReferenceObjectHelper<IStoreShardMap> shardMap) {
        CacheShardMap csm = null;

        ReferenceObjectHelper<CacheShardMap> tempRef_csm = new ReferenceObjectHelper<CacheShardMap>(csm);
        //TODO: _shardMapsByName.TryGetValue(name, tempRef_csm);
        csm = tempRef_csm.argValue;

        if (csm != null) {
            shardMap.argValue = csm.getStoreShardMap();
        } else {
            shardMap.argValue = null;
        }

        return csm;
    }

    /**
     * Finds shard map in cache given the name.
     *
     * @param shardMapId Id of shard map.
     * @return Cached shard map object.
     */
    public final CacheShardMap LookupById(UUID shardMapId) {
        CacheShardMap csm = null;

        ReferenceObjectHelper<CacheShardMap> tempRef_csm = new ReferenceObjectHelper<CacheShardMap>(csm);
        //TODO: _shardMapsById.TryGetValue(shardMapId, tempRef_csm);
        csm = tempRef_csm.argValue;

        return csm;
    }

    /**
     * Clears the cache of shard maps.
     */
    public final void Clear() {
        //TODO:

        /*for (Map.Entry<String, CacheShardMap> kvp : _shardMapsByName) {
            if (kvp.getValue() != null) {
                kvp.getValue().Dispose();
            }
        }

        for (Map.Entry<UUID, CacheShardMap> kvp : _shardMapsById) {
            if (kvp.getValue() != null) {
                kvp.getValue().Dispose();
            }
        }*/

        _shardMapsByName.clear();
        _shardMapsById.clear();
    }
}
