package com.microsoft.azure.elasticdb.shard.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;

/**
 * Cached representation of collection of mappings within shard map. The items consist of a single point values.
 */
public class CacheListMapper extends CacheMapper {

    /**
     * Mappings organized by Key.
     */
    private Map<ShardKey, CacheMapping> mappingsByKey;

    /**
     * Constructs the mapper, notes the key type for lookups.
     *
     * @param keyType
     *            Key type.
     */
    public CacheListMapper(ShardKeyType keyType) {
        super(keyType);
        // Use concurrentHashMap as it locks at key level instead of entire map for better performance.
        mappingsByKey = new ConcurrentHashMap<>();
    }

    /**
     * Add or update a mapping in cache.
     *
     * @param sm
     *            Storage mapping object.
     * @param policy
     *            Policy to use for preexisting cache entries during update.
     */
    @Override
    public void addOrUpdate(StoreMapping sm,
            CacheStoreMappingUpdatePolicy policy) {
        // Make key out of mapping key.
        ShardKey key = ShardKey.fromRawValue(this.getKeyType(), sm.getMinValue());

        CacheMapping cm = null;
        if (mappingsByKey.containsKey(key)) {
            cm = mappingsByKey.get(key);
        }

        // We need to update TTL and update entry if:
        // a) We are in update TTL mode
        // b) Mapping exists and same as the one we already have
        // c) Entry is beyond the TTL limit
        if (policy == CacheStoreMappingUpdatePolicy.UpdateTimeToLive && cm != null && cm.getMapping().getId().equals(sm.getId())) {
            cm = new CacheMapping(sm, CacheMapper.calculateNewTimeToLiveMilliseconds(cm));
        }
        else {
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
     * @param sm
     *            Storage mapping object.
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
     * @param key
     *            Key value.
     * @return Mapping object which has the key value.
     */
    @Override
    public ICacheStoreMapping lookupByKey(ShardKey key) {
        return mappingsByKey.get(key);
    }

    /**
     * Looks up a mapping by Range.
     *
     * @param range
     *            Optional range value, if null, we cover everything.
     * @param sm
     *            Storage mapping object.
     * @return Mapping object which has the key value.
     */
    @Override
    public List<ICacheStoreMapping> lookupByRange(ShardRange range,
            ReferenceObjectHelper<List<StoreMapping>> sm) {
        List<StoreMapping> mappings = new ArrayList<>();

        List<ICacheStoreMapping> filteredList = range == null ? new ArrayList<>(mappingsByKey.values())
                : mappingsByKey.entrySet().stream()
                        .filter(m -> ShardKey.opGreaterThanOrEqual(m.getKey(), range.getLow()) && ShardKey.opLessThan(m.getKey(), range.getHigh()))
                        .map(Entry::getValue).collect(Collectors.toList());

        filteredList.forEach(item -> mappings.add(item.getMapping()));

        sm.argValue = mappings;

        return filteredList;
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
     * Clears all the mappings in the lookup by Id table as well as lookup by key table.
     */
    @Override
    protected void clear() {
        mappingsByKey.clear();
    }
}
