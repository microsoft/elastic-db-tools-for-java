package com.microsoft.azure.elasticdb.shard.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

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
 * Cached representation of collection of mappings within shard map. The items consist of a ranges of key values.
 */
public class CacheRangeMapper extends CacheMapper {

    /**
     * Mappings organized by Key Ranges.
     */
    private NavigableMap<ShardRange, CacheMapping> mappingsByRange;

    /**
     * Constructs the mapper, notes the key type for lookups.
     *
     * @param keyType
     *            Key type.
     */
    public CacheRangeMapper(ShardKeyType keyType) {
        super(keyType);
        // Use concurrent sorted map to automatically take care of locking at key-level.
        mappingsByRange = new ConcurrentSkipListMap<>();
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
        ShardKey min = ShardKey.fromRawValue(this.getKeyType(), sm.getMinValue());

        // Make range out of mapping key ranges.
        ShardRange range = new ShardRange(min, ShardKey.fromRawValue(this.getKeyType(), sm.getMaxValue()));

        CacheMapping cm;
        ICacheStoreMapping csm;

        // We need to update TTL and update entry if:
        // a) We are in update TTL mode
        // b) Mapping exists and same as the one we already have
        // c) Entry is beyond the TTL limit
        if (policy == CacheStoreMappingUpdatePolicy.UpdateTimeToLive && (csm = this.lookupByKey(min)) != null
                && csm.getMapping().getId() == sm.getId()) {
            cm = new CacheMapping(sm, CacheMapper.calculateNewTimeToLiveMilliseconds(csm));
        }
        else {
            cm = new CacheMapping(sm);
        }

        this.remove(sm);

        // Add the entry to lookup table by Range.
        mappingsByRange.put(range, cm);
    }

    /**
     * Remove a mapping object from cache. Q: Do we ever need to remove multiple entries from the cache which cover the same range? A: Yes. Imagine
     * that you have some stale mapping in the cache, user performs an AddRangeMapping operation on a subset of stale mapping range, now you should
     * remove the stale mapping.
     *
     * @param sm
     *            Storage mapping object.
     */
    @Override
    public void remove(StoreMapping sm) {
        ShardKey minKey = ShardKey.fromRawValue(this.getKeyType(), sm.getMinValue());
        ShardKey maxKey = ShardKey.fromRawValue(this.getKeyType(), sm.getMaxValue());

        // Make range out of mapping key.
        ShardRange range = new ShardRange(minKey, maxKey);

        if (mappingsByRange.size() > 0) {
            // Fast code path, where cache does contain the exact range.
            if (mappingsByRange.containsKey(range)) {
                mappingsByRange.remove(range);
            }
            else {
                int indexMin = this.getIndexOfMappingWithClosestMinLessThanOrEqualToMinKey(minKey);
                int indexMax = this.getIndexOfMappingWithClosestMaxGreaterThanOrEqualToMaxKey(maxKey);

                if (indexMin < 0) {
                    indexMin = 0;
                }

                if (indexMax >= mappingsByRange.size()) {
                    indexMax = mappingsByRange.size() - 1;
                }

                // Do we need this? If yes, why?
                // Find first range with max greater than min key.
                ShardRange rangeMaxGreatMinKey = mappingsByRange.keySet().stream().filter(r -> ShardKey.opGreaterThan(r.getHigh(), minKey))
                        .findFirst().orElse(null);

                // Find first range with min less than or equal to max key.
                ShardRange rangeMinLessEqualMaxKey = mappingsByRange.keySet().stream().filter(r -> ShardKey.opLessThanOrEqual(r.getLow(), maxKey))
                        .findFirst().orElse(null);

                ArrayList<ShardRange> rangesToRemove = new ArrayList<>();
                List<ShardRange> keys = new ArrayList<>(mappingsByRange.keySet());
                for (; indexMin <= indexMax; indexMin++) {
                    rangesToRemove.add(keys.get(indexMin));
                }

                for (ShardRange rangeToRemove : rangesToRemove) {
                    mappingsByRange.remove(rangeToRemove);
                }
            }
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
        CacheMapping cm = null;

        // Performs a binary search in the ranges for key value and
        // then return the result.
        ShardRange range = this.getIndexOfMappingContainingShardKey(key);
        if (range != null) {
            cm = mappingsByRange.get(range);
        }
        return cm;
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
        List<ICacheStoreMapping> cm = new ArrayList<>();
        sm.argValue = new ArrayList<>();

        if (range == null) {
            // Filter
            for (Entry<ShardRange, CacheMapping> e : mappingsByRange.entrySet()) {
                sm.argValue.add(e.getValue().getMapping());
                cm.add(e.getValue());
            }
            return cm;
        }

        // Performs a binary search in the ranges for key value and then return the result.
        ShardRange lowerIndex = this.getIndexOfMappingContainingShardKey(range.getLow());
        ShardRange higherIndex = this.getIndexOfMappingContainingShardKey(range.getHigh());

        if (lowerIndex != null && higherIndex != null) {
            Map<ShardRange, CacheMapping> m = mappingsByRange.subMap(lowerIndex, true, higherIndex, true);

            // Filter
            for (Entry<ShardRange, CacheMapping> e : m.entrySet()) {
                sm.argValue.add(e.getValue().getMapping());
                cm.add(e.getValue());
            }

            return cm;
        }
        else {
            sm.argValue = null;
        }

        return null;
    }

    /**
     * Get number of range mappings cached in this mapper.
     *
     * @return Number of cached range mappings.
     */
    @Override
    public long getMappingsCount() {
        return mappingsByRange.size();
    }

    /**
     * Clears all the mappings in the lookup by Id table as well as lookup by range table.
     */
    @Override
    protected void clear() {
        mappingsByRange.clear();
    }

    /**
     * Performs binary search on the cached mappings and returns the index of mapping object which contains the given key.
     *
     * @param key
     *            Input key.
     * @return Index of range in the cache which contains the given key.
     */
    private ShardRange getIndexOfMappingContainingShardKey(ShardKey key) {
        // Keep low and high as same key when you lookup in sorted map.
        ShardRange rangeKey = new ShardRange(key, key);
        // Could potentially match with lower bound.
        ShardRange floorKey = mappingsByRange.floorKey(rangeKey);
        if (floorKey != null && floorKey.contains(key)) {
            return floorKey;
        }
        // Could potentially match with upper bound.
        ShardRange ceilingKey = mappingsByRange.ceilingKey(rangeKey);
        if (ceilingKey != null && ceilingKey.contains(key)) {
            return ceilingKey;
        }
        return null;
    }

    /**
     * Performs binary search on the cached mappings and returns the index of mapping object whose min-value is less than and closest to given key
     * value.
     *
     * @param key
     *            Input key.
     * @return Index of range in the cache which contains the given key.
     */
    private int getIndexOfMappingWithClosestMinLessThanOrEqualToMinKey(ShardKey key) {
        Set<ShardRange> rangeKeys = mappingsByRange.keySet();

        int lb = 0;
        int ub = rangeKeys.size() - 1;

        while (lb <= ub) {
            int mid = lb + (ub - lb) / 2;

            ShardRange current = (ShardRange) rangeKeys.toArray()[mid];

            if (ShardKey.opLessThanOrEqual(current.getLow(), key)) {
                if (ShardKey.opGreaterThan(current.getHigh(), key)) {
                    return mid;
                }
                else {
                    lb = mid + 1;
                }
            }
            else {
                ub = mid - 1;
            }
        }

        return -1;
    }

    /**
     * Performs binary search on the cached mappings and returns the index of mapping object whose min-value is less than and closest to given key
     * value.
     *
     * @param key
     *            Input key.
     * @return Index of range in the cache which contains the given key.
     */
    private int getIndexOfMappingWithClosestMaxGreaterThanOrEqualToMaxKey(ShardKey key) {
        Set<ShardRange> rangeKeys = mappingsByRange.keySet();

        int lb = 0;
        int ub = rangeKeys.size() - 1;

        while (lb <= ub) {
            int mid = lb + (ub - lb) / 2;

            ShardRange current = (ShardRange) rangeKeys.toArray()[mid];

            if (ShardKey.opGreaterThan(current.getHigh(), key)) {
                if (ShardKey.opLessThanOrEqual(current.getLow(), key)) {
                    return mid;
                }
                else {
                    ub = mid - 1;
                }
            }
            else {
                lb = mid + 1;
            }
        }

        return -1;
    }
}
