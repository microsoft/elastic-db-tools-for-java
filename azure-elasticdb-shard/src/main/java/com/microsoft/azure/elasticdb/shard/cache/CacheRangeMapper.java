package com.microsoft.azure.elasticdb.shard.cache;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import javafx.collections.transformation.SortedList;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Cached representation of collection of mappings within shard map.
 * The items consist of a ranges of key values.
 */
public class CacheRangeMapper extends CacheMapper {
    /**
     * Mappings organized by Key Ranges.
     */
    private SortedList<Pair<ShardRange, CacheMapping>> _mappingsByRange;

    /**
     * Constructs the mapper, notes the key type for lookups.
     *
     * @param keyType Key type.
     */
    public CacheRangeMapper(ShardKeyType keyType) {
        super(keyType);
        //TODO: _mappingsByRange = new SortedList<ShardRange, CacheMapping>(Comparer < ShardRange >.Default);
    }

    /**
     * Add or update a mapping in cache.
     *
     * @param sm     Storage mapping object.
     * @param policy Policy to use for preexisting cache entries during update.
     */
    @Override
    public void AddOrUpdate(StoreMapping sm, CacheStoreMappingUpdatePolicy policy) {
        ShardKey min = ShardKey.FromRawValue(this.getKeyType(), sm.getMinValue());

        // Make range out of mapping key ranges.
        ShardRange range = new ShardRange(min, ShardKey.FromRawValue(this.getKeyType(), sm.getMaxValue()));

        CacheMapping cm;
        ICacheStoreMapping csm;

        StoreMapping smDummy = null;

        // We need to update TTL and update entry if:
        // a) We are in update TTL mode
        // b) Mapping exists and same as the one we already have
        // c) Entry is beyond the TTL limit
        ReferenceObjectHelper<StoreMapping> tempRef_smDummy = new ReferenceObjectHelper<StoreMapping>(smDummy);
        if (policy == CacheStoreMappingUpdatePolicy.UpdateTimeToLive && (csm = this.LookupByKey(min, tempRef_smDummy)) != null && csm.getMapping().getId() == sm.getId()) /*&&
                TimerUtils.ElapsedMillisecondsSince(csm.CreationTime) >= csm.TimeToLiveMilliseconds */ {
            //TODO: smDummy = tempRef_smDummy.argValue;
            cm = new CacheMapping(sm, CacheMapper.CalculateNewTimeToLiveMilliseconds(csm));
        } else {
            //TODO: smDummy = tempRef_smDummy.argValue;
            cm = new CacheMapping(sm);
        }

        this.Remove(sm);

        // Add the entry to lookup table by Range.
        _mappingsByRange.add(Pair.of(range, cm));
    }

    /**
     * Remove a mapping object from cache.
     *
     * @param sm Storage maping object.
     *           <p>
     *           Q: Do we ever need to remove multiple entries from the cache which cover the same range?
     *           A: Yes. Imagine that you have some stale mapping in the cache, user just simply performs
     *           an AddRangeMapping operation on a subset of stale mapping range, now you should remove the
     *           stale mapping.
     */
    @Override
    public void Remove(StoreMapping sm) {
        ShardKey minKey = ShardKey.FromRawValue(this.getKeyType(), sm.getMinValue());
        ShardKey maxKey = ShardKey.FromRawValue(this.getKeyType(), sm.getMaxValue());

        // Make range out of mapping key.
        ShardRange range = new ShardRange(minKey, maxKey);

        // Fast code path, where cache does contain the exact range.
        //TODO:
        /*if (_mappingsByRange.ContainsKey(range)) {
            _mappingsByRange.Remove(range);
        } else {
            int indexMin = this.GetIndexOfMappingWithClosestMinLessThanOrEqualToMinKey(minKey);
            int indexMax = this.GetIndexOfMappingWithClosestMaxGreaterThanOrEqualToMaxKey(maxKey);

            if (indexMin < 0) {
                indexMin = 0;
            }

            if (indexMax >= _mappingsByRange.keySet().size()) {
                indexMax = _mappingsByRange.keySet().size() - 1;
            }

            // Find first range with max greater than min key.
            for (; indexMin <= indexMax; indexMin++) {
                ShardRange currentRange = _mappingsByRange.keySet()[indexMin];
                if (currentRange.High > minKey) {
                    break;
                }
            }

            // Find first range with min less than or equal to max key.
            for (; indexMax >= indexMin; indexMax--) {
                ShardRange currentRange = _mappingsByRange.keySet()[indexMax];
                if (currentRange.Low <= maxKey) {
                    break;
                }
            }

            ArrayList<ShardRange> rangesToRemove = new ArrayList<ShardRange>();

            for (; indexMin <= indexMax; indexMin++) {
                rangesToRemove.add(_mappingsByRange.keySet()[indexMin]);
            }

            for (ShardRange rangeToRemove : rangesToRemove) {
                _mappingsByRange.Remove(rangeToRemove);
            }
        }*/
    }

    /**
     * Looks up a mapping by key.
     *
     * @param key Key value.
     * @param sm  Storage mapping object.
     * @return Mapping object which has the key value.
     */
    @Override
    public ICacheStoreMapping LookupByKey(ShardKey key, ReferenceObjectHelper<StoreMapping> sm) {
        CacheMapping cm = null;

        // Performs a binary search in the ranges for key value and
        // then return the result.
        int rangeIndex = this.GetIndexOfMappingContainingShardKey(key);

        //TODO:
        /*if (rangeIndex != -1) {
            ShardRange range = _mappingsByRange.keySet()[rangeIndex];

            cm = _mappingsByRange.getItem(range);

            // DEVNOTE(wbasheer): We should clone the mapping.
            sm.argValue = cm.getMapping();
        } else {
            cm = null;
            sm.argValue = null;
        }*/

        return cm;
    }

    /**
     * Get number of range mappings cached in this mapper.
     *
     * @return Number of cached range mappings.
     */
    @Override
    public long GetMappingsCount() {
        return _mappingsByRange.size();
    }

    /**
     * Clears all the mappings in the lookup by Id table as well
     * as lookup by range table.
     */
    @Override
    protected void Clear() {
        //TODO: _mappingsByRange.Clear();
    }

    /**
     * Performs binary search on the cached mappings and returns the
     * index of mapping object which contains the given key.
     *
     * @param key Input key.
     * @return Index of range in the cache which contains the given key.
     */
    private int GetIndexOfMappingContainingShardKey(ShardKey key) {
        //TODO:
        /* List<ShardRange> rangeKeys = _mappingsByRange.keySet();

        int lb = 0;
        int ub = rangeKeys.size() - 1;

        while (lb <= ub) {
            int mid = lb + (ub - lb) / 2;

            ShardRange current = rangeKeys.get(mid);

            if (current.Contains(key)) {
                return mid;
            } else if (key < current.Low) {
                ub = mid - 1;
            } else {
                lb = mid + 1;
            }
        }*/

        return -1;
    }

    /**
     * Performs binary search on the cached mappings and returns the
     * index of mapping object whose min-value is less than and closest
     * to given key value.
     *
     * @param key Input key.
     * @return Index of range in the cache which contains the given key.
     */
    private int GetIndexOfMappingWithClosestMinLessThanOrEqualToMinKey(ShardKey key) {
        //TODO:
        /*List<ShardRange> rangeKeys = _mappingsByRange.keySet();

        int lb = 0;
        int ub = rangeKeys.size() - 1;

        while (lb <= ub) {
            int mid = lb + (ub - lb) / 2;

            ShardRange current = rangeKeys.get(mid);

            if (current.Low <= key) {
                if (current.High > key) {
                    return mid;
                } else {
                    lb = mid + 1;
                }
            } else {
                ub = mid - 1;
            }
        }*/

        return 0;
    }

    /**
     * Performs binary search on the cached mappings and returns the
     * index of mapping object whose min-value is less than and closest
     * to given key value.
     *
     * @param key Input key.
     * @return Index of range in the cache which contains the given key.
     */
    private int GetIndexOfMappingWithClosestMaxGreaterThanOrEqualToMaxKey(ShardKey key) {
        //TODO:
        /*List<ShardRange> rangeKeys = _mappingsByRange.keySet();

        int lb = 0;
        int ub = rangeKeys.size() - 1;

        while (lb <= ub) {
            int mid = lb + (ub - lb) / 2;

            ShardRange current = rangeKeys.get(mid);

            if (current.High > key) {
                if (current.Low <= key) {
                    return mid;
                } else {
                    ub = mid - 1;
                }
            } else {
                lb = mid + 1;
            }
        }*/

        return 0;
    }
}
