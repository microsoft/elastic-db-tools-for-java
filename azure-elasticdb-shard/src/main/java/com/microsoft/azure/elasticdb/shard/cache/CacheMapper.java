package com.microsoft.azure.elasticdb.shard.cache;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.store.IStoreMapping;

/**
 * Cached representation of collection of mappings within shard map.
 * Derived classes implement either list or range functionality.
 */
public abstract class CacheMapper {
    /**
     * Key type, usable by lookups by key in derived classes.
     */
    private ShardKeyType KeyType;

    /**
     * Constructs the mapper, notes the key type for lookups.
     *
     * @param keyType Key type.
     */
    public CacheMapper(ShardKeyType keyType) {
        this.setKeyType(keyType);
    }

    /**
     * Given current value of TTL, calculates the next TTL value in milliseconds.
     *
     * @param csm Current cached mapping object.
     * @return New TTL value.
     */
    protected static long CalculateNewTimeToLiveMilliseconds(ICacheStoreMapping csm) {
        if (csm.getTimeToLiveMilliseconds() <= 0) {
            return 5000;
        }

        // Exponentially increase the time up to a limit of 30 seconds, after which we keep
        // returning 30 seconds as the TTL for the mapping entry.
        return Math.min(30000, csm.getTimeToLiveMilliseconds() * 2);
    }

    protected final ShardKeyType getKeyType() {
        return KeyType;
    }

    private void setKeyType(ShardKeyType value) {
        KeyType = value;
    }

    /**
     * Add or update a mapping in cache.
     *
     * @param sm     Storage mapping object.
     * @param policy Policy to use for preexisting cache entries during update.
     */
    public abstract void AddOrUpdate(IStoreMapping sm, CacheStoreMappingUpdatePolicy policy);

    /**
     * Remove a mapping object from cache.
     *
     * @param sm Storage maping object.
     */
    public abstract void Remove(IStoreMapping sm);

    /**
     * Looks up a mapping by key.
     *
     * @param key Key value.
     * @param sm  Storage mapping object.
     * @return Mapping object which has the key value.
     */
    public abstract ICacheStoreMapping LookupByKey(ShardKey key, ReferenceObjectHelper<IStoreMapping> sm);

    /**
     * Gets mappings dictionary size.
     *
     * @return Number of mappings cached in the dictionary.
     */
    public abstract long GetMappingsCount();

    /**
     * Clears all the mappings in the lookup by Id table.
     */
    protected abstract void Clear();
}