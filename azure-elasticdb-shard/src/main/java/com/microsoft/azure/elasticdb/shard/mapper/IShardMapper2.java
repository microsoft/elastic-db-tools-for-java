package com.microsoft.azure.elasticdb.shard.mapper;

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.base.IShardProvider;

import java.util.UUID;

/**
 * Holder of keys to shards mappings and provides operations over such mappings.
 * <p>
 * <typeparam name="TMapping">Type of individual mapping.</typeparam>
 * <typeparam name="TValue">Type of values mapped to shards in a mapping.</typeparam>
 * <typeparam name="TKey">Key type.</typeparam>
 */
public interface IShardMapper2<TMapping extends IShardProvider<TValue>, TValue, TKey> extends IShardMapper1<TKey> {
    /**
     * Adds a mapping.
     *
     * @param mapping Mapping being added.
     */
    TMapping Add(TMapping mapping);

    /**
     * Removes a mapping.
     *
     * @param mapping     Mapping being removed.
     * @param lockOwnerId Lock owner id of the mapping
     */
    void Remove(TMapping mapping, UUID lockOwnerId);

    /**
     * Looks up the key value and returns the corresponding mapping.
     *
     * @param key      Input key value.
     * @param useCache Whether to use cache for lookups.
     * @return Mapping that contains the key value.
     */
    TMapping Lookup(TKey key, boolean useCache);

    /**
     * Tries to looks up the key value and returns the corresponding mapping.
     *
     * @param key      Input key value.
     * @param useCache Whether to use cache for lookups.
     * @param mapping  Mapping that contains the key value.
     * @return <c>true</c> if mapping is found, <c>false</c> otherwise.
     */
    boolean TryLookup(TKey key, boolean useCache, ReferenceObjectHelper<TMapping> mapping);
}