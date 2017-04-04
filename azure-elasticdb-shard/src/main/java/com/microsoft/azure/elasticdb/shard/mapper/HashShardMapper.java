package com.microsoft.azure.elasticdb.shard.mapper;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

//#if FUTUREWORK

import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;

import java.util.function.Function;

/**
 * Mapper that maps ranges of hashed key values to shards.
 * <p>
 * <typeparam name="T">Key type.</typeparam>
 * <typeparam name="U">Hashed key type.</typeparam>
 */
public final class HashShardMapper<T, U> extends RangeShardMapper<T, U> {
    /**
     * Hash function.
     */
    private Function<T, U> HashFunction;

    /**
     * Hash shard mapper, which managers hashed ranges.
     *
     * @param manager Reference to ShardMapManager.
     * @param sm      Containing shard map.
     */
    public HashShardMapper(ShardMapManager manager, ShardMap sm) {
        super(manager, sm);
    }

    public Function<T, U> getHashFunction() {
        return HashFunction;
    }

    /**
     * Function used to perform conversion of key type T to range type U.
     *
     * @param key Input key.
     * @return Mapped value of key.
     */
    @Override
    protected U MapKeyTypeToRangeType(T key) {
        assert this.getHashFunction() != null;
        return this.HashFunction(key);
    }
}
//#endif
