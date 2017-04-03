package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.map.ShardMapType;

import java.util.UUID;

/**
 * Store representation of a shard map.
 */
public interface IStoreShardMap {
    /**
     * Shard map's identity.
     */
    UUID getId();

    /**
     * Shard map name.
     */
    String getName();

    /**
     * Type of shard map.
     */
    ShardMapType getMapType();

    /**
     * Key type.
     */
    ShardKeyType getKeyType();
}