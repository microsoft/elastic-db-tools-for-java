package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.storeops.base.ShardLocation;

import java.util.*;

/**
 * Storage representation of a single shard.
 */
public interface IStoreShard {
    /**
     * Shard Id.
     */
    UUID getId();

    /**
     * Shard version.
     */
    UUID getVersion();

    /**
     * Containing shard map's Id.
     */
    UUID getShardMapId();

    /**
     * Data source location.
     */
    ShardLocation getLocation();

    /**
     * Shard status.
     */
    int getStatus();
}