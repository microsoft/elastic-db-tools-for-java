package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import java.util.UUID;

/**
 * Storage representation of a mapping b/w key ranges and shards.
 */
public interface IStoreMapping {
    /**
     * Mapping Id.
     */
    UUID getId();

    /**
     * Shard map Id.
     */
    UUID getShardMapId();

    /**
     * Min value.
     */
    byte[] getMinValue();

    /**
     * Max value.
     */
    byte[] getMaxValue();

    /**
     * Mapping status.
     */
    int getStatus();

    /**
     * Lock owner id of this mapping
     */
    UUID getLockOwnerId();

    /**
     * Shard referenced by mapping.
     */
    IStoreShard getStoreShard();
}