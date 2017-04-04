package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;

/**
 * Storage representation of a single location.
 */
public interface IStoreLocation {
    /**
     * Data source location.
     */
    ShardLocation getLocation();
}