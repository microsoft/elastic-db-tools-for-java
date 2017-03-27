package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.utils.Version;

/**
 * Storage representation of shard map manager version
 */
public interface IStoreVersion {
    /**
     * Store version information.
     */
    Version getVersion();
}