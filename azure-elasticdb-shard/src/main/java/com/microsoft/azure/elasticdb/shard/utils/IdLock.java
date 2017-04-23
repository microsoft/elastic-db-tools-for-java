package com.microsoft.azure.elasticdb.shard.utils;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import java.util.UUID;

/**
 * Implementation of Id locks. Allows mutual exclusion on Ids.
 */
public final class IdLock extends ValueLock<UUID> {
    /**
     * Instantiates an Id lock with given Id and acquires the name lock.
     *
     * @param id Given id.
     */
    public IdLock(UUID id) {
        super(id);
    }
}