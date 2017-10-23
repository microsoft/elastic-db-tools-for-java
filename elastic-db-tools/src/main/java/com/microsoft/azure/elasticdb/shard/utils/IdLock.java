package com.microsoft.azure.elasticdb.shard.utils;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import java.util.UUID;

/**
 * Implementation of Id locks. Allows mutual exclusion on Ids.
 */
public final class IdLock extends ValueLock<UUID> {

    /**
     * Instantiates an Id lock with given Id and acquires the name lock.
     *
     * @param id
     *            Given id.
     */
    public IdLock(UUID id) {
        super(id);
    }
}