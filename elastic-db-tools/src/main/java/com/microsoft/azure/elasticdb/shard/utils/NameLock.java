package com.microsoft.azure.elasticdb.shard.utils;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Implementation of name locks. Allows mutual exclusion on names.
 */
public final class NameLock extends ValueLock<String> {

    /**
     * Instantiates a name lock with given name and acquires the name lock.
     *
     * @param name
     *            Given name.
     */
    public NameLock(String name) {
        super(name);
    }
}