package com.microsoft.azure.elasticdb.shard.mapmanager;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Describes the policy used for initialization of <see cref="ShardMapManager"/> from the store.
 */
public enum ShardMapManagerLoadPolicy {
    /**
     * Load all shard maps and their corresponding mappings into the cache for fast retrieval.
     */
    Eager,

    /**
     * Load all shard maps and their corresponding mappings on as needed basis.
     */
    Lazy;

}
