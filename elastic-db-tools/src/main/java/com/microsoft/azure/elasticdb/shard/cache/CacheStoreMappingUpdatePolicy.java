package com.microsoft.azure.elasticdb.shard.cache;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Policy for AddOrUpdateMapping operation.
 */
public enum CacheStoreMappingUpdatePolicy {
    /**
     * Overwrite the mapping blindly.
     */
    OverwriteExisting,

    /**
     * Keep the original mapping but change TTL.
     */
    UpdateTimeToLive;

}
