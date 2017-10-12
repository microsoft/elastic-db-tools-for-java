package com.microsoft.azure.elasticdb.shard.cache;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.shard.store.StoreMapping;

/**
 * Represents a cache entry for a mapping.
 */
public interface ICacheStoreMapping {

    /**
     * Store representation of mapping.
     */
    StoreMapping getMapping();

    /**
     * Mapping entry creation time.
     */
    long getCreationTime();

    /**
     * Mapping entry expiration time.
     */
    long getTimeToLiveMilliseconds();

    /**
     * Resets the mapping entry expiration time to 0.
     */
    void resetTimeToLive();

    /**
     * Whether TimeToLiveMilliseconds have elapsed since CreationTime.
     */
    boolean hasTimeToLiveExpired();
}
