package com.microsoft.azure.elasticdb.shard.cache;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * Policy for AddOrUpdateMapping operation.
 */
public enum CacheStoreMappingUpdatePolicy {
    /**
     * Overwrite the mapping blindly.
     */
    OverwriteExisting(0),

    /**
     * Keep the original mapping but change TTL.
     */
    UpdateTimeToLive(1);

    public static final int SIZE = java.lang.Integer.SIZE;
    private static java.util.HashMap<Integer, CacheStoreMappingUpdatePolicy> mappings;
    private int intValue;

    private CacheStoreMappingUpdatePolicy(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, CacheStoreMappingUpdatePolicy> getMappings() {
        if (mappings == null) {
            synchronized (CacheStoreMappingUpdatePolicy.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<Integer, CacheStoreMappingUpdatePolicy>();
                }
            }
        }
        return mappings;
    }

    public static CacheStoreMappingUpdatePolicy forValue(int value) {
        return getMappings().get(value);
    }

    public int getValue() {
        return intValue;
    }
}
