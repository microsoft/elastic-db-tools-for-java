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
    OverwriteExisting(0),

    /**
     * Keep the original mapping but change TTL.
     */
    UpdateTimeToLive(1);

    public static final int SIZE = java.lang.Integer.SIZE;
    private static java.util.HashMap<Integer, CacheStoreMappingUpdatePolicy> mappings;
    private int intValue;

    CacheStoreMappingUpdatePolicy(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, CacheStoreMappingUpdatePolicy> getMappings() {
        if (mappings == null) {
            synchronized (CacheStoreMappingUpdatePolicy.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<>();
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
