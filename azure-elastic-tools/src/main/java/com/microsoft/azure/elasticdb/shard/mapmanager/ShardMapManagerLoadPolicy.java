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
    Eager(0),

    /**
     * Load all shard maps and their corresponding mappings on as needed basis.
     */
    Lazy(1);

    public static final int SIZE = java.lang.Integer.SIZE;
    private static java.util.HashMap<Integer, ShardMapManagerLoadPolicy> mappings;
    private int intValue;

    ShardMapManagerLoadPolicy(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, ShardMapManagerLoadPolicy> getMappings() {
        if (mappings == null) {
            synchronized (ShardMapManagerLoadPolicy.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<>();
                }
            }
        }
        return mappings;
    }

    public static ShardMapManagerLoadPolicy forValue(int value) {
        return getMappings().get(value);
    }

    public int getValue() {
        return intValue;
    }
}
