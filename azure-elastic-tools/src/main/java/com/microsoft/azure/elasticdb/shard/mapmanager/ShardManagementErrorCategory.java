package com.microsoft.azure.elasticdb.shard.mapmanager;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Represents error categories related to Shard Management operations.
 */
public enum ShardManagementErrorCategory {
    /**
     * ShardMap manager factory.
     */
    ShardMapManagerFactory(0),

    /**
     * ShardMap manager.
     */
    ShardMapManager(1),

    /**
     * ShardMap.
     */
    ShardMap(2),

    /**
     * List shard map.
     */
    ListShardMap(3),

    /**
     * Range shard map.
     */
    RangeShardMap(4),

    /**
     * Version validation.
     */
    Validation(5),

    /**
     * Recovery oriented errors.
     */
    Recovery(6),

    /**
     * Errors related to Schema Info Collection.
     */
    SchemaInfoCollection(7),

    /**
     * General failure category.
     */
    General(8);

    public static final int SIZE = java.lang.Integer.SIZE;
    private static java.util.HashMap<Integer, ShardManagementErrorCategory> mappings;
    private int intValue;

    ShardManagementErrorCategory(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, ShardManagementErrorCategory> getMappings() {
        if (mappings == null) {
            synchronized (ShardManagementErrorCategory.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<>();
                }
            }
        }
        return mappings;
    }

    public static ShardManagementErrorCategory forValue(int value) {
        return getMappings().get(value);
    }

    public int getValue() {
        return intValue;
    }
}
