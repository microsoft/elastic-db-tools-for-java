package com.microsoft.azure.elasticdb.shard.mapmanager;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Describes the creation options for shard map manager storage representation.
 */
public enum ShardMapManagerCreateMode {
    /**
     * If the shard map manager data structures are already present in the store, then this method will raise exception.
     */
    KeepExisting(0),

    /**
     * If the shard map manager data structures are already present in the store, then this method will overwrite them.
     */
    ReplaceExisting(1);

    public static final int SIZE = java.lang.Integer.SIZE;
    private static java.util.HashMap<Integer, ShardMapManagerCreateMode> mappings;
    private int intValue;

    ShardMapManagerCreateMode(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, ShardMapManagerCreateMode> getMappings() {
        if (mappings == null) {
            synchronized (ShardMapManagerCreateMode.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<>();
                }
            }
        }
        return mappings;
    }

    public static ShardMapManagerCreateMode forValue(int value) {
        return getMappings().get(value);
    }

    public int getValue() {
        return intValue;
    }
}
