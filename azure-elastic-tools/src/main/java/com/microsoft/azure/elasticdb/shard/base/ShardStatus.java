package com.microsoft.azure.elasticdb.shard.base;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Status of a shard.
 */
public enum ShardStatus {
    /**
     * Shard is Offline.
     */
    Offline(0),

    /**
     * Shard is Online.
     */
    Online(1);

    public static final int SIZE = Integer.SIZE;
    private static java.util.HashMap<Integer, ShardStatus> mappings;
    private int intValue;

    ShardStatus(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, ShardStatus> getMappings() {
        if (mappings == null) {
            synchronized (ShardStatus.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<>();
                }
            }
        }
        return mappings;
    }

    public static ShardStatus forValue(int value) {
        return getMappings().get(value);
    }

    public int getValue() {
        return intValue;
    }
}