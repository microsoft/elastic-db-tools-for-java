package com.microsoft.azure.elasticdb.shard.map;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * Type of shard map.
 */
public enum ShardMapType {
    /**
     * Invalid kind of shard map. Only used for serialization/deserialization.
     */
    None(0),

    /**
     * Shard map with list based mappings.
     */
    List(1),

    /**
     * Shard map with range based mappings.
     */
    Range(2);

    public static final int SIZE = Integer.SIZE;
    private static java.util.HashMap<Integer, ShardMapType> mappings;
    private int intValue;

    private ShardMapType(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, ShardMapType> getMappings() {
        if (mappings == null) {
            synchronized (ShardMapType.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<Integer, ShardMapType>();
                }
            }
        }
        return mappings;
    }

    public static ShardMapType forValue(int value) {
        return getMappings().get(value);
    }

    public int getValue() {
        return intValue;
    }
}