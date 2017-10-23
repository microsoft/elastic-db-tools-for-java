package com.microsoft.azure.elasticdb.shardmapscalability;

/*
 * Copyright (c) Microsoft. All rights reserved. Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

public enum ShardMapType {
    ListShardMap(0),
    RangeShardMap(1);

    public static final int SIZE = java.lang.Integer.SIZE;
    private static java.util.HashMap<Integer, ShardMapType> mappings;
    private int intValue;

    ShardMapType(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, ShardMapType> getMappings() {
        if (mappings == null) {
            synchronized (ShardMapType.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<>();
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
