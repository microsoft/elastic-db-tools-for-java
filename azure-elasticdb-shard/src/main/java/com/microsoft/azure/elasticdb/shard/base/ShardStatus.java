package com.microsoft.azure.elasticdb.shard.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

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

    private ShardStatus(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, ShardStatus> getMappings() {
        if (mappings == null) {
            synchronized (ShardStatus.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<Integer, ShardStatus>();
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