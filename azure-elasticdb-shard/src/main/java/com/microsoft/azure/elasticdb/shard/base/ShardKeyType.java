package com.microsoft.azure.elasticdb.shard.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * Type of shard key. Currently, only Int32, Int64, Guid and byte[] are the data types supported as shard keys.
 */
public enum ShardKeyType {
    /**
     * No type specified.
     */
    None(0),

    /**
     * 32-bit integral value.
     */
    Int32(1),

    /**
     * 64-bit integral value.
     */
    Int64(2),

    /**
     * UniqueIdentifier value.
     */
    Guid(3),

    /**
     * Array of bytes value.
     */
    Binary(4),

    /**
     * Date and time value.
     */
    DateTime(5),

    /**
     * Time value.
     */
    TimeSpan(6),

    /**
     * Date and time value with offset.
     */
    DateTimeOffset(7);

    public static final int SIZE = java.lang.Integer.SIZE;
    private static java.util.HashMap<Integer, ShardKeyType> mappings;
    private int intValue;

    private ShardKeyType(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, ShardKeyType> getMappings() {
        if (mappings == null) {
            synchronized (ShardKeyType.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<Integer, ShardKeyType>();
                }
            }
        }
        return mappings;
    }

    public static ShardKeyType forValue(int value) {
        return getMappings().get(value);
    }

    public int getValue() {
        return intValue;
    }
}
