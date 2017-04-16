package com.microsoft.azure.elasticdb.shard.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * Status of a mapping.
 */
public enum MappingStatus {
    /**
     * Mapping is Offline.
     */
    Offline(0),

    /**
     * Mapping is Online.
     */
    Online(1);

    public static final int SIZE = java.lang.Integer.SIZE;
    private static java.util.HashMap<Integer, MappingStatus> mappings;
    private int intValue;

    private MappingStatus(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, MappingStatus> getMappings() {
        if (mappings == null) {
            synchronized (MappingStatus.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<Integer, MappingStatus>();
                }
            }
        }
        return mappings;
    }

    public static MappingStatus forValue(int value) {
        return getMappings().get(value);
    }

    public int getValue() {
        return intValue;
    }
}