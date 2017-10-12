package com.microsoft.azure.elasticdb.shard.recovery;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Type of mapping difference. Useful for down casting.
 */
public enum MappingDifferenceType {
    /**
     * Violation associated with ListShardMap.
     */
    List(0),

    /**
     * Violation associated with RangeShardMap.
     */
    Range(1);

    public static final int SIZE = java.lang.Integer.SIZE;
    private static java.util.HashMap<Integer, MappingDifferenceType> mappings;
    private int intValue;

    MappingDifferenceType(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, MappingDifferenceType> getMappings() {
        if (mappings == null) {
            synchronized (MappingDifferenceType.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<>();
                }
            }
        }
        return mappings;
    }

    public static MappingDifferenceType forValue(int value) {
        return getMappings().get(value);
    }

    public int getValue() {
        return intValue;
    }
}
