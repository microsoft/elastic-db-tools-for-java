package com.microsoft.azure.elasticdb.shard.base;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Types of supported mappings.
 */
public enum MappingKind {
    PointMapping(0),
    RangeMapping(1);

    public static final int SIZE = java.lang.Integer.SIZE;
    private static java.util.HashMap<Integer, MappingKind> mappings;
    private int intValue;

    MappingKind(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, MappingKind> getMappings() {
        if (mappings == null) {
            synchronized (MappingKind.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<>();
                }
            }
        }
        return mappings;
    }

    public static MappingKind forValue(int value) {
        return getMappings().get(value);
    }

    public int getValue() {
        return intValue;
    }
}
