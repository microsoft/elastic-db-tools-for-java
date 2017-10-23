package com.microsoft.azure.elasticdb.shard.store;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Types of store connections.
 */
public enum StoreConnectionKind {
    /**
     * Connection to GSM.
     */
    Global(0),

    /**
     * Connection to LSM Source Shard.
     */
    LocalSource(1),

    /**
     * Connection to LSM Target Shard (useful for Update Location operation only).
     */
    LocalTarget(2);

    public static final int SIZE = java.lang.Integer.SIZE;
    private static java.util.HashMap<Integer, StoreConnectionKind> mappings;
    private int intValue;

    StoreConnectionKind(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, StoreConnectionKind> getMappings() {
        if (mappings == null) {
            synchronized (StoreConnectionKind.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<>();
                }
            }
        }
        return mappings;
    }

    public static StoreConnectionKind forValue(int value) {
        return getMappings().get(value);
    }

    public int getValue() {
        return intValue;
    }
}
