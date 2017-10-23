package com.microsoft.azure.elasticdb.shard.base;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Set of operations that can be performed on mappings with lockOwnerId.
 */
public enum LockOwnerIdOpType {
    /**
     * Lock the range mapping with the given lockOwnerId.
     */
    Lock(0),

    /**
     * Unlock the range mapping that has the given lockOwnerId.
     */
    UnlockMappingForId(1),

    /**
     * Unlock all the range mappings that have the given lockOwnerId.
     */
    UnlockAllMappingsForId(2),

    /**
     * Unlock all locked range mappings.
     */
    UnlockAllMappings(3);

    public static final int SIZE = java.lang.Integer.SIZE;
    private static java.util.HashMap<Integer, LockOwnerIdOpType> mappings;
    private int intValue;

    LockOwnerIdOpType(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, LockOwnerIdOpType> getMappings() {
        if (mappings == null) {
            synchronized (LockOwnerIdOpType.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<>();
                }
            }
        }
        return mappings;
    }

    public static LockOwnerIdOpType forValue(int value) {
        return getMappings().get(value);
    }

    public int getValue() {
        return intValue;
    }
}
