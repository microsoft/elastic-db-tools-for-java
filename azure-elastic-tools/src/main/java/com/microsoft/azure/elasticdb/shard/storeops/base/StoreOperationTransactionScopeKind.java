package com.microsoft.azure.elasticdb.shard.storeops.base;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Types of transaction scopes used during store operations.
 */
public enum StoreOperationTransactionScopeKind {
    /**
     * Scope of GSM.
     */
    Global(0),

    /**
     * Scope of source LSM.
     */
    LocalSource(1),

    /**
     * Scope of target LSM.
     */
    LocalTarget(2);

    public static final int SIZE = java.lang.Integer.SIZE;
    private static java.util.HashMap<Integer, StoreOperationTransactionScopeKind> mappings;
    private int intValue;

    StoreOperationTransactionScopeKind(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, StoreOperationTransactionScopeKind> getMappings() {
        if (mappings == null) {
            synchronized (StoreOperationTransactionScopeKind.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<>();
                }
            }
        }
        return mappings;
    }

    public static StoreOperationTransactionScopeKind forValue(int value) {
        return getMappings().get(value);
    }

    public int getValue() {
        return intValue;
    }
}
