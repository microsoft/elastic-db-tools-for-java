package com.microsoft.azure.elasticdb.shard.storeops.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

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

    private StoreOperationTransactionScopeKind(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, StoreOperationTransactionScopeKind> getMappings() {
        if (mappings == null) {
            synchronized (StoreOperationTransactionScopeKind.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<Integer, StoreOperationTransactionScopeKind>();
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
