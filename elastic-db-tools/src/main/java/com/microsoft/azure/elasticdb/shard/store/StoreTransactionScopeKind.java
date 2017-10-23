package com.microsoft.azure.elasticdb.shard.store;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Type of transaction scope.
 */
public enum StoreTransactionScopeKind {
    /**
     * A non-transactional scope, uses auto-commit transaction mode. Useful for performing operations that are not allowed to be executed within
     * transactions such as Kill connections.
     */
    NonTransactional(0),

    /**
     * Read only transaction scope, uses read-committed transaction mode. Read locks are acquired purely during row read and then released.
     */
    ReadOnly(1),

    /**
     * Read write transaction scope, uses repeatable-read transaction mode. Read locks are held till Commit or Rollback.
     */
    ReadWrite(2);

    public static final int SIZE = java.lang.Integer.SIZE;
    private static java.util.HashMap<Integer, StoreTransactionScopeKind> mappings;
    private int intValue;

    StoreTransactionScopeKind(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, StoreTransactionScopeKind> getMappings() {
        if (mappings == null) {
            synchronized (StoreTransactionScopeKind.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<>();
                }
            }
        }
        return mappings;
    }

    public static StoreTransactionScopeKind forValue(int value) {
        return getMappings().get(value);
    }

    public int getValue() {
        return intValue;
    }
}
