package com.microsoft.azure.elasticdb.shard.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * Types of transport protocols supported in SQL Server connections.
 */
public enum SqlProtocol {
    /**
     * Default protocol.
     */
    Default(0),

    /**
     * TCP/IP protocol.
     */
    Tcp(1),

    /**
     * Named pipes protocol.
     */
    NamedPipes(2),

    /**
     * Shared memory protocol.
     */
    SharedMemory(3);

    public static final int SIZE = java.lang.Integer.SIZE;
    private static java.util.HashMap<Integer, SqlProtocol> mappings;
    private int intValue;

    private SqlProtocol(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, SqlProtocol> getMappings() {
        if (mappings == null) {
            synchronized (SqlProtocol.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<Integer, SqlProtocol>();
                }
            }
        }
        return mappings;
    }

    public static SqlProtocol forValue(int value) {
        return getMappings().get(value);
    }

    public int getValue() {
        return intValue;
    }
}
