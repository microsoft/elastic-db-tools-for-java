package com.microsoft.azure.elasticdb.shard.mapper;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

public enum ConnectionOptions {
    /**
     * No operation will be performed on the opened connection.
     */
    None(0),

    /**
     * Validation will be performed on the connection to ensure that
     * the state of the corresponding mapping has not changed since
     * the mapping information was last cached at the client.
     */
    Validate(1);

    public static final int SIZE = java.lang.Integer.SIZE;
    private static java.util.HashMap<Integer, ConnectionOptions> mappings;
    private int intValue;

    private ConnectionOptions(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, ConnectionOptions> getMappings() {
        if (mappings == null) {
            synchronized (ConnectionOptions.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<Integer, ConnectionOptions>();
                }
            }
        }
        return mappings;
    }

    public static ConnectionOptions forValue(int value) {
        return getMappings().get(value);
    }

    public int getValue() {
        return intValue;
    }
}
