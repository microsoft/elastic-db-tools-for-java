package com.microsoft.azure.elasticdb.query.logging;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Defines the available options when executing commands against multiple shards. This enumeration has a flags attribute.
 */
public enum MultiShardExecutionOptions {
    /**
     * , Execute without any options enabled.
     */
    None(0),

    /**
     * Whether the $ShardName pseudo column should be included in the result-sets.
     */
    IncludeShardNameColumn(1);

    public static final int SIZE = java.lang.Integer.SIZE;
    private static java.util.HashMap<Integer, MultiShardExecutionOptions> mappings;
    private int intValue;

    MultiShardExecutionOptions(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, MultiShardExecutionOptions> getMappings() {
        if (mappings == null) {
            synchronized (MultiShardExecutionOptions.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<>();
                }
            }
        }
        return mappings;
    }

    public static MultiShardExecutionOptions forValue(int value) {
        return getMappings().get(value);
    }

    public int getValue() {
        return intValue;
    }
}
