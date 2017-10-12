package com.microsoft.azure.elasticdb.shard.schema;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Possible errors encountered by SchemaInfoCollection.
 */
public enum SchemaInfoErrorCode {
    /**
     * No <see cref="SchemaInfo"/> exists with the given name.
     */
    SchemaInfoNameDoesNotExist(0),

    /**
     * A <see cref="SchemaInfo"/> entry with the given name already exists.
     */
    SchemaInfoNameConflict(1),

    /**
     * An entry for the given table already exists in the <see cref="SchemaInfo"/> object.
     */
    TableInfoAlreadyPresent(2);

    public static final int SIZE = java.lang.Integer.SIZE;
    private static java.util.HashMap<Integer, SchemaInfoErrorCode> mappings;
    private int intValue;

    SchemaInfoErrorCode(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, SchemaInfoErrorCode> getMappings() {
        if (mappings == null) {
            synchronized (SchemaInfoErrorCode.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<>();
                }
            }
        }
        return mappings;
    }

    public static SchemaInfoErrorCode forValue(int value) {
        return getMappings().get(value);
    }

    public int getValue() {
        return intValue;
    }
}