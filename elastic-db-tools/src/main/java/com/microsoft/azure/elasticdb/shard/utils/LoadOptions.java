package com.microsoft.azure.elasticdb.shard.utils;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Specifies load options when parsing XML.
 */
public enum LoadOptions {
    // Does not preserve insignificant white space or load base URI and line information.
    None(0),

    // Preserves insignificant white space while parsing.
    PreserveWhitespace(1),

    // Requests the base URI information and makes it available via the BaseUri property.
    SetBaseUri(2),

    // Requests the line information and makes it available via properties on an Object.
    SetLineInfo(4);

    public static final int SIZE = Integer.SIZE;
    private static java.util.HashMap<Integer, LoadOptions> mappings;
    private int intValue;

    LoadOptions(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, LoadOptions> getMappings() {
        if (mappings == null) {
            synchronized (LoadOptions.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<>();
                }
            }
        }
        return mappings;
    }

    public static LoadOptions forValue(int value) {
        return getMappings().get(value);
    }

    public int getValue() {
        return intValue;
    }
}
