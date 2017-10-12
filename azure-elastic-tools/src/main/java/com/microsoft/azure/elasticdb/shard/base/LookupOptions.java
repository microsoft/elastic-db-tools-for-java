package com.microsoft.azure.elasticdb.shard.base;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import java.util.HashMap;

/**
 * Specifies where mapping lookup operations will search for mappings.
 */
public class LookupOptions {

    /**
     * Default invalid kind of lookup options.
     */
    public static final LookupOptions NONE = new LookupOptions(0);

    /**
     * Attempt to lookup in the local cache. If LookupInCache and LookupInStore are both specified, the cache will be searched first, then the store.
     */
    public static final LookupOptions LOOKUP_IN_CACHE = new LookupOptions(1);

    /**
     * Attempt to lookup in the global shard map store. If LookupInCache and LookupInStore are both specified, the cache will be searched first, then
     * the store.
     */
    public static final LookupOptions LOOKUP_IN_STORE = new LookupOptions(1 << 2);

    public static final int SIZE = java.lang.Integer.SIZE;
    private static HashMap<Integer, LookupOptions> mappings;
    private int intValue;

    private LookupOptions(int value) {
        intValue = value;
        synchronized (LookupOptions.class) {
            getMappings().put(value, this);
        }
    }

    private static HashMap<Integer, LookupOptions> getMappings() {
        if (mappings == null) {
            synchronized (LookupOptions.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<>();
                }
            }
        }
        return mappings;
    }

    /**
     * Lookup Options for an Integer Value.
     *
     * @param value
     *            Input integer value
     * @return Lookup Option
     */
    public static LookupOptions forValue(int value) {
        synchronized (LookupOptions.class) {
            LookupOptions enumObj = getMappings().get(value);
            if (enumObj == null) {
                return new LookupOptions(value);
            }
            else {
                return enumObj;
            }
        }
    }

    public int getValue() {
        return intValue;
    }
}
