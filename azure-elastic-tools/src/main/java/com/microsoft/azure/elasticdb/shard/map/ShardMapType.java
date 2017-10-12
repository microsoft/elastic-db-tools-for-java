package com.microsoft.azure.elasticdb.shard.map;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import javax.xml.bind.annotation.XmlEnumValue;

/**
 * Type of shard map.
 */
public enum ShardMapType {
    /**
     * Invalid kind of shard map. Only used for serialization/deserialization.
     */
    @XmlEnumValue("0")
    None(0),

    /**
     * Shard map with list based mappings.
     */
    @XmlEnumValue("1")
    List(1),

    /**
     * Shard map with range based mappings.
     */
    @XmlEnumValue("2")
    Range(2);

    public static final int SIZE = Integer.SIZE;
    private static java.util.HashMap<Integer, ShardMapType> mappings;
    private int intValue;

    ShardMapType(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, ShardMapType> getMappings() {
        if (mappings == null) {
            synchronized (ShardMapType.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<>();
                }
            }
        }
        return mappings;
    }

    public static ShardMapType forValue(int value) {
        return getMappings().get(value);
    }

    public int getValue() {
        return intValue;
    }
}