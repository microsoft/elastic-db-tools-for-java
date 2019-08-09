package com.microsoft.azure.elasticdb.shard.map;

import java.util.Map;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import javax.xml.bind.annotation.XmlEnumValue;

import com.microsoft.azure.elasticdb.core.commons.helpers.EnumHelpers;
import com.microsoft.azure.elasticdb.core.commons.helpers.MappableEnum;

/**
 * Type of shard map.
 */
public enum ShardMapType implements MappableEnum {
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

    private static final Map<Integer, ShardMapType> mappings = EnumHelpers.createMap(ShardMapType.class);
    private int intValue;

    ShardMapType(int value) {
        intValue = value;
    }

    public static ShardMapType forValue(int value) {
        return mappings.get(value);
    }

    public int getValue() {
        return intValue;
    }
}