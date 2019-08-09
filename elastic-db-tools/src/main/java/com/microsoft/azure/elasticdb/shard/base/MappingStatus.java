package com.microsoft.azure.elasticdb.shard.base;

import java.util.Map;

import com.microsoft.azure.elasticdb.core.commons.helpers.EnumHelpers;
import com.microsoft.azure.elasticdb.core.commons.helpers.MappableEnum;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Status of a mapping.
 */
public enum MappingStatus implements MappableEnum{
    /**
     * Mapping is Offline.
     */
    Offline(0),

    /**
     * Mapping is Online.
     */
    Online(1);

    public static final int SIZE = java.lang.Integer.SIZE;
    private static final Map<Integer, MappingStatus> mappings = EnumHelpers.createMap(MappingStatus.class);
    private int intValue;

    MappingStatus(int value) {
        intValue = value;
    }

    public static MappingStatus forValue(int value) {
        return mappings.get(value);
    }

    public int getValue() {
        return intValue;
    }
}