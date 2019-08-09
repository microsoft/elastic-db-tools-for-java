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
 * Records the updated properties for a mapping update object.
 */
public enum MappingUpdatedProperties implements MappableEnum{
    Status(1),
    // Only applicable for point and range update.
    Shard(2),
    //TODO: Use EnumSet instead
    All(1 | 2);

    public static final int SIZE = java.lang.Integer.SIZE;
    private static final Map<Integer, MappingUpdatedProperties> mappings = EnumHelpers.createMap(MappingUpdatedProperties.class);
    private int intValue;

    MappingUpdatedProperties(int value) {
        intValue = value;
    }

    public static MappingUpdatedProperties forValue(int value) {
        return mappings.get(value);
    }

    public int getValue() {
        return intValue;
    }
}
