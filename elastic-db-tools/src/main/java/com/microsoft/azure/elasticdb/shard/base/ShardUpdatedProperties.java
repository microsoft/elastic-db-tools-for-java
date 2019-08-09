package com.microsoft.azure.elasticdb.shard.base;

import java.util.HashMap;
import java.util.Map;

import com.microsoft.azure.elasticdb.core.commons.helpers.MappableEnum;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Records the updated properties on the shard.
 */
public enum ShardUpdatedProperties implements MappableEnum {
    Status(1),
    All(1);

    private static final Map<Integer, ShardUpdatedProperties> mappings = new HashMap<>();
    static {
    	//TODO: Things are quite messed up here.
    	mappings.put(1, All);
    }
    private int intValue;

    ShardUpdatedProperties(int value) {
        intValue = value;
    }

    public static ShardUpdatedProperties forValue(int value) {
        return mappings.get(value);
    }

    public int getValue() {
        return intValue;
    }
}
