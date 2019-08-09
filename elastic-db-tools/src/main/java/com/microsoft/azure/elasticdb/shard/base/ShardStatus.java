package com.microsoft.azure.elasticdb.shard.base;

import java.util.Collections;
import java.util.HashMap;
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
 * Status of a shard.
 */
public enum ShardStatus implements MappableEnum{
    /**
     * Shard is Offline.
     */
    Offline(0),

    /**
     * Shard is Online.
     */
    Online(1);

    private static final Map<Integer, ShardStatus> mappings = EnumHelpers.createMap(ShardStatus.class);
    private int intValue;
    
    ShardStatus(int value) {
        intValue = value;
    }

    public static ShardStatus forValue(int value) {
        return mappings.get(value);
    }

    public int getValue() {
        return intValue;
    }
}
