package com.microsoft.azure.elasticdb.shardmapscalability;

import java.util.Map;

import com.microsoft.azure.elasticdb.core.commons.helpers.EnumHelpers;
import com.microsoft.azure.elasticdb.core.commons.helpers.MappableEnum;

/*
 * Copyright (c) Microsoft. All rights reserved. Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

public enum ShardMapType implements MappableEnum{
    ListShardMap(0),
    RangeShardMap(1);

    private static final Map<Integer, ShardMapType> mappings =EnumHelpers.createMap(ShardMapType.class);
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
