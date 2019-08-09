package com.microsoft.azure.elasticdb.query.logging;

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
 * Defines the available options when executing commands against multiple shards. This enumeration has a flags attribute.
 */
public enum MultiShardExecutionOptions implements MappableEnum{
    /**
     * , Execute without any options enabled.
     */
    None(0),

    /**
     * Whether the $ShardName pseudo column should be included in the result-sets.
     */
    IncludeShardNameColumn(1);

    public static final int SIZE = java.lang.Integer.SIZE;
    private static final Map<Integer, MultiShardExecutionOptions> mappings = EnumHelpers.createMap(MultiShardExecutionOptions.class);
    private int intValue;

    MultiShardExecutionOptions(int value) {
        intValue = value;
    }

    public static MultiShardExecutionOptions forValue(int value) {
        return mappings.get(value);
    }

    public int getValue() {
        return intValue;
    }
}
