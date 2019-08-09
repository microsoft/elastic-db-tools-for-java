package com.microsoft.azure.elasticdb.shard.mapper;

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

public enum ConnectionOptions 
//TODO: can be replaced by EnumSet
implements MappableEnum{
    /**
     * No operation will be performed on the opened connection.
     */
    None(0),

    /**
     * Validation will be performed on the connection to ensure that the state of the corresponding mapping has not changed since the mapping
     * information was last cached at the client.
     */
    Validate(1);

    private static Map<Integer, ConnectionOptions> mappings = EnumHelpers.createMap(ConnectionOptions.class);
    private int intValue;

    ConnectionOptions(int value) {
        intValue = value;
    }

    
    public static ConnectionOptions forValue(int value) {
        return mappings.get(value);
    }

    public int getValue() {
        return intValue;
    }
}
