package com.microsoft.azure.elasticdb.shard.base;

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
 * Types of transport protocols supported in SQL Server connections.
 */
public enum SqlProtocol implements MappableEnum {
    /**
     * Default protocol.
     */
    @XmlEnumValue("0")
    Default(0),

    /**
     * TCP/IP protocol.
     */
    @XmlEnumValue("1")
    Tcp(1),

    /**
     * Named pipes protocol.
     */
    @XmlEnumValue("2")
    NamedPipes(2),

    /**
     * Shared memory protocol.
     */
    @XmlEnumValue("3")
    SharedMemory(3);

    private static final Map<Integer, SqlProtocol> mappings = EnumHelpers.createMap(SqlProtocol.class);
    private int intValue;

    SqlProtocol(int value) {
        intValue = value;
    }

    public static SqlProtocol forValue(int value) {
        return mappings.get(value);
    }

    public int getValue() {
        return intValue;
    }
}
