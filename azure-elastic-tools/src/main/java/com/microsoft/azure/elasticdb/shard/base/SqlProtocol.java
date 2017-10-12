package com.microsoft.azure.elasticdb.shard.base;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import javax.xml.bind.annotation.XmlEnumValue;

/**
 * Types of transport protocols supported in SQL Server connections.
 */
public enum SqlProtocol {
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

    public static final int SIZE = java.lang.Integer.SIZE;
    private static java.util.HashMap<Integer, SqlProtocol> mappings;
    private int intValue;

    SqlProtocol(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, SqlProtocol> getMappings() {
        if (mappings == null) {
            synchronized (SqlProtocol.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<>();
                }
            }
        }
        return mappings;
    }

    public static SqlProtocol forValue(int value) {
        return getMappings().get(value);
    }

    public int getValue() {
        return intValue;
    }
}
