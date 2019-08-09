package com.microsoft.azure.elasticdb.shard.storeops.base;

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
 * Operation codes identifying various store operations.
 */
public enum StoreOperationCode implements MappableEnum{
    @XmlEnumValue("1")
    AddShard(1),
    @XmlEnumValue("2")
    RemoveShard(2),
    @XmlEnumValue("3")
    UpdateShard(3),

    @XmlEnumValue("4")
    AddPointMapping(4),
    @XmlEnumValue("5")
    RemovePointMapping(5),
    @XmlEnumValue("6")
    UpdatePointMapping(6),
    @XmlEnumValue("7")
    UpdatePointMappingWithOffline(7),

    @XmlEnumValue("8")
    AddRangeMapping(8),
    @XmlEnumValue("9")
    RemoveRangeMapping(9),
    @XmlEnumValue("10")
    UpdateRangeMapping(10),
    @XmlEnumValue("11")
    UpdateRangeMappingWithOffline(11),

    @XmlEnumValue("14")
    SplitMapping(14),
    @XmlEnumValue("15")
    MergeMappings(15),

    @XmlEnumValue("16")
    AttachShard(16);

    private static final Map<Integer, StoreOperationCode> mappings = EnumHelpers.createMap(StoreOperationCode.class);
    private int intValue;

    StoreOperationCode(int value) {
        intValue = value;
    }

    public static StoreOperationCode forValue(int value) {
        return mappings.get(value);
    }

    public int getValue() {
        return intValue;
    }
}
