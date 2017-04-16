package com.microsoft.azure.elasticdb.shard.storeops.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * Operation codes identifying various store operations.
 */
public enum StoreOperationCode {
    AddShard(1),
    RemoveShard(2),
    UpdateShard(3),

    AddPointMapping(4),
    RemovePointMapping(5),
    UpdatePointMapping(6),
    UpdatePointMappingWithOffline(7),

    AddRangeMapping(8),
    RemoveRangeMapping(9),
    UpdateRangeMapping(10),
    UpdateRangeMappingWithOffline(11),

    SplitMapping(14),
    MergeMappings(15),

    AttachShard(16);

    public static final int SIZE = java.lang.Integer.SIZE;
    private static java.util.HashMap<Integer, StoreOperationCode> mappings;
    private int intValue;

    private StoreOperationCode(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static java.util.HashMap<Integer, StoreOperationCode> getMappings() {
        if (mappings == null) {
            synchronized (StoreOperationCode.class) {
                if (mappings == null) {
                    mappings = new java.util.HashMap<Integer, StoreOperationCode>();
                }
            }
        }
        return mappings;
    }

    public static StoreOperationCode forValue(int value) {
        return getMappings().get(value);
    }

    public int getValue() {
        return intValue;
    }
}
