package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import java.util.HashMap;

/**
 * Numeric storage operation result.
 * Keep these in sync with GSM and LSM stored procs.
 */
public enum StoreResult {
    Failure(0), // Generic failure.

    Success(1),

    MissingParametersForStoredProcedure(50),
    StoreVersionMismatch(51),
    ShardPendingOperation(52),
    UnexpectedStoreError(53),

    ShardMapExists(101),
    ShardMapDoesNotExist(102),
    ShardMapHasShards(103),

    ShardExists(201),
    ShardDoesNotExist(202),
    ShardHasMappings(203),
    ShardVersionMismatch(204),
    ShardLocationExists(205),

    MappingDoesNotExist(301),
    MappingRangeAlreadyMapped(302),
    MappingPointAlreadyMapped(303),
    MappingNotFoundForKey(304),
    UnableToKillSessions(305),
    MappingIsNotOffline(306),
    MappingLockOwnerIdDoesNotMatch(307),
    MappingIsAlreadyLocked(308),
    MappingIsOffline(309),

    SchemaInfoNameDoesNotExist(401),
    SchemaInfoNameConflict(402);

    public static final int SIZE = Integer.SIZE;
    private static HashMap<Integer, StoreResult> mappings;
    private int intValue;

    private StoreResult(int value) {
        intValue = value;
        getMappings().put(value, this);
    }

    private static HashMap<Integer, StoreResult> getMappings() {
        if (mappings == null) {
            synchronized (StoreResult.class) {
                if (mappings == null) {
                    mappings = new HashMap<Integer, StoreResult>();
                }
            }
        }
        return mappings;
    }

    public static StoreResult forValue(int value) {
        return getMappings().get(value);
    }

    public int getValue() {
        return intValue;
    }
}