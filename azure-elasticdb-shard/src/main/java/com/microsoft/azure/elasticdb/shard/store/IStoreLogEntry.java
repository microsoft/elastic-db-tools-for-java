package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationCode;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationState;

import java.sql.SQLXML;
import java.util.UUID;

/**
 * Represents a store operation.
 */
public interface IStoreLogEntry {
    /**
     * Identity of operation.
     */
    UUID getId();

    /**
     * Operation code. Helps in deserialization during factory method.
     */
    StoreOperationCode getOpCode();

    /**
     * Serialized representation of the operation.
     */
    SQLXML getData();

    /**
     * State from which Undo will start.
     */
    StoreOperationState getUndoStartState();

    /**
     * Original shard version for remove steps.
     */
    UUID getOriginalShardVersionRemoves();

    /**
     * Original shard version for add steps.
     */
    UUID getOriginalShardVersionAdds();
}