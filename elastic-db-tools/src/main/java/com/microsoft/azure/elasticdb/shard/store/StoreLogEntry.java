package com.microsoft.azure.elasticdb.shard.store;

import java.util.UUID;

import org.w3c.dom.Element;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationCode;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationState;

/**
 * Represents a store operation.
 */
public class StoreLogEntry {

    /**
     * Identity of operation.
     */
    private UUID id;
    /**
     * Operation code. Helps in deserialization during factory method.
     */
    private StoreOperationCode opCode;
    /**
     * Serialized representation of the operation.
     */
    private Element data;
    /**
     * State from which Undo will start.
     */
    private StoreOperationState undoStartState;
    /**
     * Original shard version for remove steps.
     */
    private UUID originalShardVersionRemoves;
    /**
     * Original shard version for add steps.
     */
    private UUID originalShardVersionAdds;

    /**
     * Creates an Instance of Store Log Entry.
     *
     * @param id
     *            Id
     * @param opCode
     *            Operation Code
     * @param data
     *            Data
     * @param undoStartState
     *            Undo Start State
     * @param originalShardVersionRemoves
     *            Original Shard Version Removes
     * @param originalShardVersionAdds
     *            Original Shard Version Adds
     */
    public StoreLogEntry(UUID id,
            StoreOperationCode opCode,
            Element data,
            StoreOperationState undoStartState,
            UUID originalShardVersionRemoves,
            UUID originalShardVersionAdds) {
        this.id = id;
        this.opCode = opCode;
        this.data = data;
        this.undoStartState = undoStartState;
        this.originalShardVersionRemoves = originalShardVersionRemoves;
        this.originalShardVersionAdds = originalShardVersionAdds;
    }

    public final UUID getId() {
        return id;
    }

    public final StoreOperationCode getOpCode() {
        return opCode;
    }

    public final Element getData() {
        return data;
    }

    public final StoreOperationState getUndoStartState() {
        return undoStartState;
    }

    public final UUID getOriginalShardVersionRemoves() {
        return originalShardVersionRemoves;
    }

    public final UUID getOriginalShardVersionAdds() {
        return originalShardVersionAdds;
    }
}