package com.microsoft.azure.elasticdb.shard.sqlstore;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.store.IStoreLogEntry;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationCode;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationState;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.UUID;

/**
 * Implementation of a store operation.
 */
public class SqlLogEntry implements IStoreLogEntry {
    /**
     * Identity of operation.
     */
    private UUID Id;
    /**
     * Operation code. Helps in deserialization during factory method.
     */
    private StoreOperationCode OpCode;
    /**
     * Serialized representation of the operation.
     */
    private SQLXML data;
    /**
     * State from which Undo will start.
     */
    private StoreOperationState UndoStartState;
    /**
     * Original shard version for remove steps.
     */
    private UUID OriginalShardVersionRemoves;
    /**
     * Original shard version for add steps.
     */
    private UUID OriginalShardVersionAdds;

    /**
     * Constructs an instance of IStoreLogEntry using parts of a row from SqlDataReader.
     * Used for creating the store operation for Undo.
     *
     * @param reader SqlDataReader whose row has operation information.
     * @param offset Reader offset for column that begins operation information.
     */
    public SqlLogEntry(ResultSet reader, int offset) throws SQLException {
        this.setId(UUID.fromString(reader.getString(offset)));
        this.setOpCode(StoreOperationCode.forValue(reader.getInt(offset + 1)));
        this.setData(reader.getSQLXML(offset + 2));
        this.setUndoStartState(StoreOperationState.forValue(reader.getInt(offset + 3)));
        UUID shardIdRemoves;
        shardIdRemoves = UUID.fromString(reader.getString(offset + 4));
        this.setOriginalShardVersionRemoves(shardIdRemoves.compareTo(new UUID(0L, 0L)) == 0 ? null : shardIdRemoves);
        UUID shardIdAdds;
        shardIdAdds = UUID.fromString(reader.getString(offset + 5));
        this.setOriginalShardVersionAdds(shardIdAdds.compareTo(new UUID(0L, 0L)) == 0 ? null : shardIdAdds);
    }

    public final UUID getId() {
        return Id;
    }

    private void setId(UUID value) {
        Id = value;
    }

    public final StoreOperationCode getOpCode() {
        return OpCode;
    }

    private void setOpCode(StoreOperationCode value) {
        OpCode = value;
    }

    public final SQLXML getData() {
        return data;
    }

    private void setData(SQLXML value) {
        data = value;
    }

    public final StoreOperationState getUndoStartState() {
        return UndoStartState;
    }

    private void setUndoStartState(StoreOperationState value) {
        UndoStartState = value;
    }

    public final UUID getOriginalShardVersionRemoves() {
        return OriginalShardVersionRemoves;
    }

    private void setOriginalShardVersionRemoves(UUID value) {
        OriginalShardVersionRemoves = value;
    }

    public final UUID getOriginalShardVersionAdds() {
        return OriginalShardVersionAdds;
    }

    private void setOriginalShardVersionAdds(UUID value) {
        OriginalShardVersionAdds = value;
    }
}