package com.microsoft.azure.elasticdb.shard.sqlstore;

import com.microsoft.azure.elasticdb.shard.store.IStoreLogEntry;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationCode;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationState;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.UUID;

class SqlLogEntry implements IStoreLogEntry {
    private UUID id, shardIdRemoves,shardIdAdds;
    private StoreOperationCode opCode;
    private SQLXML data;
    private StoreOperationState undoStartState;

    public SqlLogEntry(ResultSet rs, int offset) throws SQLException {
        id = UUID.fromString(rs.getString(offset));
        opCode = StoreOperationCode.forValue(rs.getInt(offset + 1));
        data = rs.getSQLXML(offset + 1);
        undoStartState = StoreOperationState.forValue(rs.getInt(offset + 1));
        //TODO: Populate following fields.
        shardIdAdds = null;
        shardIdRemoves = null;
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public StoreOperationCode getOpCode() {
        return opCode;
    }

    @Override
    public SQLXML getData() {
        return data;
    }

    @Override
    public StoreOperationState getUndoStartState() {
        return undoStartState;
    }

    @Override
    public UUID getOriginalShardVersionRemoves() {
        return shardIdRemoves;
    }

    @Override
    public UUID getOriginalShardVersionAdds() {
        return shardIdAdds;
    }
}
