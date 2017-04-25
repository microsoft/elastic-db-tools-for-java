package com.microsoft.azure.elasticdb.shard.store;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationCode;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationState;
import java.sql.SQLXML;
import java.util.UUID;

/**
 * Represents a store operation.
 */
public class StoreLogEntry {

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

  public StoreLogEntry(UUID id, StoreOperationCode opCode, SQLXML data,
      StoreOperationState undoStartState, UUID originalShardVersionRemoves,
      UUID originalShardVersionAdds) {
    Id = id;
    OpCode = opCode;
    this.data = data;
    UndoStartState = undoStartState;
    OriginalShardVersionRemoves = originalShardVersionRemoves;
    OriginalShardVersionAdds = originalShardVersionAdds;
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