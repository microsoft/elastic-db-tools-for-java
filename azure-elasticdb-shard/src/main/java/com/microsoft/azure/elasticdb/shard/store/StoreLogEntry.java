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
  private UUID id;
  /**
   * Operation code. Helps in deserialization during factory method.
   */
  private StoreOperationCode opCode;
  /**
   * Serialized representation of the operation.
   */
  private SQLXML data;
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
   * @param id Id
   * @param opCode Operation Code
   * @param data Data
   * @param undoStartState Undo Start State
   * @param originalShardVersionRemoves Original Shard Version Removes
   * @param originalShardVersionAdds Original Shard Version Adds
   */
  public StoreLogEntry(UUID id, StoreOperationCode opCode, SQLXML data,
      StoreOperationState undoStartState, UUID originalShardVersionRemoves,
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

  private void setId(UUID value) {
    id = value;
  }

  public final StoreOperationCode getOpCode() {
    return opCode;
  }

  private void setOpCode(StoreOperationCode value) {
    opCode = value;
  }

  public final SQLXML getData() {
    return data;
  }

  private void setData(SQLXML value) {
    data = value;
  }

  public final StoreOperationState getUndoStartState() {
    return undoStartState;
  }

  private void setUndoStartState(StoreOperationState value) {
    undoStartState = value;
  }

  public final UUID getOriginalShardVersionRemoves() {
    return originalShardVersionRemoves;
  }

  private void setOriginalShardVersionRemoves(UUID value) {
    originalShardVersionRemoves = value;
  }

  public final UUID getOriginalShardVersionAdds() {
    return originalShardVersionAdds;
  }

  private void setOriginalShardVersionAdds(UUID value) {
    originalShardVersionAdds = value;
  }
}