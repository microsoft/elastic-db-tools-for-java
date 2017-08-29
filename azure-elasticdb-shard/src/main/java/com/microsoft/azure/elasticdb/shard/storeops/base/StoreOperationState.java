package com.microsoft.azure.elasticdb.shard.storeops.base;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

/**
 * States of the operation.
 */
public enum StoreOperationState {
  /**
   * Initial state on Do.
   */
  DoBegin(0),

  /**
   * Before connect GSM on Do.
   */
  DoGlobalConnect(1),

  /**
   * Before connect LSM source on Do.
   */
  DoLocalSourceConnect(2),

  /**
   * Before connect LSM target on Do.
   */
  DoLocalTargetConnect(3),

  /**
   * Before GSM operation pre LSM operations about to Start Transaction.
   */
  DoGlobalPreLocalBeginTransaction(4),

  /**
   * Before GSM operation pre LSM operations about to execute.
   */
  DoGlobalPreLocalExecute(5),

  /**
   * Before GSM operation pre LSM operations about to commit transaction.
   */
  DoGlobalPreLocalCommitTransaction(6),

  /**
   * Before LSM operation on Source shard about to start transaction.
   */
  DoLocalSourceBeginTransaction(7),

  /**
   * Before LSM operation on Source shard about to execute.
   */
  DoLocalSourceExecute(8),

  /**
   * Before LSM operation on Source shard about to commit transaction transaction.
   */
  DoLocalSourceCommitTransaction(9),

  /**
   * Before LSM operation on Target shard about to start transaction.
   */
  DoLocalTargetBeginTransaction(10),

  /**
   * Before LSM operation on Target shard about to execute.
   */
  DoLocalTargetExecute(11),

  /**
   * Before LSM operation on Target shard about to commit transaction transaction.
   */
  DoLocalTargetCommitTransaction(12),

  /**
   * Before GSM operation post LSM operations about to Start Transaction.
   */
  DoGlobalPostLocalBeginTransaction(13),

  /**
   * Before GSM operation post LSM operations about to execute.
   */
  DoGlobalPostLocalExecute(14),

  /**
   * Before GSM operation post LSM operations about to commit transaction.
   */
  DoGlobalPostLocalCommitTransaction(15),

  /**
   * Before disconnect on Do.
   */
  DoEnd(16),

  /**
   * Initial state on Undo.
   */
  UndoBegin(100),

  /**
   * Before connect GSM on Undo.
   */
  UndoGlobalConnect(101),

  /**
   * Before connect LSM source on Undo.
   */
  UndoLocalSourceConnect(102),

  /**
   * Before connect LSM target on Undo.
   */
  UndoLocalTargetConnect(103),

  /**
   * Before GSM operation pre LSM operations about to Start Transaction.
   */
  UndoGlobalPreLocalBeginTransaction(104),

  /**
   * Before GSM operation pre LSM operations about to execute.
   */
  UndoGlobalPreLocalExecute(105),

  /**
   * Before GSM operation pre LSM operations about to commit transaction.
   */
  UndoGlobalPreLocalCommitTransaction(106),

  /**
   * Before LSM operation on Target shard about to start transaction.
   */
  UndoLocalTargetBeginTransaction(107),

  /**
   * Before LSM operation on Target shard about to execute.
   */
  UndoLocalTargetExecute(108),

  /**
   * Before LSM operation on Target shard about to commit transaction transaction.
   */
  UndoLocalTargetCommitTransaction(109),

  /**
   * Before LSM operation on Source shard about to start transaction.
   */
  UndoLocalSourceBeginTransaction(110),

  /**
   * Before LSM operation on Source shard about to execute.
   */
  UndoLocalSourceExecute(111),

  /**
   * Before LSM operation on Source shard about to commit transaction transaction.
   */
  UndoLocalSourceCommitTransaction(112),

  /**
   * Before GSM operation post LSM operations about to Start Transaction.
   */
  UndoGlobalPostLocalBeginTransaction(113),

  /**
   * Before GSM operation post LSM operations about to execute.
   */
  UndoGlobalPostLocalExecute(114),

  /**
   * Before GSM operation post LSM operations about to commit transaction.
   */
  UndoGlobalPostLocalCommitTransaction(115),

  /**
   * Before disconnect on Undo.
   */
  UndoEnd(116);

  public static final int SIZE = Integer.SIZE;
  private static java.util.HashMap<Integer, StoreOperationState> mappings;
  private int intValue;

  StoreOperationState(int value) {
    intValue = value;
    getMappings().put(value, this);
  }

  private static java.util.HashMap<Integer, StoreOperationState> getMappings() {
    if (mappings == null) {
      synchronized (StoreOperationState.class) {
        if (mappings == null) {
          mappings = new java.util.HashMap<>();
        }
      }
    }
    return mappings;
  }

  public static StoreOperationState forValue(int value) {
    return getMappings().get(value);
  }

  public int getValue() {
    return intValue;
  }
}