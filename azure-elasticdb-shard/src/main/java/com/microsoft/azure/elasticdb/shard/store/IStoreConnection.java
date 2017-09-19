package com.microsoft.azure.elasticdb.shard.store;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import java.util.UUID;

/**
 * Instance of a store connection.
 */
public interface IStoreConnection extends AutoCloseable {

  /**
   * Type of store connection.
   */
  StoreConnectionKind getKind();

  /**
   * Open the store connection, and acquire a lock on the store.
   *
   * @param lockId Lock Id.
   */
  void openWithLock(UUID lockId);

  /**
   * Closes the store connection.
   */
  void close();

  /**
   * Closes the store connection after releasing lock.
   *
   * @param lockId Lock Id.
   */
  void closeWithUnlock(UUID lockId);

  /**
   * Acquires a transactional scope on the connection.
   *
   * @param kind Type of transaction scope.
   * @return Transaction scope on the store connection.
   */
  IStoreTransactionScope getTransactionScope(StoreTransactionScopeKind kind);
}