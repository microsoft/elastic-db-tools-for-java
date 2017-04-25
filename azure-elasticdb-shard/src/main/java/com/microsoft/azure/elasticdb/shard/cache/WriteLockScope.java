package com.microsoft.azure.elasticdb.shard.cache;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * Scope for a write lock.
 */
public class WriteLockScope implements java.io.Closeable {

  /**
   * The lock object on which read lock is held.
   */
  private ReaderWriterLockSlim _lock;

  /**
   * Acquires the write lock.
   *
   * @param _lock Lock to be acquired.
   */
  public WriteLockScope(ReaderWriterLockSlim _lock) {
    this._lock = _lock;

    this._lock.EnterWriteLock();
  }

  /**
   * Exits the locking scope.
   */
  public final void close() throws java.io.IOException {
    _lock.ExitWriteLock();
  }
}