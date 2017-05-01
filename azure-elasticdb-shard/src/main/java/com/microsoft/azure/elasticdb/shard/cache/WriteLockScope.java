package com.microsoft.azure.elasticdb.shard.cache;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

/**
 * Scope for a write lock.
 */
public class WriteLockScope implements java.io.Closeable {

  /**
   * The lock object on which read lock is held.
   */
  private ReaderWriterLockSlim lock;

  /**
   * Acquires the write lock.
   *
   * @param lock Lock to be acquired.
   */
  public WriteLockScope(ReaderWriterLockSlim lock) {
    this.lock = lock;

    this.lock.enterWriteLock();
  }

  /**
   * Exits the locking scope.
   */
  public final void close() throws java.io.IOException {
    lock.exitWriteLock();
  }
}