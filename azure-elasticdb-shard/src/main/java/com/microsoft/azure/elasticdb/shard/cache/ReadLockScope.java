package com.microsoft.azure.elasticdb.shard.cache;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

/**
 * Scope for a read lock.
 */
public class ReadLockScope implements java.io.Closeable {

  /**
   * The lock object on which read lock is held.
   */
  private ReaderWriterLockSlim _lock;

  /**
   * Whether upgrade of read lock is possible.
   */
  private boolean _upgradable;

  /**
   * Acquires the read lock.
   *
   * @param _lock Lock to be acquired.
   * @param upgradable Whether the lock is upgradable.
   */
  public ReadLockScope(ReaderWriterLockSlim _lock, boolean upgradable) {
    this._lock = _lock;

    _upgradable = upgradable;

    if (_upgradable) {
      this._lock.EnterUpgradeableReadLock();
    } else {
      this._lock.EnterReadLock();
    }
  }

  /**
   * Upgrade the read lock to a write lock.
   */
  public final WriteLockScope Upgrade() {
    return new WriteLockScope(_lock);
  }

  /**
   * Exits the locking scope.
   */
  public final void close() throws java.io.IOException {
    if (_upgradable) {
      _lock.ExitUpgradeableReadLock();
    } else {
      _lock.ExitReadLock();
    }
  }
}
