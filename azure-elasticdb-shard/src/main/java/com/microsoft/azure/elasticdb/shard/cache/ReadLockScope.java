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
  private ReaderWriterLockSlim lock;

  /**
   * Whether upgrade of read lock is possible.
   */
  private boolean isUpgradable;

  /**
   * Acquires the read lock.
   *
   * @param lock Lock to be acquired.
   * @param upgradable Whether the lock is upgradable.
   */
  public ReadLockScope(ReaderWriterLockSlim lock, boolean upgradable) {
    this.lock = lock;

    isUpgradable = upgradable;

    if (isUpgradable) {
      this.lock.enterUpgradeableReadLock();
    } else {
      this.lock.enterReadLock();
    }
  }

  /**
   * Upgrade the read lock to a write lock.
   */
  public final WriteLockScope upgrade() {
    return new WriteLockScope(lock);
  }

  /**
   * Exits the locking scope.
   */
  public final void close() throws java.io.IOException {
    if (isUpgradable) {
      lock.exitUpgradeableReadLock();
    } else {
      lock.exitReadLock();
    }
  }
}
