package com.microsoft.azure.elasticdb.shard.cache;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

/**
 * Base class for all objects in the cache, providing locking facilities.
 */
public abstract class CacheObject implements AutoCloseable {

  /**
   * Lock object.
   */
  private ReaderWriterLockSlim lock;

  /**
   * Whether this object has already been disposed.
   */
  private boolean disposed = false;

  /**
   * Instantiates the underlying lock object.
   */
  protected CacheObject() {
    lock = new ReaderWriterLockSlim();
  }

  /**
   * Obtains a read locking scope on the object.
   *
   * @param upgradable Whether the read lock should be upgradable.
   * @return Read locking scope.
   */
  public final ReadLockScope getReadLockScope(boolean upgradable) {
    return new ReadLockScope(lock, upgradable);
  }

  /**
   * Obtains a write locking scope on the object.
   *
   * @return Write locking scope.
   */
  public final WriteLockScope getWriteLockScope() {
    assert !lock.isUpgradeableReadLockHeld;

    return new WriteLockScope(lock);
  }

  ///#region IDisposable

  /**
   * Public dispose method.
   */
  public final void close() {
    dispose(true);
    //TODO: GC.SuppressFinalize(this);
  }

  /**
   * Protected vitual member of the dispose pattern.
   *
   * @param disposing Call came from Dispose.
   */
  protected void dispose(boolean disposing) {
    if (!disposed) {
      if (disposing) {
        //TODO: lock.Dispose();
      }

      disposed = true;
    }
  }

  ///#endregion IDisposable
}