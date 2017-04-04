package com.microsoft.azure.elasticdb.shard.cache;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * Base class for all objects in the cache, providing locking facilities.
 */
public abstract class CacheObject implements java.io.Closeable {
    /**
     * Lock object.
     */
    private ReaderWriterLockSlim _lock;

    /**
     * Whether this object has already been disposed
     */
    private boolean _disposed = false;

    /**
     * Instantiates the underlying lock object.
     */
    protected CacheObject() {
        _lock = new ReaderWriterLockSlim();
    }

    /**
     * Obtains a read locking scope on the object.
     *
     * @param upgradable Whether the read lock should be upgradable.
     * @return Read locking scope.
     */
    public final ReadLockScope GetReadLockScope(boolean upgradable) {
        return new ReadLockScope(_lock, upgradable);
    }

    /**
     * Obtains a write locking scope on the object.
     *
     * @return Write locking scope.
     */
    public final WriteLockScope GetWriteLockScope() {
        assert !_lock.IsUpgradeableReadLockHeld;

        return new WriteLockScope(_lock);
    }

    ///#region IDisposable

    /**
     * Public dispose method.
     */
    public final void close() throws java.io.IOException {
        Dispose(true);
        //TODO: GC.SuppressFinalize(this);
    }

    /**
     * Protected vitual member of the dispose pattern.
     *
     * @param disposing Call came from Dispose.
     */
    protected void Dispose(boolean disposing) {
        if (!_disposed) {
            if (disposing) {
                //TODO: _lock.Dispose();
            }

            _disposed = true;
        }
    }

    ///#endregion IDisposable
}