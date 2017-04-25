package com.microsoft.azure.elasticdb.shard.utils;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Mutual exclusion construct for values.
 * <p>
 * <typeparam name="T">Type of values.</typeparam>
 */
public class ValueLock<T> implements java.io.Closeable {

  /**
   * Global lock for mutual exclusion on the global dictionary of values.
   */
  private static final ReentrantLock s_lock = new ReentrantLock();

  /**
   * Existing collection of values.
   */
  private static HashMap<Object, RefCountedObject> s_locks = new HashMap<>();

  /**
   * Value being locked.
   */
  private T _value;

  /**
   * Reference counter for the value.
   */
  private RefCountedObject _valueLock;

  /**
   * Constructs an instace of lock on input value and locks it.
   *
   * @param value Value being locked.
   */
  public ValueLock(T value) {
    ExceptionUtils.DisallowNullArgument(value, "value");

    _value = value;

    synchronized (s_lock) {
      if (!s_locks.containsKey(_value)) {
        _valueLock = new RefCountedObject();
        s_locks.put(_value, _valueLock);
      } else {
        _valueLock = s_locks.get(_value);
        _valueLock.AddRef();
      }
    }
    //TODO? Monitor.Enter(_valueLock);
  }

  /**
   * Releases the reference on the value, unlocks it if reference
   * count reaches 0.
   */
  public final void close() throws java.io.IOException {
    //TODO? Monitor.Exit(_valueLock);

    synchronized (s_lock) {
      // Impossible to have acquired a lock without a name.
      assert s_locks.containsKey(_value);

      if (_valueLock.Release() == 0) {
        s_locks.remove(_value);
      }
    }
  }

  /**
   * Reference counter implementation.
   */
  private static class RefCountedObject {

    /**
     * Number of references.
     */
    private int _refCount;

    /**
     * Instantiates the reference counter, initally set to 1.
     */
    public RefCountedObject() {
      _refCount = 1;
    }

    /**
     * Increments reference count.
     */
    public final void AddRef() {
      _refCount++;
    }

    /**
     * Decrements the reference count.
     *
     * @return New value of reference count.
     */
    public final int Release() {
      return --_refCount;
    }
  }
}