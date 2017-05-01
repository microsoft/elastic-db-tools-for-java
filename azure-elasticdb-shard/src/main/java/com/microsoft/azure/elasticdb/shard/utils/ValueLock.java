package com.microsoft.azure.elasticdb.shard.utils;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Mutual exclusion construct for values.
 *
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
  private T value;

  /**
   * Reference counter for the value.
   */
  private RefCountedObject valueLock;

  /**
   * Constructs an instace of lock on input value and locks it.
   *
   * @param value Value being locked.
   */
  public ValueLock(T value) {
    ExceptionUtils.disallowNullArgument(value, "value");

    this.value = value;

    synchronized (s_lock) {
      if (!s_locks.containsKey(this.value)) {
        valueLock = new RefCountedObject();
        s_locks.put(this.value, valueLock);
      } else {
        valueLock = s_locks.get(this.value);
        valueLock.addRef();
      }
    }
    //TODO? Monitor.Enter(valueLock);
  }

  /**
   * Releases the reference on the value, unlocks it if reference
   * count reaches 0.
   */
  public final void close() throws java.io.IOException {
    //TODO? Monitor.Exit(valueLock);

    synchronized (s_lock) {
      // Impossible to have acquired a lock without a name.
      assert s_locks.containsKey(value);

      if (valueLock.release() == 0) {
        s_locks.remove(value);
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
    private int refCount;

    /**
     * Instantiates the reference counter, initally set to 1.
     */
    public RefCountedObject() {
      refCount = 1;
    }

    /**
     * Increments reference count.
     */
    public final void addRef() {
      refCount++;
    }

    /**
     * Decrements the reference count.
     *
     * @return New value of reference count.
     */
    public final int release() {
      return --refCount;
    }
  }
}