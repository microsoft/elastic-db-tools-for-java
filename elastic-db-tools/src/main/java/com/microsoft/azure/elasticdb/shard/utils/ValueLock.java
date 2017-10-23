package com.microsoft.azure.elasticdb.shard.utils;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Mutual exclusion construct for values.
 *
 * <typeparam name="T">Type of values.</typeparam>
 */
public class ValueLock<T> implements AutoCloseable {

    /**
     * Global lock for mutual exclusion on the global dictionary of values.
     */
    private static final ReentrantLock LOCK = new ReentrantLock();

    /**
     * Existing collection of values.
     */
    private static HashMap<Object, RefCountedObject> LOCKS = new HashMap<>();

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
     * @param value
     *            Value being locked.
     */
    public ValueLock(T value) {
        ExceptionUtils.disallowNullArgument(value, "value");

        this.value = value;

        synchronized (LOCK) {
            if (!LOCKS.containsKey(this.value)) {
                valueLock = new RefCountedObject();
                LOCKS.put(this.value, valueLock);
            }
            else {
                valueLock = LOCKS.get(this.value);
                valueLock.addRef();
            }
        }
    }

    /**
     * Releases the reference on the value, unlocks it if reference count reaches 0.
     */
    public final void close() throws java.io.IOException {
        synchronized (LOCK) {
            // Impossible to have acquired a lock without a name.
            assert LOCKS.containsKey(value);

            if (valueLock.release() == 0) {
                LOCKS.remove(value);
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