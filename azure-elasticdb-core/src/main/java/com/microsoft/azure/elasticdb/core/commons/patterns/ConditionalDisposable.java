package com.microsoft.azure.elasticdb.core.commons.patterns;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

/**
 * A disposable object which opts-out of disposing the inner disposable
 * only when instructed by the caller.
 * <typeparam name="T"></typeparam>
 */
public final class ConditionalDisposable<T extends AutoCloseable> implements AutoCloseable {

  /**
   * Inner disposable object.
   */
  private T innerDisposable;
  private boolean doNotDispose;

  /**
   * Constructor which takes an inner disposable object.
   */
  public ConditionalDisposable(T innerDisposable) {
    this.innerDisposable = innerDisposable;
  }

  /**
   * Used for notifying about disposable decision on inner object.
   */
  public boolean getDoNotDispose() {
    return doNotDispose;
  }

  /**
   * Used for notifying about disposable decision on inner object.
   */
  public void setDoNotDispose(boolean value) {
    doNotDispose = value;
  }

  /**
   * Disposes the inner object if doNotDispose is set to false.
   */
  public void close() throws Exception {
    if (!this.getDoNotDispose()) {
      innerDisposable.close();
    }
  }

  /**
   * Gets the inner disposable object.
   */
  public T getValue() {
    return this.innerDisposable;
  }
}