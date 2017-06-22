package com.microsoft.azure.elasticdb.core.commons.transientfaulthandling;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.helpers.EventArgs;
import java.time.Duration;

/**
 * Shard management retrying event arguments.
 */
public final class RetryingEventArgs extends EventArgs {

  /**
   * Gets the current retry count.
   */
  private int currentRetryCount;
  /**
   * Gets the delay that indicates how long the current thread will be suspended before the next
   * iteration is invoked.
   */
  private Duration delay = Duration.ZERO;
  /**
   * Gets the exception that caused the retry conditions to occur.
   */
  private RuntimeException lastException;

  /**
   * Initializes new instance of <see cref="RetryingEventArgs"/> class.
   *
   * @param arg RetryingEventArgs from RetryPolicy.retrying event.
   */
  public RetryingEventArgs(RetryingEventArgs arg) {
    this.setCurrentRetryCount(arg.currentRetryCount);
    this.setDelay(arg.delay);
    this.setLastException(arg.lastException);
  }

  public RetryingEventArgs(int retryCount, Duration delay, RuntimeException ex) {
    this.setCurrentRetryCount(retryCount);
    this.setDelay(delay);
    this.setLastException(ex);
  }

  public int getCurrentRetryCount() {
    return currentRetryCount;
  }

  private void setCurrentRetryCount(int value) {
    currentRetryCount = value;
  }

  public Duration getDelay() {
    return delay;
  }

  private void setDelay(Duration value) {
    delay = value;
  }

  public RuntimeException getLastException() {
    return lastException;
  }

  private void setLastException(RuntimeException value) {
    lastException = value;
  }
}