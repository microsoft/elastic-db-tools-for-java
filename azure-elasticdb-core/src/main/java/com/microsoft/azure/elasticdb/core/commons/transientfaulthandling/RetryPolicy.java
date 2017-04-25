package com.microsoft.azure.elasticdb.core.commons.transientfaulthandling;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.google.common.base.Preconditions;
import com.microsoft.azure.elasticdb.core.commons.helpers.EventHandler;
import java.util.concurrent.Callable;

/**
 * Provides the base implementation of the retry mechanism for unreliable actions and transient
 * conditions.
 */
public class RetryPolicy {

  public static RetryPolicy DefaultRetryPolicy = new RetryPolicy();
  public EventHandler<RetryingEventArgs> Retrying;

  public RetryPolicy(Object strategy, Object arg) {
  }

  public RetryPolicy() {

  }

  public static RetryStrategy GetRetryStrategy() {
    return null;
  }

  /**
   * Repetitively executes the specified action while it satisfies the current retry policy.
   *
   * @param runnable A delegate that represents the executable action that doesn't return any
   * results.
   */
  public void ExecuteAction(Runnable runnable) {
    runnable.run();
  }

  public <TResult> TResult ExecuteAction(Callable<TResult> action) {
    Preconditions.checkNotNull(action);
    try {
      return action.call();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
