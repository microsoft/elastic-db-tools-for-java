package com.microsoft.azure.elasticdb.core.commons.transientfaulthandling;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import java.time.Duration;

/**
 * Defines a callback delegate that will be invoked whenever a retry condition is encountered.
 */
@FunctionalInterface
public interface ShouldRetry {

  /**
   * Defines a callback delegate that will be invoked whenever a retry condition is encountered.
   *
   * @param retryCount The current retry attempt count.
   * @param lastException The exception that caused the retry conditions to occur.
   * @param delay The delay that indicates how long the current thread will be suspended before the
   * next iteration is invoked.
   * @return <see langword="true"/> if a retry is allowed; otherwise, <see langword="false"/>.
   */
  boolean invoke(int retryCount, RuntimeException lastException,
      ReferenceObjectHelper<Duration> delay);
}