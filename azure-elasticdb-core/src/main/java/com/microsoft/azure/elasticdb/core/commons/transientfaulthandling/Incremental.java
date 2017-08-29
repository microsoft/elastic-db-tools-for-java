package com.microsoft.azure.elasticdb.core.commons.transientfaulthandling;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import java.time.Duration;

/**
 * A retry strategy with a specified number of retry attempts and an incremental time interval
 * between retries.
 */
public class Incremental extends RetryStrategy {

  private int retryCount;
  private Duration initialInterval = Duration.ZERO;
  private Duration increment = Duration.ZERO;

  /**
   * Initializes a new instance of the <see cref="Incremental"/> class.
   */
  public Incremental() {
    this(DEFAULT_CLIENT_RETRY_COUNT, DEFAULT_RETRY_INTERVAL, DEFAULT_RETRY_INCREMENT);
  }

  /**
   * Initializes a new instance of the <see cref="Incremental"/> class with the specified retry
   * settings.
   *
   * @param retryCount The number of retry attempts.
   * @param initialInterval The initial interval that will apply for the first retry.
   * @param increment The incremental time value that will be used to calculate the progressive
   * delay between retries.
   */
  public Incremental(int retryCount, Duration initialInterval, Duration increment) {
    this(null, retryCount, initialInterval, increment);
  }

  /**
   * Initializes a new instance of the <see cref="Incremental"/> class with the specified name and
   * retry settings.
   *
   * @param name The retry strategy name.
   * @param retryCount The number of retry attempts.
   * @param initialInterval The initial interval that will apply for the first retry.
   * @param increment The incremental time value that will be used to calculate the progressive
   * delay between retries.
   */
  public Incremental(String name, int retryCount, Duration initialInterval, Duration increment) {
    this(name, retryCount, initialInterval, increment, DEFAULT_FIRST_FAST_RETRY);
  }

  /**
   * Initializes a new instance of the <see cref="Incremental"/> class with the specified number of
   * retry attempts, time interval, retry strategy, and fast start option.
   *
   * @param name The retry strategy name.
   * @param retryCount The number of retry attempts.
   * @param initialInterval The initial interval that will apply for the first retry.
   * @param increment The incremental time value that will be used to calculate the progressive
   * delay between retries.
   * @param firstFastRetry true to immediately retry in the first attempt; otherwise, false. The
   * subsequent retries will remain subject to the configured retry interval.
   */
  public Incremental(String name, int retryCount, Duration initialInterval, Duration increment,
      boolean firstFastRetry) {
    super(name, firstFastRetry);
    Guard.argumentNotNegativeValue(retryCount, "retryCount");
    Guard.argumentNotNegativeValue(initialInterval.getSeconds(), "initialInterval");
    Guard.argumentNotNegativeValue(increment.getSeconds(), "increment");

    this.retryCount = retryCount;
    this.initialInterval = initialInterval;
    this.increment = increment;
  }

  /**
   * Returns the corresponding ShouldRetry delegate.
   *
   * @return The ShouldRetry delegate.
   */
  @Override
  public ShouldRetry getShouldRetry() {
    return (int currentRetryCount, RuntimeException lastException,
        ReferenceObjectHelper<Duration> retryInterval) -> {
      if (currentRetryCount < retryCount) {
        retryInterval.argValue = Duration.ofSeconds(initialInterval.getSeconds()
            + (increment.getSeconds() * currentRetryCount));
        return true;
      }

      retryInterval.argValue = Duration.ZERO;
      return false;
    };
  }
}