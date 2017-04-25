package com.microsoft.azure.elasticdb.core.commons.transientfaulthandling;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import java.time.Duration;

/**
 * A retry strategy with backoff parameters for calculating the exponential delay between retries.
 */
public class ExponentialBackoff extends RetryStrategy {

  private int _retryCount;
  private Duration _minBackoff = Duration.ZERO;
  private Duration _maxBackoff = Duration.ZERO;
  private Duration _deltaBackoff = Duration.ZERO;

  /**
   * Initializes a new instance of the <see cref="ExponentialBackoff"/> class.
   */
  public ExponentialBackoff() {
    this(DefaultClientRetryCount, DefaultMinBackoff, DefaultMaxBackoff, DefaultClientBackoff);
  }

  /**
   * Initializes a new instance of the <see cref="ExponentialBackoff"/> class with the specified
   * retry settings.
   *
   * @param retryCount The maximum number of retry attempts.
   * @param minBackoff The minimum backoff time
   * @param maxBackoff The maximum backoff time.
   * @param deltaBackoff The value that will be used to calculate a random delta in the exponential
   * delay between retries.
   */
  public ExponentialBackoff(int retryCount, Duration minBackoff, Duration maxBackoff,
      Duration deltaBackoff) {
    this(null, retryCount, minBackoff, maxBackoff, deltaBackoff, DefaultFirstFastRetry);
  }

  /**
   * Initializes a new instance of the <see cref="ExponentialBackoff"/> class with the specified
   * name and retry settings.
   *
   * @param name The name of the retry strategy.
   * @param retryCount The maximum number of retry attempts.
   * @param minBackoff The minimum backoff time
   * @param maxBackoff The maximum backoff time.
   * @param deltaBackoff The value that will be used to calculate a random delta in the exponential
   * delay between retries.
   */
  public ExponentialBackoff(String name, int retryCount, Duration minBackoff, Duration maxBackoff,
      Duration deltaBackoff) {
    this(name, retryCount, minBackoff, maxBackoff, deltaBackoff, DefaultFirstFastRetry);
  }

  /**
   * Initializes a new instance of the <see cref="ExponentialBackoff"/> class with the specified
   * name, retry settings, and fast retry option.
   *
   * @param name The name of the retry strategy.
   * @param retryCount The maximum number of retry attempts.
   * @param minBackoff The minimum backoff time
   * @param maxBackoff The maximum backoff time.
   * @param deltaBackoff The value that will be used to calculate a random delta in the exponential
   * delay between retries.
   * @param firstFastRetry true to immediately retry in the first attempt; otherwise, false. The
   * subsequent retries will remain subject to the configured retry interval.
   */
  public ExponentialBackoff(String name, int retryCount, Duration minBackoff, Duration maxBackoff,
      Duration deltaBackoff, boolean firstFastRetry) {
    super(name, firstFastRetry);
        /* TODO:
        Guard.ArgumentNotNegativeValue(retryCount, "retryCount");
        Guard.ArgumentNotNegativeValue(minBackoff.Ticks, "minBackoff");
		Guard.ArgumentNotNegativeValue(maxBackoff.Ticks, "maxBackoff");
		Guard.ArgumentNotNegativeValue(deltaBackoff.Ticks, "deltaBackoff");
		Guard.ArgumentNotGreaterThan(minBackoff.TotalMilliseconds, maxBackoff.TotalMilliseconds, "minBackoff");
		*/

    _retryCount = retryCount;
    _minBackoff = minBackoff;
    _maxBackoff = maxBackoff;
    _deltaBackoff = deltaBackoff;
  }

  /**
   * Returns the corresponding ShouldRetry delegate.
   *
   * @return The ShouldRetry delegate.
   */
  @Override
  public ShouldRetry GetShouldRetry() {
    return null; //TODO
  }
}