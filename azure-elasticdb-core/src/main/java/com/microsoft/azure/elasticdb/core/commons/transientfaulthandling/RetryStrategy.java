package com.microsoft.azure.elasticdb.core.commons.transientfaulthandling;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import java.time.Duration;

/**
 * Represents a retry strategy that determines the number of retry attempts and the interval between
 * retries.
 */
public abstract class RetryStrategy {
  ///#region Public members

  /**
   * Represents the default number of retry attempts.
   */
  public static final int DEFAULT_CLIENT_RETRY_COUNT = 10;

  /**
   * Represents the default amount of time used when calculating a random delta in the exponential
   * delay between retries.
   */
  public static final Duration DEFAULT_CLIENT_BACKOFF = Duration.ofSeconds(10);

  /**
   * Represents the default maximum amount of time used when calculating the exponential delay
   * between retries.
   */
  public static final Duration DEFAULT_MAX_BACKOFF = Duration.ofSeconds(30);

  /**
   * Represents the default minimum amount of time used when calculating the exponential delay
   * between retries.
   */
  public static final Duration DEFAULT_MIN_BACKOFF = Duration.ofSeconds(1);

  /**
   * Represents the default interval between retries.
   */
  public static final Duration DEFAULT_RETRY_INTERVAL = Duration.ofSeconds(1);

  /**
   * Represents the default time increment between retry attempts in the progressive delay policy.
   */
  public static final Duration DEFAULT_RETRY_INCREMENT = Duration.ofSeconds(1);

  /**
   * Represents the default flag indicating whether the first retry attempt will be made
   * immediately, whereas subsequent retries will remain subject to the retry interval.
   */
  public static final boolean DEFAULT_FIRST_FAST_RETRY = true;

  ///#endregion

  private static RetryStrategy noRetry = new FixedInterval(0, DEFAULT_RETRY_INTERVAL);
  private static RetryStrategy defaultFixed = new FixedInterval(DEFAULT_CLIENT_RETRY_COUNT,
      DEFAULT_RETRY_INTERVAL);
  private static RetryStrategy defaultProgressive = new Incremental(DEFAULT_CLIENT_RETRY_COUNT,
      DEFAULT_RETRY_INTERVAL, DEFAULT_RETRY_INCREMENT);
  private static RetryStrategy defaultExponential = new ExponentialBackoff(
      DEFAULT_CLIENT_RETRY_COUNT, DEFAULT_MIN_BACKOFF, DEFAULT_MAX_BACKOFF, DEFAULT_CLIENT_BACKOFF);
  /**
   * Gets or sets a value indicating whether the first retry attempt will be made immediately,
   * whereas subsequent retries will remain subject to the retry interval.
   */
  private boolean fastFirstRetry;
  /**
   * Gets the name of the retry strategy.
   */
  private String name;

  /**
   * Initializes a new instance of the <see cref="RetryStrategy"/> class.
   *
   * @param name The name of the retry strategy.
   * @param firstFastRetry true to immediately retry in the first attempt; otherwise, false. The
   * subsequent retries will remain subject to the configured retry interval.
   */
  protected RetryStrategy(String name, boolean firstFastRetry) {
    this.setName(name);
    this.setFastFirstRetry(firstFastRetry);
  }

  /**
   * Returns a default policy that performs no retries, but invokes the action only once.
   */
  public static RetryStrategy getNoRetry() {
    return noRetry;
  }

  /**
   * Returns a default policy that implements a fixed retry interval configured with the <see
   * cref="RetryStrategy.DEFAULT_CLIENT_RETRY_COUNT"/> and <see cref="RetryStrategy.DEFAULT_RETRY_INTERVAL"/>
   * parameters. The default retry policy treats all caught exceptions as transient errors.
   */
  public static RetryStrategy getDefaultFixed() {
    return defaultFixed;
  }

  /**
   * Returns a default policy that implements a progressive retry interval configured with the <see
   * cref="RetryStrategy.DEFAULT_CLIENT_RETRY_COUNT"/>, <see cref="RetryStrategy.DEFAULT_RETRY_INTERVAL"/>,
   * and <see cref="RetryStrategy.DEFAULT_RETRY_INCREMENT"/> parameters. The default retry policy
   * treats all caught exceptions as transient errors.
   */
  public static RetryStrategy getDefaultProgressive() {
    return defaultProgressive;
  }

  /**
   * Returns a default policy that implements a random exponential retry interval configured with
   * the <see cref="RetryStrategy.DEFAULT_CLIENT_RETRY_COUNT"/>, <see
   * cref="RetryStrategy.DEFAULT_MIN_BACKOFF"/>, <see cref="RetryStrategy.DEFAULT_MAX_BACKOFF"/>,
   * and <see cref="RetryStrategy.DEFAULT_CLIENT_BACKOFF"/> parameters. The default retry policy
   * treats all caught exceptions as transient errors.
   */
  public static RetryStrategy getDefaultExponential() {
    return defaultExponential;
  }

  public final boolean getFastFirstRetry() {
    return fastFirstRetry;
  }

  public final void setFastFirstRetry(boolean value) {
    fastFirstRetry = value;
  }

  public final String getName() {
    return name;
  }

  private void setName(String value) {
    name = value;
  }

  /**
   * Returns the corresponding ShouldRetry delegate.
   *
   * @return The ShouldRetry delegate.
   */
  public abstract ShouldRetry getShouldRetry();
}