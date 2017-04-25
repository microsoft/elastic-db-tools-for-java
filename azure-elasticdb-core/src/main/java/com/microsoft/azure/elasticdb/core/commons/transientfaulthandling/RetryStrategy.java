package com.microsoft.azure.elasticdb.core.commons.transientfaulthandling;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * Represents a retry strategy that determines the number of retry attempts and the interval between
 * retries.
 */
public abstract class RetryStrategy {
  ///#region Public members

  /**
   * Represents the default number of retry attempts.
   */
  public static final int DefaultClientRetryCount = 10;

  /**
   * Represents the default amount of time used when calculating a random delta in the exponential
   * delay between retries.
   */
  public static final Duration DefaultClientBackoff = Duration.of(10, ChronoUnit.SECONDS);

  /**
   * Represents the default maximum amount of time used when calculating the exponential delay
   * between retries.
   */
  public static final Duration DefaultMaxBackoff = Duration.of(30, ChronoUnit.SECONDS);

  /**
   * Represents the default minimum amount of time used when calculating the exponential delay
   * between retries.
   */
  public static final Duration DefaultMinBackoff = Duration.of(1, ChronoUnit.SECONDS);

  /**
   * Represents the default interval between retries.
   */
  public static final Duration DefaultRetryInterval = Duration.of(1, ChronoUnit.SECONDS);

  /**
   * Represents the default time increment between retry attempts in the progressive delay policy.
   */
  public static final Duration DefaultRetryIncrement = Duration.of(1, ChronoUnit.SECONDS);

  /**
   * Represents the default flag indicating whether the first retry attempt will be made
   * immediately, whereas subsequent retries will remain subject to the retry interval.
   */
  public static final boolean DefaultFirstFastRetry = true;

  ///#endregion
  /**
   * Gets or sets a value indicating whether the first retry attempt will be made immediately,
   * whereas subsequent retries will remain subject to the retry interval.
   */
  private boolean FastFirstRetry;
  /**
   * Gets the name of the retry strategy.
   */
  private String Name;

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

  public final boolean getFastFirstRetry() {
    return FastFirstRetry;
  }

  public final void setFastFirstRetry(boolean value) {
    FastFirstRetry = value;
  }

  public final String getName() {
    return Name;
  }

  private void setName(String value) {
    Name = value;
  }

  /**
   * Returns the corresponding ShouldRetry delegate.
   *
   * @return The ShouldRetry delegate.
   */
  public abstract ShouldRetry GetShouldRetry();
}
