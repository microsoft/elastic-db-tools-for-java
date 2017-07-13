package com.microsoft.azure.elasticdb.core.commons.transientfaulthandling;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.helpers.Event;
import com.microsoft.azure.elasticdb.core.commons.helpers.EventHandler;
import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import java.time.Duration;
import java.util.concurrent.Callable;

/**
 * Provides the base implementation of the retry mechanism for unreliable actions and transient
 * conditions.
 */
public class RetryPolicy {

  /**
   * Retry policy that tries up to 5 times with exponential backoff before giving up.
   */
  private static final RetryPolicy DEFAULT_RETRY_POLICY = new RetryPolicy(5,
      RetryStrategy.DEFAULT_MIN_BACKOFF, RetryStrategy.DEFAULT_MAX_BACKOFF,
      RetryStrategy.DEFAULT_CLIENT_BACKOFF);
  private static RetryPolicy noRetry = new RetryPolicy(new TransientErrorIgnoreStrategy(),
      RetryStrategy.getNoRetry());
  private static RetryPolicy defaultFixed = new RetryPolicy(new TransientErrorCatchAllStrategy(),
      RetryStrategy.getDefaultFixed());
  private static RetryPolicy defaultProgressive = new RetryPolicy(
      new TransientErrorCatchAllStrategy(), RetryStrategy.getDefaultProgressive());
  private static RetryPolicy defaultExponential = new RetryPolicy(
      new TransientErrorCatchAllStrategy(), RetryStrategy.getDefaultExponential());
  /**
   * An instance of a callback delegate that will be invoked whenever a retry condition is
   * encountered.
   */
  public Event<EventHandler<RetryingEventArgs>> retrying;
  /**
   * Gets the number of retries.
   */
  private int retryCount;
  /**
   * Gets minimum backoff time.
   */
  private Duration minBackOff = Duration.ZERO;
  /**
   * Gets maximum backoff time.
   */
  private Duration maxBackOff = Duration.ZERO;
  /**
   * Gets value used to calculate random delta in the exponential delay between retries. Time delta
   * for next retry attempt is 2^(currentRetryCount - 1) * random value between 80% and 120% of
   * DeltaBackOff.
   */
  private Duration deltaBackOff = Duration.ZERO;
  /**
   * Gets the retry strategy.
   */
  private RetryStrategy retryStrategy;
  /**
   * Gets the instance of the error detection strategy.
   */
  private ITransientErrorDetectionStrategy errorDetectionStrategy;

  /**
   * Initializes an instance of the <see cref="RetryPolicy"/> class.
   *
   * @param retryCount The number of retry attempts.
   * @param minBackOff Minimum backoff time for exponential backoff policy.
   * @param maxBackOff Maximum backoff time for exponential backoff policy.
   * @param deltaBackOff Delta backoff time for exponential backoff policy.
   */
  public RetryPolicy(int retryCount, Duration minBackOff, Duration maxBackOff,
      Duration deltaBackOff) {
    this.setRetryCount((retryCount < 0) ? 0 : retryCount);
    this.setMinBackOff((minBackOff.getSeconds() < Duration.ZERO.getSeconds())
        ? Duration.ZERO : minBackOff);
    this.setMaxBackOff((maxBackOff.getSeconds() < Duration.ZERO.getSeconds())
        ? Duration.ZERO : maxBackOff);
    this.setDeltaBackOff((deltaBackOff.getSeconds() < Duration.ZERO.getSeconds())
        ? Duration.ZERO : deltaBackOff);
    this.setRetryStrategy(getExponentialRetryStrategy());
  }

  /**
   * Initializes a new instance of the <see cref="RetryPolicy"/> class with the specified number of
   * retry attempts and parameters defining the progressive delay between retries.
   *
   * @param errorDetectionStrategy The <see cref="ITransientErrorDetectionStrategy"/> that is
   * responsible for detecting transient conditions.
   * @param retryStrategy The strategy to use for this retry policy.
   */
  public RetryPolicy(ITransientErrorDetectionStrategy errorDetectionStrategy,
      RetryStrategy retryStrategy) {
    Guard.argumentNotNull(errorDetectionStrategy, "errorDetectionStrategy");
    Guard.argumentNotNull(retryStrategy, "retryStrategy");

    this.setErrorDetectionStrategy(errorDetectionStrategy);
    this.setRetryStrategy(retryStrategy);
    this.retrying = new Event<>();
  }

  /**
   * Initializes a new instance of the <see cref="RetryPolicy"/> class with the specified number of
   * retry attempts and default fixed time interval between retries.
   *
   * @param errorDetectionStrategy The <see cref="ITransientErrorDetectionStrategy"/> that is
   * responsible for detecting transient conditions.
   * @param retryCount The number of retry attempts.
   */
  public RetryPolicy(ITransientErrorDetectionStrategy errorDetectionStrategy, int retryCount) {
    this(errorDetectionStrategy, new FixedInterval(retryCount));
  }

  /**
   * Initializes a new instance of the <see cref="RetryPolicy"/> class with the specified number of
   * retry attempts and fixed time interval between retries.
   *
   * @param errorDetectionStrategy The <see cref="ITransientErrorDetectionStrategy"/> that is
   * responsible for detecting transient conditions.
   * @param retryCount The number of retry attempts.
   * @param retryInterval The interval between retries.
   */
  public RetryPolicy(ITransientErrorDetectionStrategy errorDetectionStrategy, int retryCount,
      Duration retryInterval) {
    this(errorDetectionStrategy, new FixedInterval(retryCount, retryInterval));
  }

  /**
   * Initializes a new instance of the <see cref="RetryPolicy"/> class with the specified number of
   * retry attempts and backoff parameters for calculating the exponential delay between retries.
   *
   * @param errorDetectionStrategy The <see cref="ITransientErrorDetectionStrategy"/> that is
   * responsible for detecting transient conditions.
   * @param retryCount The number of retry attempts.
   * @param minBackoff The minimum backoff time.
   * @param maxBackoff The maximum backoff time.
   * @param deltaBackoff The time value that will be used to calculate a random delta in the
   * exponential delay between retries.
   */
  public RetryPolicy(ITransientErrorDetectionStrategy errorDetectionStrategy, int retryCount,
      Duration minBackoff, Duration maxBackoff, Duration deltaBackoff) {
    this(errorDetectionStrategy,
        new ExponentialBackoff(retryCount, minBackoff, maxBackoff, deltaBackoff));
  }

  /**
   * Initializes a new instance of the <see cref="RetryPolicy"/> class with the specified number of
   * retry attempts and parameters defining the progressive delay between retries.
   *
   * @param errorDetectionStrategy The <see cref="ITransientErrorDetectionStrategy"/> that is
   * responsible for detecting transient conditions.
   * @param retryCount The number of retry attempts.
   * @param initialInterval The initial interval that will apply for the first retry.
   * @param increment The incremental time value that will be used to calculate the progressive
   * delay between retries.
   */
  public RetryPolicy(ITransientErrorDetectionStrategy errorDetectionStrategy, int retryCount,
      Duration initialInterval, Duration increment) {
    this(errorDetectionStrategy, new Incremental(retryCount, initialInterval, increment));
  }

  /**
   * Gets the default retry policy. 5 retries at 1 second intervals.
   */
  public static RetryPolicy getDefaultRetryPolicy() {
    return DEFAULT_RETRY_POLICY;
  }

  /**
   * Returns a default policy that performs no retries, but invokes the action only once.
   */
  public static RetryPolicy getNoRetry() {
    return noRetry;
  }

  /**
   * Returns a default policy that implements a fixed retry interval configured with the default
   * <see cref="FixedInterval"/> retry strategy. The default retry policy treats all caught
   * exceptions as transient errors.
   */
  public static RetryPolicy getDefaultFixed() {
    return defaultFixed;
  }

  /**
   * Returns a default policy that implements a progressive retry interval configured with the
   * default <see cref="Incremental"/> retry strategy. The default retry policy treats all caught
   * exceptions as transient errors.
   */
  public static RetryPolicy getDefaultProgressive() {
    return defaultProgressive;
  }

  /**
   * Returns a default policy that implements a random exponential retry interval configured with
   * the default <see cref="FixedInterval"/> retry strategy. The default retry policy treats all
   * caught exceptions as transient errors.
   */
  public static RetryPolicy getDefaultExponential() {
    return defaultExponential;
  }

  public int getRetryCount() {
    return retryCount;
  }

  private void setRetryCount(int value) {
    retryCount = value;
  }

  public Duration getMinBackOff() {
    return minBackOff;
  }

  private void setMinBackOff(Duration value) {
    minBackOff = value;
  }

  public Duration getMaxBackOff() {
    return maxBackOff;
  }

  private void setMaxBackOff(Duration value) {
    maxBackOff = value;
  }

  public Duration getDeltaBackOff() {
    return deltaBackOff;
  }

  private void setDeltaBackOff(Duration value) {
    deltaBackOff = value;
  }

  /**
   * Marshals this instance into the TFH library RetryStrategy type.
   *
   * @return The RetryStrategy
   */
  public RetryStrategy getExponentialRetryStrategy() {
    return new ExponentialBackoff(this.getRetryCount(), this.getMinBackOff(), this.getMaxBackOff(),
        this.getDeltaBackOff());
  }

  /**
   * String representation of <see cref="RetryPolicy"/>.
   */
  @Override
  public String toString() {
    return String
        .format("RetryCount: %1$s, , MinBackoff: %2$s, MaxBackoff: %3$s, DeltaBackoff: %4$s",
            this.getRetryCount(), this.getMinBackOff(), this.getMaxBackOff(),
            this.getDeltaBackOff());
  }

  public final RetryStrategy getRetryStrategy() {
    return retryStrategy;
  }

  private void setRetryStrategy(RetryStrategy value) {
    retryStrategy = value;
  }

  public final ITransientErrorDetectionStrategy getErrorDetectionStrategy() {
    return errorDetectionStrategy;
  }

  private void setErrorDetectionStrategy(ITransientErrorDetectionStrategy value) {
    errorDetectionStrategy = value;
  }

  /**
   * Repetitively executes the specified action while it satisfies the current retry policy.
   *
   * @param action A delegate that represents the executable action that doesn't return any
   * results.
   */
  public void executeAction(Runnable action) throws Exception {
    Guard.argumentNotNull(action, "action");

    this.executeAction(() -> {
      action.run();
      return null;
    });
  }

  /**
   * Repetitively executes the specified action while it satisfies the current retry policy.
   * <typeparam name="ResultT">The type of result expected from the executable action.</typeparam>
   *
   * @param callable A delegate that represents the executable action that returns the result of
   * type <typeparamref name="ResultT"/>.
   * @return The result from the action.
   */
  public <ResultT> ResultT executeAction(Callable<ResultT> callable) throws Exception {
    Guard.argumentNotNull(callable, "callable");

    int retryCount = 0;
    Duration delay = Duration.ZERO;
    RuntimeException lastError;

    ShouldRetry shouldRetry = this.getRetryStrategy().getShouldRetry();

    for (; ; ) {
      lastError = null;

      try {
        return callable.call();
      /*} catch (RetryLimitExceededException limitExceededEx) {
        // The user code can throw a RetryLimitExceededException to force the exit from the retry
        // loop. The RetryLimitExceeded exception can have an inner exception attached to it. This
        // is the exception which we will have to throw up the stack so that callers can handle it.
        if (limitExceededEx.getCause() != null) {
          throw limitExceededEx.getCause();
        } else {
          return null;
        }*/
      } catch (RuntimeException ex) {
        lastError = ex;

        ReferenceObjectHelper<Duration> tempRefDelay = new ReferenceObjectHelper<>(delay);
        if (this.getErrorDetectionStrategy().isTransient(lastError)
            && shouldRetry.invoke(retryCount++, lastError, tempRefDelay)) {
          delay = tempRefDelay.argValue;
        } else {
          throw ex;
        }
      }

      // Perform an extra check in the delay interval. Should prevent from accidentally ending up
      // with the value of -1 that will block a thread indefinitely. In addition, any other negative
      // numbers will cause an OutOfRangeException fault that will be thrown by Thread.Sleep.
      if (delay.getSeconds() < 0) {
        delay = Duration.ZERO;
      }

      this.onRetrying(retryCount, lastError, delay);

      if (retryCount > 1 || !this.getRetryStrategy().getFastFirstRetry()) {
        Thread.sleep(delay.getSeconds());
      }
    }
  }

  private void onRetrying(int retryCount, RuntimeException lastError, Duration delay) {
    if (this.retrying != null) {
      this.retrying.listeners().forEach(e -> e.invoke(this, new RetryingEventArgs(retryCount,
          delay, lastError)));
    }
  }

  /**
   * Implements a strategy that treats all exceptions as transient errors.
   */
  private static final class TransientErrorCatchAllStrategy implements
      ITransientErrorDetectionStrategy {

    /**
     * Always returns true.
     *
     * @param ex The exception.
     * @return Always true.
     */
    @Override
    public boolean isTransient(Exception ex) {
      return true;
    }
  }

  /**
   * Implements a strategy that ignores any transient errors.
   */
  private static final class TransientErrorIgnoreStrategy implements
      ITransientErrorDetectionStrategy {

    /**
     * Always returns false.
     *
     * @param ex The exception.
     * @return Always false.
     */
    @Override
    public boolean isTransient(Exception ex) {
      return false;
    }
  }
}