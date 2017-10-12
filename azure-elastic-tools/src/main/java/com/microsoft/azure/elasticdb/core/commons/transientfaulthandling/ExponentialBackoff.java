package com.microsoft.azure.elasticdb.core.commons.transientfaulthandling;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;

/**
 * A retry strategy with backoff parameters for calculating the exponential delay between retries.
 */
public class ExponentialBackoff extends RetryStrategy {

    private int retryCount;
    private Duration minBackoff = Duration.ZERO;
    private Duration maxBackoff = Duration.ZERO;
    private Duration deltaBackoff = Duration.ZERO;

    /**
     * Initializes a new instance of the <see cref="ExponentialBackoff"/> class.
     */
    public ExponentialBackoff() {
        this(DEFAULT_CLIENT_RETRY_COUNT, DEFAULT_MIN_BACKOFF, DEFAULT_MAX_BACKOFF, DEFAULT_CLIENT_BACKOFF);
    }

    /**
     * Initializes a new instance of the <see cref="ExponentialBackoff"/> class with the specified retry settings.
     *
     * @param retryCount
     *            The maximum number of retry attempts.
     * @param minBackoff
     *            The minimum backoff time
     * @param maxBackoff
     *            The maximum backoff time.
     * @param deltaBackoff
     *            The value that will be used to calculate a random delta in the exponential delay between retries.
     */
    public ExponentialBackoff(int retryCount,
            Duration minBackoff,
            Duration maxBackoff,
            Duration deltaBackoff) {
        this(null, retryCount, minBackoff, maxBackoff, deltaBackoff, DEFAULT_FIRST_FAST_RETRY);
    }

    /**
     * Initializes a new instance of the <see cref="ExponentialBackoff"/> class with the specified name and retry settings.
     *
     * @param name
     *            The name of the retry strategy.
     * @param retryCount
     *            The maximum number of retry attempts.
     * @param minBackoff
     *            The minimum backoff time
     * @param maxBackoff
     *            The maximum backoff time.
     * @param deltaBackoff
     *            The value that will be used to calculate a random delta in the exponential delay between retries.
     */
    public ExponentialBackoff(String name,
            int retryCount,
            Duration minBackoff,
            Duration maxBackoff,
            Duration deltaBackoff) {
        this(name, retryCount, minBackoff, maxBackoff, deltaBackoff, DEFAULT_FIRST_FAST_RETRY);
    }

    /**
     * Initializes a new instance of the <see cref="ExponentialBackoff"/> class with the specified name, retry settings, and fast retry option.
     *
     * @param name
     *            The name of the retry strategy.
     * @param retryCount
     *            The maximum number of retry attempts.
     * @param minBackoff
     *            The minimum backoff time
     * @param maxBackoff
     *            The maximum backoff time.
     * @param deltaBackoff
     *            The value that will be used to calculate a random delta in the exponential delay between retries.
     * @param firstFastRetry
     *            true to immediately retry in the first attempt; otherwise, false. The subsequent retries will remain subject to the configured retry
     *            interval.
     */
    public ExponentialBackoff(String name,
            int retryCount,
            Duration minBackoff,
            Duration maxBackoff,
            Duration deltaBackoff,
            boolean firstFastRetry) {
        super(name, firstFastRetry);
        Guard.argumentNotNegativeValue(retryCount, "retryCount");
        Guard.argumentNotNegativeValue(minBackoff.getSeconds(), "minBackoff");
        Guard.argumentNotNegativeValue(maxBackoff.getSeconds(), "maxBackoff");
        Guard.argumentNotNegativeValue(deltaBackoff.getSeconds(), "deltaBackoff");
        Guard.argumentNotGreaterThan(minBackoff.getSeconds(), maxBackoff.getSeconds(), "minBackoff");

        this.retryCount = retryCount;
        this.minBackoff = minBackoff;
        this.maxBackoff = maxBackoff;
        this.deltaBackoff = deltaBackoff;
    }

    /**
     * Returns the corresponding ShouldRetry delegate.
     *
     * @return The ShouldRetry delegate.
     */
    @Override
    public ShouldRetry getShouldRetry() {
        return (int currentRetryCount,
                RuntimeException lastException,
                ReferenceObjectHelper<Duration> refRetryInterval) -> {
            if (currentRetryCount < this.retryCount) {
                Double delta = this.deltaBackoff == Duration.ZERO ? 0.0
                        : (Math.pow(2.0, currentRetryCount) - 1) * ThreadLocalRandom.current().nextDouble((this.deltaBackoff.getSeconds() * 0.8),
                                (this.deltaBackoff.getSeconds() * 1.2));
                Long interval = Math.min((this.minBackoff.getSeconds() + delta.intValue()), this.maxBackoff.getSeconds());
                refRetryInterval.argValue = Duration.ofMillis(interval);
                return true;
            }

            refRetryInterval.argValue = Duration.ZERO;
            return false;
        };
    }
}