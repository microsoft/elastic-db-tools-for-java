package com.microsoft.azure.elasticdb.core.commons.transientfaulthandling;

import java.time.Duration;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;

/**
 * Represents a retry strategy with a specified number of retry attempts and a default, fixed time interval between retries.
 */
public class FixedInterval extends RetryStrategy {

    private int retryCount;
    private Duration retryInterval = Duration.ZERO;

    /**
     * Initializes a new instance of the <see cref="FixedInterval"/> class.
     */
    public FixedInterval() {
        this(DEFAULT_CLIENT_RETRY_COUNT);
    }

    /**
     * Initializes a new instance of the <see cref="FixedInterval"/> class with the specified number of retry attempts.
     *
     * @param retryCount
     *            The number of retry attempts.
     */
    public FixedInterval(int retryCount) {
        this(retryCount, DEFAULT_RETRY_INTERVAL);
    }

    /**
     * Initializes a new instance of the <see cref="FixedInterval"/> class with the specified number of retry attempts and time interval.
     *
     * @param retryCount
     *            The number of retry attempts.
     * @param retryInterval
     *            The time interval between retries.
     */
    public FixedInterval(int retryCount,
            Duration retryInterval) {
        this(null, retryCount, retryInterval, DEFAULT_FIRST_FAST_RETRY);
    }

    /**
     * Initializes a new instance of the <see cref="FixedInterval"/> class with the specified number of retry attempts, time interval, and retry
     * strategy.
     *
     * @param name
     *            The retry strategy name.
     * @param retryCount
     *            The number of retry attempts.
     * @param retryInterval
     *            The time interval between retries.
     */
    public FixedInterval(String name,
            int retryCount,
            Duration retryInterval) {
        this(name, retryCount, retryInterval, DEFAULT_FIRST_FAST_RETRY);
    }

    /**
     * Initializes a new instance of the <see cref="FixedInterval"/> class with the specified number of retry attempts, time interval, retry strategy,
     * and fast start option.
     *
     * @param name
     *            The retry strategy name.
     * @param retryCount
     *            The number of retry attempts.
     * @param retryInterval
     *            The time interval between retries.
     * @param firstFastRetry
     *            true to immediately retry in the first attempt; otherwise, false. The subsequent retries will remain subject to the configured retry
     *            interval.
     */
    public FixedInterval(String name,
            int retryCount,
            Duration retryInterval,
            boolean firstFastRetry) {
        super(name, firstFastRetry);
        Guard.argumentNotNegativeValue(retryCount, "retryCount");
        Guard.argumentNotNegativeValue(retryInterval.getSeconds(), "retryInterval");

        this.retryCount = retryCount;
        this.retryInterval = retryInterval;
    }

    /**
     * Returns the corresponding ShouldRetry delegate.
     *
     * @return The ShouldRetry delegate.
     */
    @Override
    public ShouldRetry getShouldRetry() {
        if (retryCount == 0) {
            return (int currentRetryCount,
                    RuntimeException lastException,
                    ReferenceObjectHelper<Duration> interval) -> {
                interval.argValue = Duration.ZERO;
                return false;
            };
        }

        return (int currentRetryCount,
                RuntimeException lastException,
                ReferenceObjectHelper<Duration> interval) -> {
            if (currentRetryCount < retryCount) {
                interval.argValue = retryInterval;
                return true;
            }

            interval.argValue = Duration.ZERO;
            return false;
        };
    }
}