package com.microsoft.azure.elasticdb.core.commons.transientfaulthandling;

import java.time.Duration;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.core.commons.helpers.EventArgs;

/**
 * Shard management retrying event arguments.
 */
public final class RetryingEventArgs extends EventArgs {

    /**
     * Gets the current retry count.
     */
    private int currentRetryCount;
    /**
     * Gets the delay that indicates how long the current thread will be suspended before the next iteration is invoked.
     */
    private Duration delay = Duration.ZERO;
    /**
     * Gets the exception that caused the retry conditions to occur.
     */
    private RuntimeException lastException;

    /**
     * Initializes new instance of <see cref="RetryingEventArgs"/> class.
     *
     * @param arg
     *            RetryingEventArgs from RetryPolicy.retrying event.
     */
    public RetryingEventArgs(RetryingEventArgs arg) {
        this.setCurrentRetryCount(arg.currentRetryCount);
        this.setDelay(arg.delay);
        this.setLastException(arg.lastException);
    }

    /**
     * Initializes a new instance of the <see cref="RetryingEventArgs"/> class.
     *
     * @param retryCount
     *            The current retry attempt count.
     * @param delay
     *            The delay that indicates how long the current thread will be suspended before the next iteration is invoked.
     * @param ex
     *            The exception that caused the retry conditions to occur.
     */
    public RetryingEventArgs(int retryCount,
            Duration delay,
            RuntimeException ex) {
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