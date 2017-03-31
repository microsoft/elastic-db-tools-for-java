package com.microsoft.azure.elasticdb.core.commons.transientfaulthandling;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.helpers.EventArgs;

import java.time.Duration;

/**
 * Shard management retrying event arguments
 */
public final class RetryingEventArgs extends EventArgs {
    /**
     * Gets the current retry count.
     */
    private int CurrentRetryCount;
    /**
     * Gets the delay that indicates how long the current thread will be suspended before the next iteration is invoked.
     */
    private Duration Delay = Duration.ZERO;
    /**
     * Gets the exception that caused the retry conditions to occur.
     */
    private RuntimeException LastException;

    /**
     * Initializes new instance of <see cref="RetryingEventArgs"/> class.
     *
     * @param arg RetryingEventArgs from RetryPolicy.Retrying event.
     */
    public RetryingEventArgs(RetryingEventArgs arg) {
        this.setCurrentRetryCount(arg.CurrentRetryCount);
        this.setDelay(arg.Delay);
        this.setLastException(arg.LastException);
    }

    public int getCurrentRetryCount() {
        return CurrentRetryCount;
    }

    private void setCurrentRetryCount(int value) {
        CurrentRetryCount = value;
    }

    public Duration getDelay() {
        return Delay;
    }

    private void setDelay(Duration value) {
        Delay = value;
    }

    public RuntimeException getLastException() {
        return LastException;
    }

    private void setLastException(RuntimeException value) {
        LastException = value;
    }
}