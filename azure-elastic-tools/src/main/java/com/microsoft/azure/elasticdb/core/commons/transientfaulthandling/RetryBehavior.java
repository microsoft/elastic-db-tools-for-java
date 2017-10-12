package com.microsoft.azure.elasticdb.core.commons.transientfaulthandling;

import java.util.function.Function;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.google.common.base.Preconditions;

/**
 * Defines the retry behavior to use for detecting transient errors.
 */
public final class RetryBehavior {

    /**
     * Retry policy that tries up to 5 times with a 1 second backoff before giving up.
     */
    private static final RetryBehavior DEFAULT_RETRY_BEHAVIOR = new RetryBehavior((e) -> false);
    /**
     * Transient error detector predicate which decides whether a given exception is transient or not.
     */
    private Function<Exception, Boolean> transientErrorDetector;

    /**
     * Initializes an instance of the <see cref="RetryBehavior"/> class.
     *
     * @param transientErrorDetector
     *            Function that detects transient errors given an exception. The function needs to return true for an exception that should be treated
     *            as transient.
     */
    public RetryBehavior(Function<Exception, Boolean> transientErrorDetector) {
        this.transientErrorDetector = Preconditions.checkNotNull(transientErrorDetector);
    }

    /**
     * Gets the default retry behavior. The default retry behavior has a built-in set of exceptions that are considered transient. You may create and
     * use a custom <see cref="RetryBehavior"/> object in order to treat additional exceptions as transient.
     */
    public static RetryBehavior getDefaultRetryBehavior() {
        return DEFAULT_RETRY_BEHAVIOR;
    }

    public Function<Exception, Boolean> getTransientErrorDetector() {
        return transientErrorDetector;
    }
}