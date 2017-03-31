package com.microsoft.azure.elasticdb.core.commons.transientfaulthandling;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.helpers.ActionGeneric1Param;

/**
 * Defines the retry behavior to use for detecting transient errors.
 */
public final class RetryBehavior {
    /**
     * Retry policy that tries upto 5 times with a 1 second backoff before giving up
     */
    private static final RetryBehavior s_defaultRetryBehavior = new RetryBehavior((e) -> false);
    /**
     * Transient error detector predicate which decides whether a given exception is transient or not.
     */
    private ActionGeneric1Param<RuntimeException, Boolean> TransientErrorDetector;

    /**
     * Initializes an instance of the <see cref="RetryBehavior"/> class
     *
     * @param transientErrorDetector Function that detects transient errors given an exception.
     *                               The function needs to return true for an exception that should be treated as transient.
     */
    public RetryBehavior(ActionGeneric1Param<RuntimeException, Boolean> transientErrorDetector) {
        if (transientErrorDetector == null) {
            throw new IllegalArgumentException("transientErrorDetector");
        }

        this.setTransientErrorDetector((RuntimeException arg) -> transientErrorDetector.invoke(arg));
    }

    /**
     * Gets the default retry behavior.
     * <p>
     * <p>
     * The default retry behavior has a built-in set of exceptions that are considered transient.
     * You may create and use a custom <see cref="RetryBehavior"/> object in order
     * to treat additional exceptions as transient.
     */
    public static RetryBehavior getDefaultRetryBehavior() {
        return s_defaultRetryBehavior;
    }

    public ActionGeneric1Param<RuntimeException, Boolean> getTransientErrorDetector() {
        return TransientErrorDetector;
    }

    public void setTransientErrorDetector(ActionGeneric1Param<RuntimeException, Boolean> value) {
        TransientErrorDetector = (RuntimeException arg) -> value.invoke(arg);
    }
}