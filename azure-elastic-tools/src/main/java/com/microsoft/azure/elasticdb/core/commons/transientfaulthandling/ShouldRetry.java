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
 * Defines a callback delegate that will be invoked whenever a retry condition is encountered.
 */
@FunctionalInterface
public interface ShouldRetry {

    /**
     * Defines a callback delegate that will be invoked whenever a retry condition is encountered.
     *
     * @param retryCount
     *            The current retry attempt count.
     * @param lastException
     *            The exception that caused the retry conditions to occur.
     * @param delay
     *            The delay that indicates how long the current thread will be suspended before the next iteration is invoked.
     * @return <see langword="true"/> if a retry is allowed; otherwise, <see langword="false"/>.
     */
    boolean invoke(int retryCount,
            RuntimeException lastException,
            ReferenceObjectHelper<Duration> delay);
}