package com.microsoft.azure.elasticdb.core.commons.transientfaulthandling;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * Defines an interface that must be implemented by custom components responsible for detecting specific transient conditions.
 */
public interface ITransientErrorDetectionStrategy {
    /**
     * Determines whether the specified exception represents a transient failure that can be compensated by a retry.
     *
     * @param ex The exception object to be verified.
     * @return true if the specified exception is considered as transient; otherwise, false.
     */
    boolean isTransient(Exception ex);
}
