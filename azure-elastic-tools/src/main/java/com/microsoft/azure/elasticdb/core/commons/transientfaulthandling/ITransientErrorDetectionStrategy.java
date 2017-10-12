package com.microsoft.azure.elasticdb.core.commons.transientfaulthandling;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Defines an interface that must be implemented by custom components responsible for detecting specific transient conditions.
 */
public interface ITransientErrorDetectionStrategy {

    /**
     * Determines whether the specified exception represents a transient failure that can be compensated by a retry.
     *
     * @param ex
     *            The exception object to be verified.
     * @return true if the specified exception is considered as transient; otherwise, false.
     */
    boolean isTransient(Exception ex);
}
