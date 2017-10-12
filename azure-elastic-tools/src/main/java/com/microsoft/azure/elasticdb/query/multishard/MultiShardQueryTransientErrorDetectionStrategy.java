package com.microsoft.azure.elasticdb.query.multishard;

import java.util.function.Function;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.ITransientErrorDetectionStrategy;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryBehavior;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.SqlDatabaseTransientErrorDetectionStrategy;

/**
 * Provides the transient error detection logic for transient faults that are specific to cross shard query.
 */
public final class MultiShardQueryTransientErrorDetectionStrategy implements ITransientErrorDetectionStrategy {

    /**
     * Delegate used for detecting transient faults.
     */
    private Function<Exception, Boolean> transientFaultDetector;

    /**
     * Standard transient error detection strategy.
     */
    private SqlDatabaseTransientErrorDetectionStrategy standardDetectionStrategy;

    /**
     * Creates a new instance of transient error detection strategy for Shard map manager.
     *
     * @param retryBehavior
     *            Behavior for detecting transient errors.
     */
    public MultiShardQueryTransientErrorDetectionStrategy(RetryBehavior retryBehavior) {
        standardDetectionStrategy = new SqlDatabaseTransientErrorDetectionStrategy();
        transientFaultDetector = (Exception arg) -> retryBehavior.getTransientErrorDetector().apply(arg);
    }

    /**
     * Determines whether the specified exception represents a transient failure that can be compensated by a retry.
     *
     * @param ex
     *            The exception object to be verified.
     * @return true if the specified exception is considered as transient; otherwise, false.
     */
    @Override
    public boolean isTransient(Exception ex) {
        return standardDetectionStrategy.isTransient(ex) || transientFaultDetector.apply(ex);
    }
}