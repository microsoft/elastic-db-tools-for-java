package com.microsoft.azure.elasticdb.shard.mapmanager;

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
import com.microsoft.azure.elasticdb.shard.utils.SqlUtils;

/**
 * Provides the transient error detection logic for transient faults that are specific to Shard map manager.
 */
public final class ShardManagementTransientErrorDetectionStrategy implements ITransientErrorDetectionStrategy {

    /**
     * Delegate used for detecting transient faults.
     */
    private Function<Exception, Boolean> transientFaultDetector;

    /**
     * Creates a new instance of transient error detection strategy for Shard map manager.
     *
     * @param retryBehavior
     *            User specified retry behavior.
     */
    public ShardManagementTransientErrorDetectionStrategy(RetryBehavior retryBehavior) {
        transientFaultDetector = retryBehavior.getTransientErrorDetector();
    }

    /**
     * Determines whether the specified exception represents a transient failure that can be compensated by a retry.
     *
     * @param ex
     *            The exception object to be verified.
     * @return true if the specified exception is considered as transient; otherwise, false.
     */
    public boolean isTransient(Exception ex) {
        return SqlUtils.getTransientErrorDetector().apply(ex) || transientFaultDetector.apply(ex);
    }
}