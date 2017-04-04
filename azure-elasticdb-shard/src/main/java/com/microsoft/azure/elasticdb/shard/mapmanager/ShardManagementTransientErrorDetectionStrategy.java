package com.microsoft.azure.elasticdb.shard.mapmanager;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.ITransientErrorDetectionStrategy;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryBehavior;

import java.util.function.Function;

/**
 * Provides the transient error detection logic for transient faults that are specific to Shard map manager.
 */
public final class ShardManagementTransientErrorDetectionStrategy implements ITransientErrorDetectionStrategy {

    /**
     * Delegate used for detecting transient faults.
     */
    private Function<Exception, Boolean> _transientFaultDetector;

    /**
     * Creates a new instance of transient error detection strategy for Shard map manager.
     *
     * @param retryBehavior User specified retry behavior.
     */
    public ShardManagementTransientErrorDetectionStrategy(RetryBehavior retryBehavior) {
        _transientFaultDetector = retryBehavior.getTransientErrorDetector();
    }

    /**
     * Determines whether the specified exception represents a transient failure that can be compensated by a retry.
     *
     * @param ex The exception object to be verified.
     * @return true if the specified exception is considered as transient; otherwise, false.
     */
    public boolean isTransient(Exception ex) {
        return /*SqlUtils.TransientErrorDetector(ex) || */_transientFaultDetector.apply(ex);
    }
}