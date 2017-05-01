package com.microsoft.azure.elasticdb.query.multishard;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.ITransientErrorDetectionStrategy;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryBehavior;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.SqlDatabaseTransientErrorDetectionStrategy;
import java.util.function.Function;

/**
 * Provides the transient error detection logic for transient faults that are specific to cross
 * shard query.
 */
public final class MultiShardQueryTransientErrorDetectionStrategy implements
    ITransientErrorDetectionStrategy {

  /**
   * Delegate used for detecting transient faults.
   */
  private Function<RuntimeException, Boolean> transientFaultDetector;

  /**
   * Standard transient error detection strategy.
   */
  private SqlDatabaseTransientErrorDetectionStrategy standardDetectionStrategy;

  /**
   * Creates a new instance of transient error detection strategy for Shard map manager.
   *
   * @param retryBehavior Behavior for detecting transient errors.
   */
  public MultiShardQueryTransientErrorDetectionStrategy(RetryBehavior retryBehavior) {
    standardDetectionStrategy = new SqlDatabaseTransientErrorDetectionStrategy();
    //TODO:
    /*transientFaultDetector = (RuntimeException arg) -> retryBehavior.getTransientErrorDetector()
        .invoke(arg);*/
  }

  /**
   * Determines whether the specified exception represents a transient failure that can be
   * compensated by a retry.
   *
   * @param ex The exception object to be verified.
   * @return true if the specified exception is considered as transient; otherwise, false.
   */
  @Override
  public boolean isTransient(Exception ex) {
    return standardDetectionStrategy
        .isTransient(ex); //TODO: || transientFaultDetector.invoke(ex);
  }
}