package com.microsoft.azure.elasticdb.query.multishard;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryBehavior;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;

/**
 * Purpose: Various utilities used by other classes in this project.
 */
public final class MultiShardUtils {

  /**
   * The retry policy to use when connecting to sql databases
   *
   * @param retryPolicyPerShard An instance of the <see cref="RetryPolicy"/> class
   * @param retryBehavior Behavior to use for detecting transient faults.
   * @return An instance of the <see cref="RetryPolicy"/> class Separate method from the one below
   * because we might allow for custom retry strategies in the near future
   */
  public static RetryPolicy getSqlConnectionRetryPolicy(RetryPolicy retryPolicyPerShard,
      RetryBehavior retryBehavior) {
    return new RetryPolicy(new MultiShardQueryTransientErrorDetectionStrategy(retryBehavior),
        retryPolicyPerShard.getExponentialRetryStrategy());
  }

  /**
   * The retry policy to use when executing commands against sql databases
   *
   * @param retryPolicyPerShard An instance of the <see cref="RetryPolicy"/> class
   * @param retryBehavior Behavior to use for detecting transient faults.
   * @return An instance of the <see cref="RetryPolicy"/> class
   */
  public static RetryPolicy getSqlCommandRetryPolicy(RetryPolicy retryPolicyPerShard,
      RetryBehavior retryBehavior) {
    return new RetryPolicy(new MultiShardQueryTransientErrorDetectionStrategy(retryBehavior),
        retryPolicyPerShard.getExponentialRetryStrategy());
  }
}