package com.microsoft.azure.elasticdb.query.multishard;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryBehavior;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;

/**
 * Purpose: Various utilities used by other classes in this project.
 */
public final class MultiShardUtils {

  /**
   * Asynchronously opens the given connection.
   *
   * @param shardConnection The connection to Open
   * @return The task handling the Open. A completed task if the conn is already Open
   */
  public static Callable openShardConnectionAsync(Connection shardConnection) {
    try {
      if (!shardConnection.isClosed()) {
        return () -> shardConnection;
      } else {
        return null;
      }
    } catch (SQLException e) {
      e.printStackTrace();
      return null;
    }
  }

  /*public static Callable OpenShardConnectionAsync(Connection shardConnection,
      CancellationToken cancellationToken) {
    if (!shardConnection.isClosed()) {
      return shardConnection.OpenAsync(cancellationToken);
    } else {
      return null;
    }
  }*/

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
        RetryPolicy.getRetryStrategy());
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
        RetryPolicy.getRetryStrategy());
  }

  /**
   * Clones the given command object and associates with the given connection.
   *
   * @param cmd Command object to clone.
   * @param conn Connection associated with the cloned command.
   * @return clone of <paramref name="cmd"/>.
   */
  public static DbCommand cloneDbCommand(DbCommand cmd, Connection conn) {
    DbCommand clone = cmd.clone();
    clone.setConnection(conn);

    return clone;
  }
}