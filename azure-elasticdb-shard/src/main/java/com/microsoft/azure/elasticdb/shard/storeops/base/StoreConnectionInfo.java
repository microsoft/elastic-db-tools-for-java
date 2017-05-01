package com.microsoft.azure.elasticdb.shard.storeops.base;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;

/**
 * Provides information regarding LSM connections.
 */
public class StoreConnectionInfo {

  /**
   * Optional source shard location.
   */
  private ShardLocation sourceLocation;
  /**
   * Optional target shard location.
   */
  private ShardLocation targetLocation;

  public final ShardLocation getSourceLocation() {
    return sourceLocation;
  }

  public final void setSourceLocation(ShardLocation value) {
    sourceLocation = value;
  }

  public final ShardLocation getTargetLocation() {
    return targetLocation;
  }

  public final void setTargetLocation(ShardLocation value) {
    targetLocation = value;
  }
}
