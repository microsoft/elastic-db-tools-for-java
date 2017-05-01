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
  private ShardLocation SourceLocation;
  /**
   * Optional target shard location.
   */
  private ShardLocation TargetLocation;

  public final ShardLocation getSourceLocation() {
    return SourceLocation;
  }

  public final void setSourceLocation(ShardLocation value) {
    SourceLocation = value;
  }

  public final ShardLocation getTargetLocation() {
    return TargetLocation;
  }

  public final void setTargetLocation(ShardLocation value) {
    TargetLocation = value;
  }
}
