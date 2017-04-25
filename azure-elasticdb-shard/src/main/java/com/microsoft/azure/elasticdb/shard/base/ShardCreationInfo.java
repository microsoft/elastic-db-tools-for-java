package com.microsoft.azure.elasticdb.shard.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;

/**
 * Arguments used to create a <see cref="Shard"/>.
 */
public final class ShardCreationInfo {

  /**
   * Gets Location of the shard.
   */
  private ShardLocation Location;
  /**
   * Gets Status of the shard. Users can assign application-specific
   * values to the status field, which are kept together with the shard for convenience.
   */
  private ShardStatus Status = ShardStatus.values()[0];

  /**
   * Arguments used to create a <see cref="Shard"/>.
   *
   * @param location Location of the shard.
   */
  public ShardCreationInfo(ShardLocation location) {
    this(location, ShardStatus.Online);
  }

  /**
   * Arguments used to create a <see cref="Shard"/>.
   *
   * @param location Location of the shard.
   * @param status Status of the shard.
   */
  public ShardCreationInfo(ShardLocation location, ShardStatus status) {
    ExceptionUtils.DisallowNullArgument(location, "location");
    this.setLocation(location);
    this.setStatus(status);
  }

  public ShardLocation getLocation() {
    return Location;
  }

  private void setLocation(ShardLocation value) {
    Location = value;
  }

  public ShardStatus getStatus() {
    return Status;
  }

  public void setStatus(ShardStatus value) {
    Status = value;
  }
}