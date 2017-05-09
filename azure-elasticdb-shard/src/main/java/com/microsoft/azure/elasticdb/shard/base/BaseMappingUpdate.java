package com.microsoft.azure.elasticdb.shard.base;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;

/**
 * Base class for updates to mappings from shardlets to shards.
 * <typeparam name="StatusT">Type of status field.</typeparam>
 */
public abstract class BaseMappingUpdate<StatusT> implements IMappingUpdate<StatusT> {

  /**
   * Records the modified properties for update.
   */
  private MappingUpdatedProperties updatedProperties;

  /**
   * Holder for update to status property.
   */
  private StatusT status;

  /**
   * Holder for update to shard property.
   */
  private Shard shard;

  /**
   * Gets the Status property.
   */
  public final StatusT getStatus() {
    return status;
  }

  /**
   * Sets the Status property.
   */
  public final void setStatus(StatusT value) {
    status = value;
    updatedProperties = updatedProperties == null ? MappingUpdatedProperties.Status
        : MappingUpdatedProperties.forValue(updatedProperties.getValue()
            | MappingUpdatedProperties.Status.getValue());
  }

  /**
   * Gets the Shard property.
   */
  public final Shard getShard() {
    return shard.clone();
  }

  /**
   * Sets the Shard property.
   */
  public final void setShard(Shard value) {
    ExceptionUtils.disallowNullArgument(value, "value");
    shard = value.clone();
    updatedProperties = updatedProperties == null ? MappingUpdatedProperties.Shard
        : MappingUpdatedProperties.forValue(updatedProperties.getValue()
            | MappingUpdatedProperties.Shard.getValue());
  }

  /**
   * Checks if any property is set in the given bitmap.
   *
   * @param properties Properties bitmap.
   * @return True if any of the properties is set, false otherwise.
   */
  public final boolean isAnyPropertySet(MappingUpdatedProperties properties) {
    return (updatedProperties.getValue() & properties.getValue()) != 0;
  }

  /**
   * Checks if the mapping is being taken offline.
   *
   * @param originalStatus Original status.
   * @return True of the update will take the mapping offline.
   */
  public final boolean isMappingBeingTakenOffline(StatusT originalStatus) {
    return (updatedProperties.getValue() & MappingUpdatedProperties.Status.getValue())
        == MappingUpdatedProperties.Status.getValue() && this.isBeingTakenOffline(originalStatus,
        this.getStatus());
  }

  /**
   * Detects if the current mapping is being taken offline.
   *
   * @param originalStatus Original status.
   * @param updatedStatus Updated status.
   * @return Detects in the derived types if the mapping is being taken offline.
   */
  protected abstract boolean isBeingTakenOffline(StatusT originalStatus, StatusT updatedStatus);
}
