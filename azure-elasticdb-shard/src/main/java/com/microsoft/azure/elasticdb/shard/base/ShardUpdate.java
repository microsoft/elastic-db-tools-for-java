package com.microsoft.azure.elasticdb.shard.base;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

/**
 * Represents updates to a Shard.
 */
public final class ShardUpdate {

  /**
   * Records the modified properties for update.
   */
  private ShardUpdatedProperties _updatedProperties = ShardUpdatedProperties.forValue(0);

  /**
   * Holder for update to status property.
   */
  private ShardStatus _status = ShardStatus.values()[0];

  /**
   * Instantiates the shard update object with no property set.
   */
  public ShardUpdate() {
  }

  /**
   * Status property.
   */
  public ShardStatus getStatus() {
    return _status;
  }

  public void setStatus(ShardStatus value) {
    _status = value;
    int shardUpdatePropertyValue = _updatedProperties == null
        ? ShardUpdatedProperties.Status.getValue()
        : _updatedProperties.getValue() | ShardUpdatedProperties.Status.getValue();
    _updatedProperties = ShardUpdatedProperties
        .forValue(shardUpdatePropertyValue);
  }

  /**
   * Checks if any of the properties specified in the given bitmap have
   * been set by the user.
   *
   * @param p Bitmap of properties.
   * @return True if any property is set, false otherwise.
   */
  public boolean IsAnyPropertySet(ShardUpdatedProperties p) {
    return (_updatedProperties.getValue() & p.getValue()) != 0;
  }
}