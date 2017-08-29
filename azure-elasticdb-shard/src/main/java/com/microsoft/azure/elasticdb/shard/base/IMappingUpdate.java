package com.microsoft.azure.elasticdb.shard.base;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

/**
 * Common interface for point/range mapping updates.
 * <typeparam name="StatusT">Status type.</typeparam>
 */
public interface IMappingUpdate<StatusT> {

  /**
   * Status property.
   */
  StatusT getStatus();

  /**
   * Shard property.
   */
  Shard getShard();

  /**
   * Checks if any property is set in the given bitmap.
   *
   * @param properties Properties bitmap.
   * @return True if any of the properties is set, false otherwise.
   */
  boolean isAnyPropertySet(MappingUpdatedProperties properties);

  /**
   * Checks if the mapping is being taken offline.
   *
   * @param originalStatus Original status.
   * @return True of the update will take the mapping offline.
   */
  boolean isMappingBeingTakenOffline(StatusT originalStatus);
}