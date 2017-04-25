package com.microsoft.azure.elasticdb.shard.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * Common interface for point/range mapping updates.
 * <typeparam name="TStatus">Status type.</typeparam>
 */
public interface IMappingUpdate<TStatus> {

  /**
   * Status property.
   */
  TStatus getStatus();

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
  boolean IsAnyPropertySet(MappingUpdatedProperties properties);

  /**
   * Checks if the mapping is being taken offline.
   *
   * @param originalStatus Original status.
   * @return True of the update will take the mapping offline.
   */
  boolean IsMappingBeingTakenOffline(TStatus originalStatus);
}