package com.microsoft.azure.elasticdb.core.commons.logging;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import java.util.UUID;

//
// Purpose: Utility class to set and restore the System.Diagnostics CorrelationManager
// ActivityId via the using pattern

/**
 * Utility class to set and restore the System.Diagnostics CorrelationManager
 * ActivityId via the using pattern
 */
public final class ActivityIdScope implements AutoCloseable {

  /**
   * The previous activity id that was in scope
   */
  private UUID _previousActivityId;

  /**
   * Creates an instance of the <see cref="ActivityIdScope"/> class
   */
  public ActivityIdScope(UUID activityId) {
    // TODO
  }

  /**
   * Restores the previous activity id when this instance is disposed
   */
  public void close() {
    // TODO
  }
}