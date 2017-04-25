package com.microsoft.azure.elasticdb.shard.cache;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * Encapsulates extension methods for CacheMappings
 */
public final class CacheMappingExtensions {

  /**
   * Resets the mapping entry expiration time to 0 if necessary
   */
  public static void ResetTimeToLiveIfNecessary(ICacheStoreMapping csm) {
    // Reset TTL on successful connection.
    if (csm != null && csm.getTimeToLiveMilliseconds() > 0) {
      csm.ResetTimeToLive();
    }
  }
}