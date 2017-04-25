package com.microsoft.azure.elasticdb.shard.cache;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.store.StoreMapping;

/**
 * Represents a cache entry for a mapping.
 */
public interface ICacheStoreMapping {

  /**
   * Store representation of mapping.
   */
  StoreMapping getMapping();

  /**
   * Mapping entry creation time.
   */
  long getCreationTime();

  /**
   * Mapping entry expiration time.
   */
  long getTimeToLiveMilliseconds();

  /**
   * Resets the mapping entry expiration time to 0.
   */
  void ResetTimeToLive();

  /**
   * Whether TimeToLiveMilliseconds have elapsed
   * since CreationTime
   */
  boolean HasTimeToLiveExpired();
}
