package com.microsoft.azure.elasticdb.shard.utils;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * Implementation of name locks. Allows mutual exclusion on names.
 */
public final class NameLock extends ValueLock<String> {

  /**
   * Instantiates a name lock with given name and acquires the name lock.
   *
   * @param name Given name.
   */
  public NameLock(String name) {
    super(name);
  }
}