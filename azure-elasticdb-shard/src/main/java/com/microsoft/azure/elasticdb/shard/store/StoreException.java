package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import java.io.Serializable;

/**
 * Representation of exceptions that occur during storage operations.
 */
public final class StoreException extends RuntimeException implements Serializable {

  /**
   * Initializes a new instance with a specified error message.
   */
  public StoreException() {
    this("StoreException occured");
  }

  /**
   * Initializes a new instance with a specified error message.
   *
   * @param message Error message.
   */
  public StoreException(String message) {
    super(message);
  }

  /**
   * Initializes a new instance with a specified error message and a reference to the inner
   * exception that is the cause of this exception.
   *
   * @param message A message that describes the error
   * @param inner The exception that is the cause of the current exception
   */
  public StoreException(String message, RuntimeException inner) {
    super(message, inner);
  }

  public StoreException(String message, Exception inner) {
    super(message, inner);
  }
}
