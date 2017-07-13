package com.microsoft.azure.elasticdb.query.exception;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import java.io.Serializable;

/**
 * Custom exception to throw when the <see cref="MultiShardResultSet"/> is closed and
 * the user attempts to perform an operation on the closed reader.
 * Purpose:
 * Custom exception to throw when the MultiShardResultSet is closed and
 * the user attempts to perform some operation.
 */
public class MultiShardResultSetClosedException extends RuntimeException implements Serializable {

  /**
   * Initializes a new instance of the MultiShardReaderClosedException class with a specified error
   * message and a reference to the inner exception that is the cause of this exception.
   *
   * @param message The error message that explains the reason for the exception.
   * @param innerException The exception that is the cause of the current exception, or a null
   * reference if no inner exception is specified.
   */
  public MultiShardResultSetClosedException(String message, RuntimeException innerException) {
    super(message, innerException);
  }

  /**
   * Initializes a new instance of the MultiShardResultSetClosedException class with a specified
   * error message.
   *
   * @param message The message that describes the error.
   */
  public MultiShardResultSetClosedException(String message) {
    super(message);
  }

  /**
   * Initializes a new instance of the MultiShardResultSetClosedException class.
   */
  public MultiShardResultSetClosedException() {
    super();
  }
}