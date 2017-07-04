package com.microsoft.azure.elasticdb.query.exception;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import java.io.Serializable;

/**
 * Custom exception that is thrown when the <see cref="MultiShardDataReader"/> is in an invalid
 * state. If you experience this exception repeatedly, please contact Microsoft Customer Support.
 * Purpose:
 * Custom exception to throw when the MultiShardDataReader is in an invalid state.
 * This error should not make it out to the user.
 */
public class MultiShardDataReaderInternalException extends RuntimeException implements
    Serializable {

  /**
   * Initializes a new instance of the MultiShardDataReaderInternalException class.
   */
  public MultiShardDataReaderInternalException() {
    super();
  }

  /**
   * Initializes a new instance of the MultiShardDataReaderInternalException class with a
   * specified error message.
   *
   * @param message The message that describes the error.
   */
  public MultiShardDataReaderInternalException(String message) {
    super(message);
  }

  /**
   * Initializes a new instance of the MultiShardDataReaderInternalException class
   * with a message and an inner exception.
   *
   * @param message The message to encapsulate in the exception.
   * @param innerException The underlying exception that causes this exception.
   */
  public MultiShardDataReaderInternalException(String message, RuntimeException innerException) {
    super(message, innerException);
  }
}