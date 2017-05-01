package com.microsoft.azure.elasticdb.query.exception;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/
//
// Purpose:
// Custom exception to throw when the MultiShardDataReader is in an invalid state.
// This error should not make it out to the user.
//
// Notes:

// Suppression rationale: "Multi" is the spelling we want here.
//

import java.io.Serializable;

/**
 * Custom exception that is thrown when the <see cref="MultiShardDataReader"/> is in an invalid
 * state. If you experience this exception repeatedly, please contact Microsoft Customer Support.
 */
public class MultiShardDataReaderInternalException extends RuntimeException implements
    Serializable {
  ///#region Standard Exception Constructors

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

  /**
   * Initializes a new instance of the MultiShardDataReaderInternalException class
   * with serialized data and context.
   *
   * @param info    The <see cref="SerializationInfo"/> holds the serialized object data about the
   * exception being thrown.
   * @param context The <see cref="StreamingContext"/> that contains contextual information about
   * the source or destination.
   */
  /*protected MultiShardDataReaderInternalException(SerializationInfo info,
      StreamingContext context) {
    super(info, context);
  }*/

  ///#endregion Standard Exception Constructors
}