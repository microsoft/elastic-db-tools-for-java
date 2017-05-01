package com.microsoft.azure.elasticdb.query.exception;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/
//
// Purpose:
// Custom exception to throw when the MultiShardDataReader is closed and
// the user attempts to perform some operation.
//
// Notes:

// Suppression rationale: "Multi" is the spelling we want here.
//

import java.io.Serializable;

/**
 * Custom exception to throw when the <see cref="MultiShardDataReader"/> is closed and
 * the user attempts to perform an operation on the closed reader.
 */
public class MultiShardDataReaderClosedException extends RuntimeException implements Serializable {
  ///#region Standard Exception Constructors

  /**
   * Initializes a new instance of the MultiShardReaderClosedException class with a specified error
   * message and a reference to the inner exception that is the cause of this exception.
   *
   * @param message The error message that explains the reason for the exception.
   * @param innerException The exception that is the cause of the current exception, or a null
   * reference if no inner exception is specified.
   */
  public MultiShardDataReaderClosedException(String message, RuntimeException innerException) {
    super(message, innerException);
  }

  /**
   * Initializes a new instance of the MultiShardDataReaderClosedException class with a specified
   * error message.
   *
   * @param message The message that describes the error.
   */
  public MultiShardDataReaderClosedException(String message) {
    super(message);
  }

  /**
   * Initializes a new instance of the MultiShardDataReaderClosedException class.
   */
  public MultiShardDataReaderClosedException() {
    super();
  }

  /**
   Initializes a new instance of the MultiShardDataReaderClosedException class with serialized data.

   @param info
   The SerializationInfo that holds the serialized object data about the exception being thrown.

   @param context
   The StreamingContext that contains contextual information about the source or destination.
   */
  /*protected MultiShardDataReaderClosedException(SerializationInfo info,
    StreamingContext context) {
    super(info, context);
  }*/

  ///#endregion Standard Exception Constructors
}