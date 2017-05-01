package com.microsoft.azure.elasticdb.shard.schema;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import java.io.Serializable;
import java.util.Locale;

/**
 * The exception that is thrown when an error occurs during operations related to schema info
 * collection.
 */
public final class SchemaInfoException extends RuntimeException implements Serializable {

  /**
   * Error code.
   */
  private SchemaInfoErrorCode errorCode;

  /**
   * Initializes a new instance.
   */
  public SchemaInfoException() {
    super();
  }

  /**
   * Initializes a new instance with a specified error message.
   *
   * @param message Error message.
   */
  public SchemaInfoException(String message) {
    super(message);
  }

  /**
   * Initializes a new instance with a specified formatted error message.
   *
   * @param format The format message that describes the error.
   * @param args The arguments to the format string.
   */
  public SchemaInfoException(String format, Object... args) {
    super(String.format(Locale.getDefault(), format, args));
  }

  /**
   * Initializes a new instance with a specified error message and a reference to the inner
   * exception that caused this exception.
   *
   * @param message A message that describes the error.
   * @param inner The exception that is the cause of the current exception.
   */
  public SchemaInfoException(String message, RuntimeException inner) {
    super(message, inner);
  }

  /**
   * Initializes a new instance with a specified formatted error message.
   *
   * @param code Error code.
   * @param format The format message that describes the error
   * @param args The arguments to the format string
   */
  public SchemaInfoException(SchemaInfoErrorCode code, String format, Object... args) {
    super(String.format(Locale.getDefault(), format, args));
    this.setErrorCode(code);
  }

  /**
   * Initializes a new instance with serialized data.
   * @param info    The object that holds the serialized object data
   * @param context The contextual information about the source or destination
   */
  /*private SchemaInfoException(SerializationInfo info, StreamingContext context) {
    super(info, context);
    this.setErrorCode(
        (SchemaInfoErrorCode) info.GetValue("errorCode", ShardManagementErrorCode.class));
  }*/

  /**
   * Populates a SerializationInfo with the data needed to serialize the target object.
   * @param info    The SerializationInfo to populate with data.
   * @param context The destination (see StreamingContext) for this serialization.
   */
  /*@Override
  public void GetObjectData(SerializationInfo info, StreamingContext context) {
    if (info != null) {
      info.AddValue("errorCode", getErrorCode());
      super.GetObjectData(info, context);
    }
  }*/

  /**
   * Initializes a new instance with a specified error message and a reference to the inner
   * exception that is the cause of this exception.
   *
   * @param code Error code.
   * @param message A message that describes the error
   * @param inner The exception that is the cause of the current exception
   */
  public SchemaInfoException(SchemaInfoErrorCode code, String message, RuntimeException inner) {
    super(message, inner);
    this.setErrorCode(code);
  }

  public SchemaInfoErrorCode getErrorCode() {
    return errorCode;
  }

  private void setErrorCode(SchemaInfoErrorCode value) {
    errorCode = value;
  }
}