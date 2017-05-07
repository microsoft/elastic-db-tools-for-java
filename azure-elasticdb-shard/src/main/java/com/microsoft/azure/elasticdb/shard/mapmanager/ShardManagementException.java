package com.microsoft.azure.elasticdb.shard.mapmanager;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import java.io.Serializable;
import java.util.Locale;

/**
 * Representation of exceptions that occur during storage operations.
 */
public final class ShardManagementException extends RuntimeException implements Serializable {

  /**
   * Error category.
   */
  private ShardManagementErrorCategory errorCategory = ShardManagementErrorCategory.values()[0];
  /**
   * Error code.
   */
  private ShardManagementErrorCode errorCode = ShardManagementErrorCode.values()[0];

  /**
   * Initializes a new instance with a specified error message.
   *
   * @param category Category of error.
   * @param code Error code.
   * @param message Error message.
   */
  public ShardManagementException(ShardManagementErrorCategory category,
      ShardManagementErrorCode code, String message) {
    super(message);
    this.setErrorCategory(category);
    this.setErrorCode(code);
  }

  /**
   * Initializes a new instance with a specified formatted error message.
   *
   * @param category Category of error.
   * @param code Error code.
   * @param format The format message that describes the error
   * @param args The arguments to the format string
   */
  public ShardManagementException(ShardManagementErrorCategory category,
      ShardManagementErrorCode code, String format, Object... args) {
    super(String.format(Locale.getDefault(), format, args));
    this.setErrorCategory(category);
    this.setErrorCode(code);
  }

  //TODO:
  /**
   * Initializes a new instance with serialized data.
   * @param info    The object that holds the serialized object data
   * @param context The contextual information about the source or destination
   */
  /*private ShardManagementException(SerializabletionInfo info, StreamingContext context) {
    super(info, context);
    this.setErrorCategory(ShardManagementErrorCategory
        .forValue((Integer) info.GetValue("errorCategory", ShardManagementErrorCategory.class)));
    this.setErrorCode(ShardManagementErrorCode
        .forValue((Integer) info.GetValue("errorCode", ShardManagementErrorCode.class)));
  }*/

  ///#region Serialization Support

  /**
   * Populates a SerializationInfo with the data needed to serialize the target object.
   *
   * @param info The SerializationInfo to populate with data.
   * @param context The destination (see StreamingContext) for this serialization.
   */
  /*@Override
  public void GetObjectData(SerializationInfo info, StreamingContext context) {
    if (info != null) {
      info.AddValue("errorCategory", getErrorCategory());
      info.AddValue("errorCode", getErrorCode());
      super.GetObjectData(info, context);
    }
  }*/

  ///#endregion Serialization Support

  /**
   * Initializes a new instance with a specified error message and a reference to the inner
   * exception that is the cause of this exception.
   *
   * @param category Category of error.
   * @param code Error code.
   * @param message A message that describes the error
   * @param inner The exception that is the cause of the current exception
   */
  public ShardManagementException(ShardManagementErrorCategory category,
      ShardManagementErrorCode code, String message, RuntimeException inner) {
    super(message, inner);
    this.setErrorCategory(category);
    this.setErrorCode(code);
  }

  /**
   * Initializes a new instance with a specified formatted error message and a reference to the
   * inner exception that is the cause of this exception.
   *
   * @param category Category of error.
   * @param code Error code.
   * @param format The format message that describes the error
   * @param inner The exception that is the cause of the current exception
   * @param args The arguments to the format string
   */
  public ShardManagementException(ShardManagementErrorCategory category,
      ShardManagementErrorCode code, String format, RuntimeException inner, Object... args) {
    super(String.format(Locale.getDefault(), format, args), inner);
    this.setErrorCategory(category);
    this.setErrorCode(code);
  }

  public ShardManagementErrorCategory getErrorCategory() {
    return errorCategory;
  }

  private void setErrorCategory(ShardManagementErrorCategory value) {
    errorCategory = value;
  }

  public ShardManagementErrorCode getErrorCode() {
    return errorCode;
  }

  private void setErrorCode(ShardManagementErrorCode value) {
    errorCode = value;
  }
}