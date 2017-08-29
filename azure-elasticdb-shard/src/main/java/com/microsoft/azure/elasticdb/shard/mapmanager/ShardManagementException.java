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