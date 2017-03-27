package com.microsoft.azure.elasticdb.shard.mapmanager;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

/**
 * Representation of exceptions that occur during storage operations.
 */
public final class ShardManagementException extends RuntimeException {
    /**
     * Error category.
     */
    private ShardManagementErrorCategory ErrorCategory;
    /**
     * Error code.
     */
    private ShardManagementErrorCode ErrorCode;

    /**
     * Initializes a new instance with a specified error message.
     *
     * @param category Category of error.
     * @param code     Error code.
     * @param message  Error message.
     */
    public ShardManagementException(ShardManagementErrorCategory category, ShardManagementErrorCode code, String message) {
        super(message);
        this.setErrorCategory(category);
        this.setErrorCode(code);
    }

    /**
     * Initializes a new instance with a specified error message and a reference to the inner exception
     * that is the cause of this exception.
     *
     * @param category Category of error.
     * @param code     Error code.
     * @param message  A message that describes the error
     * @param inner    The exception that is the cause of the current exception
     */
    public ShardManagementException(ShardManagementErrorCategory category, ShardManagementErrorCode code, String message, RuntimeException inner) {
        super(message, inner);
        this.setErrorCategory(category);
        this.setErrorCode(code);
    }

    public ShardManagementErrorCategory getErrorCategory() {
        return ErrorCategory;
    }

    private void setErrorCategory(ShardManagementErrorCategory value) {
        ErrorCategory = value;
    }

    public ShardManagementErrorCode getErrorCode() {
        return ErrorCode;
    }

    private void setErrorCode(ShardManagementErrorCode value) {
        ErrorCode = value;
    }
}
