package com.microsoft.azure.elasticdb.shard.schema;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import java.io.Serializable;
import java.util.Locale;

/**
 * The exception that is thrown when an error occurs during operations related to schema info collection.
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
     * @param message
     *            Error message.
     */
    public SchemaInfoException(String message) {
        super(message);
    }

    /**
     * Initializes a new instance with a specified formatted error message.
     *
     * @param format
     *            The format message that describes the error.
     * @param args
     *            The arguments to the format string.
     */
    public SchemaInfoException(String format,
            Object... args) {
        super(String.format(Locale.getDefault(), format, args));
    }

    /**
     * Initializes a new instance with a specified error message and a reference to the inner exception that caused this exception.
     *
     * @param message
     *            A message that describes the error.
     * @param inner
     *            The exception that is the cause of the current exception.
     */
    public SchemaInfoException(String message,
            RuntimeException inner) {
        super(message, inner);
    }

    /**
     * Initializes a new instance with a specified formatted error message.
     *
     * @param code
     *            Error code.
     * @param format
     *            The format message that describes the error
     * @param args
     *            The arguments to the format string
     */
    public SchemaInfoException(SchemaInfoErrorCode code,
            String format,
            Object... args) {
        super(String.format(Locale.getDefault(), format, args));
        this.setErrorCode(code);
    }

    /**
     * Initializes a new instance with a specified error message and a reference to the inner exception that is the cause of this exception.
     *
     * @param code
     *            Error code.
     * @param message
     *            A message that describes the error
     * @param inner
     *            The exception that is the cause of the current exception
     */
    public SchemaInfoException(SchemaInfoErrorCode code,
            String message,
            RuntimeException inner) {
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