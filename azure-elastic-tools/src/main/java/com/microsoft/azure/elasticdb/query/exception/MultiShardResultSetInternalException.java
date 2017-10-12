package com.microsoft.azure.elasticdb.query.exception;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import java.io.Serializable;

/**
 * Custom exception that is thrown when the <see cref="MultiShardResultSet"/> is in an invalid state. If you experience this exception repeatedly,
 * please contact Microsoft Customer Support. Purpose: Custom exception to throw when the MultiShardResultSet is in an invalid state. This error
 * should not make it out to the user.
 */
public class MultiShardResultSetInternalException extends RuntimeException implements Serializable {

    /**
     * Initializes a new instance of the MultiShardResultSetInternalException class.
     */
    public MultiShardResultSetInternalException() {
        super();
    }

    /**
     * Initializes a new instance of the MultiShardResultSetInternalException class with a specified error message.
     *
     * @param message
     *            The message that describes the error.
     */
    public MultiShardResultSetInternalException(String message) {
        super(message);
    }

    /**
     * Initializes a new instance of the MultiShardResultSetInternalException class with a message and an inner exception.
     *
     * @param message
     *            The message to encapsulate in the exception.
     * @param innerException
     *            The underlying exception that causes this exception.
     */
    public MultiShardResultSetInternalException(String message,
            RuntimeException innerException) {
        super(message, innerException);
    }
}