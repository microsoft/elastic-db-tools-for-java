package com.microsoft.azure.elasticdb.shard.store;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import java.io.Serializable;

/**
 * Representation of exceptions that occur during storage operations.
 */
public final class StoreException extends RuntimeException implements Serializable {

    /**
     * Initializes a new instance with a specified error message.
     */
    public StoreException() {
        this("StoreException occurred");
    }

    /**
     * Initializes a new instance with a specified error message.
     *
     * @param message
     *            Error message.
     */
    public StoreException(String message) {
        super(message);
    }

    /**
     * Initializes a new instance with a specified error message and a reference to the inner exception that is the cause of this exception.
     *
     * @param message
     *            A message that describes the error
     * @param inner
     *            The exception that is the cause of the current exception
     */
    public StoreException(String message,
            RuntimeException inner) {
        super(message, inner);
    }

    public StoreException(String message,
            Exception inner) {
        super(message, inner);
    }
}
