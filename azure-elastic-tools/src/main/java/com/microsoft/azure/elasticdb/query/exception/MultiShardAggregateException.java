package com.microsoft.azure.elasticdb.query.exception;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Represents one or more <see cref="Exception"/> errors that occurred when executing a query across a shard set. The InnerExceptions field collects
 * these exceptions and one can iterate through the InnerExceptions for further inspection or processing. Purpose: Public type that communicates
 * errors that occurred across multiple shards
 */
public class MultiShardAggregateException extends Exception implements Serializable {

    private List<Exception> innerExceptions;

    /**
     * Initializes a new instance of the <see cref="MultiShardAggregateException"/> class.
     */
    public MultiShardAggregateException() {
        this("One or more errors occurred across shards");
    }

    /**
     * Initializes a new instance of the <see cref="MultiShardAggregateException"/> class.
     *
     * @param message
     *            The error message that explains the reason for the exception
     */
    public MultiShardAggregateException(String message) {
        super(message);
        innerExceptions = new ArrayList<>();
        innerExceptions.add(new Exception(message));
    }

    /**
     * Initializes a new instance of the <see cref="MultiShardAggregateException"/> class.
     *
     * @param innerException
     *            The <see cref="Exception"/> that caused the current exception
     */
    public MultiShardAggregateException(Exception innerException) {
        this(Collections.singletonList(innerException));
    }

    /**
     * Initializes a new instance of the <see cref="MultiShardAggregateException"/> class.
     *
     * @param message
     *            The error message that explains the reason for the exception
     * @param innerException
     *            A list of <see cref="Exception"/> that caused the current exception
     */
    public MultiShardAggregateException(String message,
            Exception innerException) {
        super(message);
        if (innerExceptions == null) {
            innerExceptions = new ArrayList<>();
        }
        innerExceptions.add(innerException);
    }

    /**
     * Initializes a new instance of the <see cref="MultiShardAggregateException"/> class.
     *
     * @param innerExceptions
     *            A list of <see cref="Exception"/> that caused the current exception
     */
    public MultiShardAggregateException(List<Exception> innerExceptions) {
        this("One or more errors occurred across shards", innerExceptions);
    }

    /**
     * Initializes a new instance of the <see cref="MultiShardAggregateException"/> class.
     *
     * @param message
     *            The error message that explains the reason for the exception
     * @param innerExceptions
     *            A list of <see cref="Exception"/> that caused the current exception
     * @throws IllegalArgumentException
     *             The <paramref name="innerExceptions"/> is null
     */
    public MultiShardAggregateException(String message,
            List<Exception> innerExceptions) {
        super(message, innerExceptions != null ? innerExceptions.get(0) : null);
        if (null == innerExceptions) {
            throw new IllegalArgumentException("innerExceptions");
        }

        if (this.innerExceptions == null) {
            this.innerExceptions = new ArrayList<>();
        }

        this.innerExceptions.addAll(innerExceptions);
    }

    /**
     * Gets a read-only collection of the <see cref="Exception"/> instances that caused the current exception.
     */
    public final List<Exception> getInnerExceptions() {
        // Put them in a readonly collection
        return Collections.unmodifiableList(innerExceptions);
    }

    /**
     * Provides a string representation of this exception including its inner exceptions.
     */
    @Override
    public String toString() {
        String newLine = System.lineSeparator();
        String text = super.toString();

        for (int i = 0; i < innerExceptions.size(); i++) {
            text = String.format(Locale.getDefault(), "%1$s%2$s---> (Inner Exception #%3$s) %4$s%5$s%6$s", text, newLine, i,
                    innerExceptions.get(i).toString(), "<---", newLine);
        }

        return text;
    }
}