package com.microsoft.azure.elasticdb.core.commons.logging;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import java.util.UUID;

/**
 * Utility class to set and restore the CorrelationManager ActivityId via the using pattern.
 */
public final class ActivityIdScope implements AutoCloseable {

    /**
     * The previous activity id that was in scope.
     */
    private UUID previousActivityId;

    /**
     * Creates an instance of the <see cref="ActivityIdScope"/> class.
     */
    public ActivityIdScope(UUID activityId) {
        // TODO
    }

    /**
     * Restores the previous activity id when this instance is disposed.
     */
    public void close() {
        // TODO
    }
}