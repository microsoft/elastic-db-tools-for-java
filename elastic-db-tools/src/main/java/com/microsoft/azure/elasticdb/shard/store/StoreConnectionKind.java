package com.microsoft.azure.elasticdb.shard.store;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Types of store connections.
 */
public enum StoreConnectionKind {
    /**
     * Connection to GSM.
     */
    Global,

    /**
     * Connection to LSM Source Shard.
     */
    LocalSource,

    /**
     * Connection to LSM Target Shard (useful for Update Location operation only).
     */
    LocalTarget;

}
