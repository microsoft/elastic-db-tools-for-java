package com.microsoft.azure.elasticdb.shard.storeops.base;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Types of transaction scopes used during store operations.
 */
public enum StoreOperationTransactionScopeKind {
    /**
     * Scope of GSM.
     */
    Global,

    /**
     * Scope of source LSM.
     */
    LocalSource,

    /**
     * Scope of target LSM.
     */
    LocalTarget;

}
