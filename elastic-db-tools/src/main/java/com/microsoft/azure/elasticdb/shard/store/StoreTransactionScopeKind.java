package com.microsoft.azure.elasticdb.shard.store;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

/**
 * Type of transaction scope.
 */
public enum StoreTransactionScopeKind {
    /**
     * A non-transactional scope, uses auto-commit transaction mode. Useful for performing operations that are not allowed to be executed within
     * transactions such as Kill connections.
     */
    NonTransactional,

    /**
     * Read only transaction scope, uses read-committed transaction mode. Read locks are acquired purely during row read and then released.
     */
    ReadOnly,

    /**
     * Read write transaction scope, uses repeatable-read transaction mode. Read locks are held till Commit or Rollback.
     */
    ReadWrite;

}
