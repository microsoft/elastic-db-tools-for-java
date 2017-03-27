package com.microsoft.azure.elasticdb.shard.store;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import javafx.concurrent.Task;

import java.util.*;

/**
 * Allows scoping of a transactional operation on the store.
 */
public interface IStoreTransactionScope extends java.io.Closeable {
    /**
     * Type of transaction scope.
     */
    StoreTransactionScopeKind getKind();

    /**
     * When set to true, implies that the transaction is ready for commit.
     */
    boolean getSuccess();

    void setSuccess(boolean value);

    /**
     * Executes the given operation using the <paramref name="operationData"/> values
     * as input to the operation.
     *
     * @param operationName Operation to execute.
     * @param operationData Input data for operation.
     * @return Storage results object.
     */
    IStoreResults ExecuteOperation(String operationName, Object operationData);

    /**
     * Asynchronously executes the given operation using the <paramref name="operationData"/> values
     * as input to the operation.
     *
     * @param operationName Operation to execute.
     * @param operationData Input data for operation.
     * @return Task encapsulating storage results object.
     */
    Task<IStoreResults> ExecuteOperationAsync(String operationName, Object operationData);

    /**
     * Executes the given command.
     *
     * @param command Command to execute.
     * @return Storage results object.
     */
    IStoreResults ExecuteCommandSingle(StringBuilder command);

    /**
     * Executes the given set of commands.
     *
     * @param commands Collection of commands to execute.
     */
    void ExecuteCommandBatch(List<StringBuilder> commands);
}