package com.microsoft.azure.elasticdb.shard.store;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import java.util.List;
import java.util.concurrent.Callable;

import javax.xml.bind.JAXBElement;

/**
 * Allows scoping of a transactional operation on the store.
 */
public interface IStoreTransactionScope extends AutoCloseable {

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
     * Executes the given operation using the <paramref name="operationData"/> values as input to the operation.
     *
     * @param operationName
     *            Operation to execute.
     * @param operationData
     *            Input data for operation.
     * @return Storage results object.
     */
    StoreResults executeOperation(String operationName,
            JAXBElement operationData);

    /**
     * Asynchronously executes the given operation using the <paramref name="operationData"/> values as input to the operation.
     *
     * @param operationName
     *            Operation to execute.
     * @param operationData
     *            Input data for operation.
     * @return Task encapsulating storage results object.
     */
    Callable<StoreResults> executeOperationAsync(String operationName,
            JAXBElement operationData);

    /**
     * Executes the given command.
     *
     * @param command
     *            Command to execute.
     * @return Storage results object.
     */
    StoreResults executeCommandSingle(StringBuilder command);

    /**
     * Executes the given set of commands.
     *
     * @param commands
     *            Collection of commands to execute.
     */
    void executeCommandBatch(List<StringBuilder> commands);
}