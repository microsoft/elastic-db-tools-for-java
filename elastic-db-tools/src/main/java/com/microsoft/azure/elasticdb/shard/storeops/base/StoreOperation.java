package com.microsoft.azure.elasticdb.shard.storeops.base;

import java.util.UUID;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlConnectionStringBuilder;
import com.microsoft.azure.elasticdb.shard.store.IStoreConnection;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreConnectionKind;
import com.microsoft.azure.elasticdb.shard.store.StoreException;
import com.microsoft.azure.elasticdb.shard.store.StoreResult;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreTransactionScopeKind;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;

/**
 * Represents a SQL store operation.
 */
public abstract class StoreOperation implements IStoreOperation, AutoCloseable {

    /**
     * GSM connection.
     */
    private IStoreConnection globalConnection;

    /**
     * Source LSM connection.
     */
    private IStoreConnection localConnectionSource;

    /**
     * Target LSM connection.
     */
    private IStoreConnection localConnectionTarget;

    /**
     * State of the operation.
     */
    private StoreOperationState operationState;

    /**
     * Maximum state reached during Do operation.
     */
    private StoreOperationState maxDoState;
    /**
     * ShardMapManager object.
     */
    private ShardMapManager shardMapManager;
    /**
     * Operation Id.
     */
    private UUID id;
    /**
     * Operation code.
     */
    private StoreOperationCode operationCode;
    /**
     * Earliest point to start Undo operation.
     */
    private StoreOperationState undoStartState;
    /**
     * Original shard version on source.
     */
    private UUID originalShardVersionRemoves;
    /**
     * Original shard version on target.
     */
    private UUID originalShardVersionAdds;

    /**
     * Constructs an instance of StoreOperation.
     *
     * @param shardMapManager
     *            ShardMapManager object.
     * @param operationId
     *            Operation id.
     * @param undoStartState
     *            State from which Undo operation starts.
     * @param opCode
     *            Operation code.
     * @param originalShardVersionRemoves
     *            Original shard version for removes.
     * @param originalShardVersionAdds
     *            Original shard version for adds.
     */
    protected StoreOperation(ShardMapManager shardMapManager,
            UUID operationId,
            StoreOperationState undoStartState,
            StoreOperationCode opCode,
            UUID originalShardVersionRemoves,
            UUID originalShardVersionAdds) {
        this.setId(operationId);
        this.setOperationCode(opCode);
        this.setShardMapManager(shardMapManager);
        this.setUndoStartState(undoStartState);
        operationState = StoreOperationState.DoBegin;
        maxDoState = StoreOperationState.DoBegin;
        this.setOriginalShardVersionRemoves(originalShardVersionRemoves);
        this.setOriginalShardVersionAdds(originalShardVersionAdds);
    }

    /**
     * Given a state of the Do operation progress, gets the corresponding starting point for Undo operations.
     *
     * @param doState
     *            State at which Do operation was executing.
     * @return Corresponding state for Undo operation.
     */
    private static StoreOperationState undoStateForDoState(StoreOperationState doState) {
        switch (doState) {
            case DoGlobalConnect:
            case DoLocalSourceConnect:
            case DoLocalTargetConnect:
            case DoGlobalPreLocalBeginTransaction:
            case DoGlobalPreLocalExecute:
                return StoreOperationState.UndoEnd;

            case DoGlobalPreLocalCommitTransaction:
            case DoLocalSourceBeginTransaction:
            case DoLocalSourceExecute:
                return StoreOperationState.UndoGlobalPostLocalBeginTransaction;

            case DoLocalSourceCommitTransaction:
            case DoLocalTargetBeginTransaction:
            case DoLocalTargetExecute:
                return StoreOperationState.UndoLocalSourceBeginTransaction;

            case DoLocalTargetCommitTransaction:
            case DoGlobalPostLocalBeginTransaction:
            case DoGlobalPostLocalExecute:
            case DoGlobalPostLocalCommitTransaction:
                return StoreOperationState.UndoLocalTargetBeginTransaction;

            case DoBegin:
            case DoEnd:
            default:
                // Debug.Fail("Unexpected Do states for corresponding Undo operation.");
                return StoreOperationState.UndoBegin;
        }
    }

    protected final ShardMapManager getShardMapManager() {
        return shardMapManager;
    }

    private void setShardMapManager(ShardMapManager value) {
        shardMapManager = value;
    }

    protected final UUID getId() {
        return id;
    }

    private void setId(UUID value) {
        id = value;
    }

    protected final StoreOperationCode getOperationCode() {
        return operationCode;
    }

    private void setOperationCode(StoreOperationCode value) {
        operationCode = value;
    }

    /**
     * Operation Name.
     */
    protected final String getOperationName() {
        return StoreOperationErrorHandler.operationNameFromStoreOperationCode(this.getOperationCode());
    }

    protected final StoreOperationState getUndoStartState() {
        return undoStartState;
    }

    private void setUndoStartState(StoreOperationState value) {
        undoStartState = value;
    }

    protected final UUID getOriginalShardVersionRemoves() {
        return originalShardVersionRemoves;
    }

    private void setOriginalShardVersionRemoves(UUID value) {
        originalShardVersionRemoves = value;
    }

    protected final UUID getOriginalShardVersionAdds() {
        return originalShardVersionAdds;
    }

    private void setOriginalShardVersionAdds(UUID value) {
        originalShardVersionAdds = value;
    }

    /**
     * Performs the store operation.
     *
     * @return Results of the operation.
     */
    public final StoreResults doOperation() {
        StoreResults result;

        try {
            do {
                result = this.shardMapManager.getRetryPolicy().executeAction(() -> {
                    StoreResults r;

                    try {
                        // Open connections & acquire the necessary app locks.
                        this.establishConnnections(false);

                        // Execute & commit the Global pre-Local operations.
                        r = this.doGlobalPreLocal();

                        // If pending operation, we need to release the locks.
                        if (r.getStoreOperations().isEmpty()) {
                            // Execute & commit the Local operations on source.
                            this.doLocalSource();

                            // Execute & commit the Local operations on target.
                            this.doLocalTarget();

                            // Execute & commit the Global post-Local operations.
                            r = this.doGlobalPostLocal();

                            assert r != null;

                            operationState = StoreOperationState.DoEnd;
                        }
                    }
                    finally {
                        // Figure out the maximum of the progress made yet during Do operation.
                        if (maxDoState.getValue() < operationState.getValue()) {
                            maxDoState = operationState;
                        }

                        // close connections & release the necessary app locks.
                        this.teardownConnections();
                    }

                    return r;
                });

                // If pending operation, deserialize the pending operation and perform Undo.
                if (!result.getStoreOperations().isEmpty()) {
                    assert result.getStoreOperations().size() == 1;

                    try (IStoreOperation op = this.shardMapManager.getStoreOperationFactory().fromLogEntry(this.shardMapManager,
                            result.getStoreOperations().get(0))) {
                        op.undoOperation();
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        throw new StoreException(e.getMessage(), e.getCause() != null ? (Exception) e.getCause() : e);
                    }
                }
            }
            while (!result.getStoreOperations().isEmpty());
        }
        catch (StoreException se) {
            // If store exception was thrown, we will attempt to undo the current operation.
            this.attemptUndo();

            throw this.onStoreException(se, operationState);
        }
        catch (ShardManagementException e) {
            // If shard map manager exception was thrown, we will attempt to undo the operation.
            this.attemptUndo();

            throw e;
        }
        catch (Exception e) {
            throw new StoreException(e.getMessage(), e);
        }

        return result;
    }

    /**
     * Performs the undo store operation.
     */
    public final void undoOperation() {
        try {
            this.shardMapManager.getRetryPolicy().executeAction(() -> {
                try {
                    // Open connections & acquire the necessary app locks.
                    this.establishConnnections(true);

                    if (this.undoGlobalPreLocal()) {
                        if (this.getUndoStartState().getValue() <= StoreOperationState.UndoLocalTargetBeginTransaction.getValue()) {
                            // Execute & commit the Local operations on target.
                            this.undoLocalTarget();
                        }

                        if (this.getUndoStartState().getValue() <= StoreOperationState.UndoLocalSourceBeginTransaction.getValue()) {
                            // Execute & commit the Local undo operations on source.
                            this.undoLocalSource();
                        }

                        if (this.getUndoStartState().getValue() <= StoreOperationState.UndoGlobalPostLocalBeginTransaction.getValue()) {
                            // Execute & commit the Global post-Local operations.
                            this.undoGlobalPostLocal();
                        }
                    }

                    operationState = StoreOperationState.UndoEnd;
                }
                finally {
                    // close connections & release the necessary app locks.
                    this.teardownConnections();
                }
            });
        }
        catch (StoreException se) {
            throw this.onStoreException(se, operationState);
        }
        catch (Exception e) {
            throw new StoreException(e.getMessage(), e);
        }
    }

    /**
     * Performs actual Dispose of resources.
     */
    public void close() {
        if (localConnectionTarget != null) {
            localConnectionTarget.close();
            localConnectionTarget = null;
        }

        if (localConnectionSource != null) {
            localConnectionSource.close();
            localConnectionSource = null;
        }

        if (globalConnection != null) {
            globalConnection.close();
            globalConnection = null;
        }
    }

    /**
     * Requests the derived class to provide information regarding the connections needed for the operation.
     *
     * @return Information about shards involved in the operation.
     */
    public abstract StoreConnectionInfo getStoreConnectionInfo();

    /**
     * Performs the initial GSM operation prior to LSM operations.
     *
     * @param ts
     *            Transaction scope.
     * @return Pending operations on the target objects if any.
     */
    public abstract StoreResults doGlobalPreLocalExecute(IStoreTransactionScope ts);

    /**
     * Handles errors from the initial GSM operation prior to LSM operations.
     *
     * @param result
     *            Operation result.
     */
    public abstract void handleDoGlobalPreLocalExecuteError(StoreResults result);

    /**
     * Performs the LSM operation on the source shard.
     *
     * @param ts
     *            Transaction scope.
     * @return Result of the operation.
     */
    public abstract StoreResults doLocalSourceExecute(IStoreTransactionScope ts);

    /**
     * Handles errors from the the LSM operation on the source shard.
     *
     * @param result
     *            Operation result.
     */
    public abstract void handleDoLocalSourceExecuteError(StoreResults result);

    /**
     * Performs the LSM operation on the target shard.
     *
     * @param ts
     *            Transaction scope.
     * @return Result of the operation.
     */
    public StoreResults doLocalTargetExecute(IStoreTransactionScope ts) {
        return new StoreResults();
    }

    /**
     * Performs the final GSM operation after the LSM operations.
     *
     * @param result
     *            Operation result.
     */
    public void handleDoLocalTargetExecuteError(StoreResults result) {
        assert result.getResult() == StoreResult.Success;
    }

    /**
     * Performs the final GSM operation after the LSM operations.
     *
     * @param ts
     *            Transaction scope.
     * @return Pending operations on the target objects if any.
     */
    public abstract StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts);

    /**
     * Handles errors from the final GSM operation after the LSM operations.
     *
     * @param result
     *            Operation result.
     */
    public abstract void handleDoGlobalPostLocalExecuteError(StoreResults result);

    /**
     * Refreshes the cache on successful commit of the final GSM operation after the LSM operations.
     *
     * @param result
     *            Operation result.
     */
    public void doGlobalPostLocalUpdateCache(StoreResults result) {
    }

    /**
     * Performs undo of LSM operation on the target shard.
     *
     * @param ts
     *            Transaction scope.
     * @return Result of the operation.
     */
    public StoreResults undoLocalTargetExecute(IStoreTransactionScope ts) {
        return new StoreResults();
    }

    /**
     * Performs undo of LSM operation on the target shard.
     *
     * @param result
     *            Operation result.
     */
    public void handleUndoLocalTargetExecuteError(StoreResults result) {
        assert result.getResult() == StoreResult.Success;
    }

    /**
     * Performs the undo of GSM operation prior to LSM operations.
     *
     * @param ts
     *            Transaction scope.
     * @return Result of the operation.
     */
    public StoreResults undoGlobalPreLocalExecute(IStoreTransactionScope ts) {
        return ts.executeOperation(StoreOperationRequestBuilder.SP_FIND_AND_UPDATE_OPERATION_LOG_ENTRY_BY_ID_GLOBAL,
                StoreOperationRequestBuilder.findAndUpdateOperationLogEntryByIdGlobal(this.getId(), this.getUndoStartState()));
    }

    /**
     * Handles errors from the undo of GSM operation prior to LSM operations.
     *
     * @param result
     *            Operation result.
     */
    public void handleUndoGlobalPreLocalExecuteError(StoreResults result) {
        // Possible errors are:
        // StoreResult.StoreVersionMismatch
        // StoreResult.MissingParametersForStoredProcedure
        throw StoreOperationErrorHandler.onCommonErrorGlobal(result,
                StoreOperationErrorHandler.operationNameFromStoreOperationCode(this.getOperationCode()),
                StoreOperationRequestBuilder.SP_FIND_AND_UPDATE_OPERATION_LOG_ENTRY_BY_ID_GLOBAL);
    }

    /**
     * Performs the undo of LSM operation on the source shard.
     *
     * @param ts
     *            Transaction scope.
     * @return Result of the operation.
     */
    public abstract StoreResults undoLocalSourceExecute(IStoreTransactionScope ts);

    /**
     * Handles errors from the undo of LSM operation on the source shard.
     *
     * @param result
     *            Operation result.
     */
    public abstract void handleUndoLocalSourceExecuteError(StoreResults result);

    /**
     * Performs the undo of GSM operation after LSM operations.
     *
     * @param ts
     *            Transaction scope.
     * @return Pending operations on the target objects if any.
     */
    public abstract StoreResults undoGlobalPostLocalExecute(IStoreTransactionScope ts);

    /**
     * Handles errors from the undo of GSM operation after LSM operations.
     *
     * @param result
     *            Operation result.
     */
    public abstract void handleUndoGlobalPostLocalExecuteError(StoreResults result);

    /**
     * Returns the ShardManagementException to be thrown corresponding to a StoreException.
     *
     * @param se
     *            Store Exception that has been raised.
     * @param state
     *            SQL operation state.
     * @return ShardManagementException to be thrown.
     */
    public ShardManagementException onStoreException(StoreException se,
            StoreOperationState state) {
        switch (state) {
            case DoGlobalConnect:

            case DoGlobalPreLocalBeginTransaction:
            case DoGlobalPreLocalExecute:
            case DoGlobalPreLocalCommitTransaction:

            case DoGlobalPostLocalBeginTransaction:
            case DoGlobalPostLocalExecute:
            case DoGlobalPostLocalCommitTransaction:

            case UndoGlobalConnect:

            case UndoGlobalPreLocalBeginTransaction:
            case UndoGlobalPreLocalExecute:
            case UndoGlobalPreLocalCommitTransaction:

            case UndoGlobalPostLocalBeginTransaction:
            case UndoGlobalPostLocalExecute:
            case UndoGlobalPostLocalCommitTransaction:
                return ExceptionUtils.getStoreExceptionGlobal(this.getErrorCategory(), se, this.getOperationName());

            case DoLocalSourceConnect:
            case DoLocalSourceBeginTransaction:
            case DoLocalSourceExecute:
            case DoLocalSourceCommitTransaction:

            case UndoLocalSourceConnect:
            case UndoLocalSourceBeginTransaction:
            case UndoLocalSourceExecute:
            case UndoLocalSourceCommitTransaction:
                return ExceptionUtils.getStoreExceptionLocal(this.getErrorCategory(), se, this.getOperationName(), this.getErrorSourceLocation());

            case DoLocalTargetConnect:
            case DoLocalTargetBeginTransaction:
            case DoLocalTargetExecute:
            case DoLocalTargetCommitTransaction:

            case UndoLocalTargetConnect:
            case UndoLocalTargetBeginTransaction:
            case UndoLocalTargetExecute:
            case UndoLocalTargetCommitTransaction:

            default:
                return ExceptionUtils.getStoreExceptionLocal(this.getErrorCategory(), se, this.getOperationName(), this.getErrorTargetLocation());
        }
    }

    /**
     * Source location of error.
     */
    protected abstract ShardLocation getErrorSourceLocation();

    /**
     * Target location of error.
     */
    protected abstract ShardLocation getErrorTargetLocation();

    /**
     * Error category for error.
     */
    protected abstract ShardManagementErrorCategory getErrorCategory();

    /**
     * Obtains the connection string for an LSM location.
     *
     * @return Connection string for LSM given its location.
     */
    protected final String getConnectionStringForShardLocation(ShardLocation location) {
        SqlConnectionStringBuilder tempVar = new SqlConnectionStringBuilder(this.shardMapManager.getCredentials().getConnectionStringShard());
        tempVar.setDataSource(location.getDataSource());
        tempVar.setDatabaseName(location.getDatabase());
        return tempVar.getConnectionString();
    }

    /**
     * Attempts to Undo the current operation which actually had caused an exception. This is basically a best effort attempt.
     */
    private void attemptUndo() {
        // Identify the point from which we shall start the undo operation.
        this.setUndoStartState(undoStateForDoState(maxDoState));

        // If there is something to Undo.
        if (this.getUndoStartState().getValue() < StoreOperationState.UndoEnd.getValue()) {
            try {
                this.undoOperation();
            }
            catch (StoreException | ShardManagementException e) {
                // Do nothing, since we are raising the original Do operation exception.
            }
        }
    }

    /**
     * Established connections to the target databases.
     *
     * @param undo
     *            Is this undo operation.
     */
    private void establishConnnections(boolean undo) {
        operationState = undo ? StoreOperationState.UndoGlobalConnect : StoreOperationState.DoGlobalConnect;

        // Find the necessary information for connections.
        StoreConnectionInfo sci = this.getStoreConnectionInfo();

        assert sci != null;

        // Open global & local connections & acquire application level locks for the corresponding scope.
        globalConnection = this.shardMapManager.getStoreConnectionFactory().getConnection(StoreConnectionKind.Global,
                this.shardMapManager.getCredentials().getConnectionStringShardMapManager());

        globalConnection.openWithLock(this.getId());

        if (sci.getSourceLocation() != null) {
            operationState = undo ? StoreOperationState.UndoLocalSourceConnect : StoreOperationState.DoLocalSourceConnect;

            localConnectionSource = this.shardMapManager.getStoreConnectionFactory().getConnection(StoreConnectionKind.LocalSource,
                    this.getConnectionStringForShardLocation(sci.getSourceLocation()));

            localConnectionSource.openWithLock(this.getId());
        }

        if (sci.getTargetLocation() != null) {
            assert sci.getSourceLocation() != null;

            operationState = undo ? StoreOperationState.UndoLocalTargetConnect : StoreOperationState.DoLocalTargetConnect;

            localConnectionTarget = this.shardMapManager.getStoreConnectionFactory().getConnection(StoreConnectionKind.LocalTarget,
                    this.getConnectionStringForShardLocation(sci.getTargetLocation()));

            localConnectionTarget.openWithLock(this.getId());
        }
    }

    /**
     * Acquires the transaction scope.
     *
     * @return Transaction scope, operations within the scope excute atomically.
     */
    private IStoreTransactionScope getTransactionScope(StoreOperationTransactionScopeKind scopeKind) {
        switch (scopeKind) {
            case Global:
                return globalConnection.getTransactionScope(StoreTransactionScopeKind.ReadWrite);

            case LocalSource:
                return localConnectionSource.getTransactionScope(StoreTransactionScopeKind.ReadWrite);

            default:
                assert scopeKind == StoreOperationTransactionScopeKind.LocalTarget;
                return localConnectionTarget.getTransactionScope(StoreTransactionScopeKind.ReadWrite);
        }
    }

    /**
     * Performs the initial GSM operation prior to LSM operations.
     *
     * @return Pending operations on the target objects if any.
     */
    private StoreResults doGlobalPreLocal() {
        StoreResults result;

        operationState = StoreOperationState.DoGlobalPreLocalBeginTransaction;

        try (IStoreTransactionScope ts = this.getTransactionScope(StoreOperationTransactionScopeKind.Global)) {
            operationState = StoreOperationState.DoGlobalPreLocalExecute;

            result = this.doGlobalPreLocalExecute(ts);

            if (result.getResult() == StoreResult.Success) {
                ts.setSuccess(true);
                operationState = StoreOperationState.DoGlobalPreLocalCommitTransaction;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new StoreException(e.getMessage(), e.getCause() != null ? (Exception) e.getCause() : e);
        }

        if (result.getResult() != StoreResult.Success && result.getResult() != StoreResult.ShardPendingOperation) {
            this.handleDoGlobalPreLocalExecuteError(result);
        }

        return result;
    }

    /**
     * Performs the LSM operation on the source shard.
     */
    private void doLocalSource() {
        StoreResults result;

        operationState = StoreOperationState.DoLocalSourceBeginTransaction;

        try (IStoreTransactionScope ts = this.getTransactionScope(StoreOperationTransactionScopeKind.LocalSource)) {
            operationState = StoreOperationState.DoLocalSourceExecute;

            result = this.doLocalSourceExecute(ts);

            if (result.getResult() == StoreResult.Success) {
                ts.setSuccess(true);
                operationState = StoreOperationState.DoLocalSourceCommitTransaction;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new StoreException(e.getMessage(), e.getCause() != null ? (Exception) e.getCause() : e);
        }

        if (result.getResult() != StoreResult.Success) {
            this.handleDoLocalSourceExecuteError(result);
        }
    }

    /**
     * Performs the LSM operation on the target shard.
     */
    private void doLocalTarget() {
        if (localConnectionTarget != null) {
            StoreResults result;

            operationState = StoreOperationState.DoLocalTargetBeginTransaction;

            try (IStoreTransactionScope ts = this.getTransactionScope(StoreOperationTransactionScopeKind.LocalTarget)) {
                operationState = StoreOperationState.DoLocalTargetExecute;

                result = this.doLocalTargetExecute(ts);

                if (result.getResult() == StoreResult.Success) {
                    ts.setSuccess(true);
                    operationState = StoreOperationState.DoLocalTargetCommitTransaction;
                }
            }
            catch (Exception e) {
                e.printStackTrace();
                throw new StoreException(e.getMessage(), e.getCause() != null ? (Exception) e.getCause() : e);
            }

            if (result.getResult() != StoreResult.Success) {
                this.handleDoLocalTargetExecuteError(result);
            }
        }
    }

    /**
     * Performs the final GSM operation after the LSM operations.
     *
     * @return Results of the GSM operation.
     */
    private StoreResults doGlobalPostLocal() {
        StoreResults result;

        operationState = StoreOperationState.DoGlobalPostLocalBeginTransaction;

        try (IStoreTransactionScope ts = this.getTransactionScope(StoreOperationTransactionScopeKind.Global)) {
            operationState = StoreOperationState.DoGlobalPostLocalExecute;

            result = this.doGlobalPostLocalExecute(ts);

            if (result.getResult() == StoreResult.Success) {
                ts.setSuccess(true);
                operationState = StoreOperationState.DoGlobalPostLocalCommitTransaction;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new StoreException(e.getMessage(), e.getCause() != null ? (Exception) e.getCause() : e);
        }

        if (result.getResult() != StoreResult.Success) {
            this.handleDoGlobalPostLocalExecuteError(result);
        }
        else {
            this.doGlobalPostLocalUpdateCache(result);
        }

        return result;
    }

    /**
     * Perform undo of GSM operations before LSM operations. Basically checks if the operation to be undone is still present in the log.
     *
     * @return <c>true</c> if further undo operations are necessary, <c>false</c> otherwise.
     */
    private boolean undoGlobalPreLocal() {
        StoreResults result;

        operationState = StoreOperationState.UndoGlobalPreLocalBeginTransaction;

        try (IStoreTransactionScope ts = this.getTransactionScope(StoreOperationTransactionScopeKind.Global)) {
            operationState = StoreOperationState.UndoGlobalPreLocalExecute;

            result = this.undoGlobalPreLocalExecute(ts);

            if (result.getResult() == StoreResult.Success) {
                ts.setSuccess(true);

                if (!result.getStoreOperations().isEmpty()) {
                    this.setOriginalShardVersionRemoves(result.getStoreOperations().get(0).getOriginalShardVersionRemoves());
                    this.setOriginalShardVersionAdds(result.getStoreOperations().get(0).getOriginalShardVersionAdds());
                    operationState = StoreOperationState.UndoGlobalPreLocalCommitTransaction;
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new StoreException(e.getMessage(), e.getCause() != null ? (Exception) e.getCause() : e);
        }

        if (result.getResult() != StoreResult.Success) {
            this.handleUndoGlobalPreLocalExecuteError(result);
        }

        return !result.getStoreOperations().isEmpty();
    }

    /**
     * Performs the undo of LSM operation on the target shard.
     */
    private void undoLocalTarget() {
        if (localConnectionTarget != null) {
            StoreResults result;

            operationState = StoreOperationState.UndoLocalTargetBeginTransaction;

            try (IStoreTransactionScope ts = this.getTransactionScope(StoreOperationTransactionScopeKind.LocalTarget)) {
                operationState = StoreOperationState.UndoLocalTargetExecute;

                result = this.undoLocalTargetExecute(ts);

                if (result.getResult() == StoreResult.Success) {
                    ts.setSuccess(true);
                    operationState = StoreOperationState.UndoLocalTargetCommitTransaction;
                }
            }
            catch (Exception e) {
                e.printStackTrace();
                throw new StoreException(e.getMessage(), e.getCause() != null ? (Exception) e.getCause() : e);
            }

            if (result.getResult() != StoreResult.Success) {
                this.handleUndoLocalTargetExecuteError(result);
            }
        }
    }

    /**
     * Performs the undo of LSM operation on the source shard.
     */
    private void undoLocalSource() {
        StoreResults result;

        operationState = StoreOperationState.UndoLocalSourceBeginTransaction;

        try (IStoreTransactionScope ts = this.getTransactionScope(StoreOperationTransactionScopeKind.LocalSource)) {
            operationState = StoreOperationState.UndoLocalSourceExecute;

            result = this.undoLocalSourceExecute(ts);

            if (result.getResult() == StoreResult.Success) {
                ts.setSuccess(true);
                operationState = StoreOperationState.UndoLocalSourceCommitTransaction;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new StoreException(e.getMessage(), e.getCause() != null ? (Exception) e.getCause() : e);
        }

        if (result.getResult() != StoreResult.Success) {
            this.handleUndoLocalSourceExecuteError(result);
        }
    }

    /**
     * Performs the undo of GSM operation after LSM operations.
     */
    private void undoGlobalPostLocal() {
        StoreResults result;

        operationState = StoreOperationState.UndoGlobalPostLocalBeginTransaction;

        try (IStoreTransactionScope ts = this.getTransactionScope(StoreOperationTransactionScopeKind.Global)) {
            operationState = StoreOperationState.UndoGlobalPostLocalExecute;

            result = this.undoGlobalPostLocalExecute(ts);

            if (result.getResult() == StoreResult.Success) {
                ts.setSuccess(true);
                operationState = StoreOperationState.UndoGlobalPostLocalCommitTransaction;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new StoreException(e.getMessage(), e.getCause() != null ? (Exception) e.getCause() : e);
        }

        if (result.getResult() != StoreResult.Success) {
            this.handleUndoGlobalPostLocalExecuteError(result);
        }
    }

    /**
     * Terminates connections to target databases.
     */
    private void teardownConnections() {
        if (localConnectionTarget != null) {
            localConnectionTarget.closeWithUnlock(this.getId());
        }

        if (localConnectionSource != null) {
            localConnectionSource.closeWithUnlock(this.getId());
        }

        if (globalConnection != null) {
            globalConnection.closeWithUnlock(this.getId());
        }
    }
}
