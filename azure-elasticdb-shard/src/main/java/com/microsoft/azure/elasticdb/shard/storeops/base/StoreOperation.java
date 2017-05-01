package com.microsoft.azure.elasticdb.shard.storeops.base;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

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
import java.util.UUID;

/**
 * Represents a SQL store operation.
 */
public abstract class StoreOperation implements IStoreOperation {

  /**
   * GSM connection.
   */
  private IStoreConnection _globalConnection;

  /**
   * Source LSM connection.
   */
  private IStoreConnection _localConnectionSource;

  /**
   * Target LSM connection.
   */
  private IStoreConnection _localConnectionTarget;


  /**
   * State of the operation.
   */
  private StoreOperationState _operationState;

  /**
   * Maximum state reached during Do operation.
   */
  private StoreOperationState _maxDoState;
  /**
   * ShardMapManager object.
   */
  private ShardMapManager shardMapManager;
  /**
   * Operation Id.
   */
  private UUID Id;
  /**
   * Operation code.
   */
  private StoreOperationCode OperationCode;
  /**
   * Earliest point to start Undo operation.
   */
  private StoreOperationState UndoStartState;
  /**
   * Original shard version on source.
   */
  private UUID OriginalShardVersionRemoves;
  /**
   * Original shard version on target.
   */
  private UUID OriginalShardVersionAdds;

  /**
   * Constructs an instance of StoreOperation.
   *
   * @param shardMapManager ShardMapManager object.
   * @param operationId Operation Id.
   * @param undoStartState State from which Undo operation starts.
   * @param opCode Operation code.
   * @param originalShardVersionRemoves Original shard version for removes.
   * @param originalShardVersionAdds Original shard version for adds.
   */
  protected StoreOperation(ShardMapManager shardMapManager, UUID operationId,
      StoreOperationState undoStartState, StoreOperationCode opCode,
      UUID originalShardVersionRemoves, UUID originalShardVersionAdds) {
    this.setId(operationId);
    this.setOperationCode(opCode);
    this.setShardMapManager(shardMapManager);
    this.setUndoStartState(undoStartState);
    _operationState = StoreOperationState.DoBegin;
    _maxDoState = StoreOperationState.DoBegin;
    this.setOriginalShardVersionRemoves(originalShardVersionRemoves);
    this.setOriginalShardVersionAdds(originalShardVersionAdds);
  }

  /**
   * Given a state of the Do operation progress, gets the corresponding starting point
   * for Undo operations.
   *
   * @param doState State at which Do operation was executing.
   * @return Corresponding state for Undo operation.
   */
  private static StoreOperationState UndoStateForDoState(StoreOperationState doState) {
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
        //Debug.Fail("Unexpected Do states for corresponding Undo operation.");
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
    return Id;
  }

  private void setId(UUID value) {
    Id = value;
  }

  protected final StoreOperationCode getOperationCode() {
    return OperationCode;
  }

  private void setOperationCode(StoreOperationCode value) {
    OperationCode = value;
  }

  /**
   * Operation Name.
   */
  protected final String getOperationName() {
    return StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode());
  }

  protected final StoreOperationState getUndoStartState() {
    return UndoStartState;
  }

  private void setUndoStartState(StoreOperationState value) {
    UndoStartState = value;
  }

  protected final UUID getOriginalShardVersionRemoves() {
    return OriginalShardVersionRemoves;
  }

  private void setOriginalShardVersionRemoves(UUID value) {
    OriginalShardVersionRemoves = value;
  }

  protected final UUID getOriginalShardVersionAdds() {
    return OriginalShardVersionAdds;
  }

  private void setOriginalShardVersionAdds(UUID value) {
    OriginalShardVersionAdds = value;
  }

  /**
   * Performs the store operation.
   *
   * @return Results of the operation.
   */
  public final StoreResults Do() {
    StoreResults result;

    try {
      do {
        result = this.getShardMapManager().getRetryPolicy().executeAction(() -> {
          StoreResults r;

          try {
            // Open connections & acquire the necessary app locks.
            this.EstablishConnnections(false);

            // Execute & commit the Global pre-Local operations.
            r = this.DoGlobalPreLocal();

            // If pending operation, we need to release the locks.
            if (r.getStoreOperations().isEmpty()) {
              // Execute & commit the Local operations on source.
              this.DoLocalSource();

              // Execute & commit the Local operations on target.
              this.DoLocalTarget();

              // Execute & commit the Global post-Local operations.
              r = this.DoGlobalPostLocal();

              assert r != null;

              _operationState = StoreOperationState.DoEnd;
            }
          } finally {
            // Figure out the maximum of the progress made yet during Do operation.
            if (_maxDoState.getValue() < _operationState.getValue()) {
              _maxDoState = _operationState;
            }

            // close connections & release the necessary app locks.
            this.TeardownConnections();
          }

          return r;
        });

        // If pending operation, deserialize the pending operation and perform Undo.
        if (!result.getStoreOperations().isEmpty()) {
          assert result.getStoreOperations().size() == 1;

          try (IStoreOperation op = this.getShardMapManager().getStoreOperationFactory()
              .FromLogEntry(this.getShardMapManager(), result.getStoreOperations().get(0))) {
            op.Undo();
          } catch (Exception e) {
            e.printStackTrace();
            //TODO: Handle Exception
          }
        }
      } while (!result.getStoreOperations().isEmpty());
    } catch (StoreException se) {
      // If store exception was thrown, we will attempt to undo the current operation.
      this.AttemptUndo();

      throw this.OnStoreException(se, _operationState);
    } catch (ShardManagementException e) {
      // If shard map manager exception was thrown, we will attempt to undo the operation.
      this.AttemptUndo();

      throw e;
    }

    assert result != null;
    return result;
  }

  ///#region IDisposable

  /**
   * Performs the undo store operation.
   */
  public final void Undo() {
    try {
      this.getShardMapManager().getRetryPolicy().executeAction(() -> {
        try {
          // Open connections & acquire the necessary app locks.
          this.EstablishConnnections(true);

          if (this.UndoGlobalPreLocal()) {
            if (this.getUndoStartState().getValue()
                <= StoreOperationState.UndoLocalTargetBeginTransaction.getValue()) {
              // Execute & commit the Local operations on target.
              this.UndoLocalTarget();
            }

            if (this.getUndoStartState().getValue()
                <= StoreOperationState.UndoLocalSourceBeginTransaction.getValue()) {
              // Execute & commit the Local undo operations on source.
              this.UndoLocalSource();
            }

            if (this.getUndoStartState().getValue()
                <= StoreOperationState.UndoGlobalPostLocalBeginTransaction.getValue()) {
              // Execute & commit the Global post-Local operations.
              this.UndoGlobalPostLocal();
            }
          }

          _operationState = StoreOperationState.UndoEnd;
        } finally {
          // close connections & release the necessary app locks.
          this.TeardownConnections();
        }
      });
    } catch (StoreException se) {
      throw this.OnStoreException(se, _operationState);
    }
  }

  /**
   * Disposes the object.
   */
  public final void Dispose() {
    this.Dispose(true);
    //TODO: GC.SuppressFinalize(this);
  }

  ///#endregion IDisposable

  /**
   * Performs actual Dispose of resources.
   *
   * @param disposing Whether the invocation was from IDisposable.Dipose method.
   */
  protected void Dispose(boolean disposing) {
    if (_localConnectionTarget != null) {
      //TODO: _localConnectionTarget.Dispose();
      _localConnectionTarget = null;
    }

    if (_localConnectionSource != null) {
      //TODO: _localConnectionSource.Dispose();
      _localConnectionSource = null;
    }

    if (_globalConnection != null) {
      //TODO: _globalConnection.Dispose();
      _globalConnection = null;
    }
  }

  /**
   * Requests the derived class to provide information regarding the connections
   * needed for the operation.
   *
   * @return Information about shards involved in the operation.
   */
  public abstract StoreConnectionInfo GetStoreConnectionInfo();

  /**
   * Performs the initial GSM operation prior to LSM operations.
   *
   * @param ts Transaction scope.
   * @return Pending operations on the target objects if any.
   */
  public abstract StoreResults DoGlobalPreLocalExecute(IStoreTransactionScope ts);

  /**
   * Handles errors from the initial GSM operation prior to LSM operations.
   *
   * @param result Operation result.
   */
  public abstract void HandleDoGlobalPreLocalExecuteError(StoreResults result);

  /**
   * Performs the LSM operation on the source shard.
   *
   * @param ts Transaction scope.
   * @return Result of the operation.
   */
  public abstract StoreResults DoLocalSourceExecute(IStoreTransactionScope ts);

  /**
   * Handles errors from the the LSM operation on the source shard.
   *
   * @param result Operation result.
   */
  public abstract void HandleDoLocalSourceExecuteError(StoreResults result);

  /**
   * Performs the LSM operation on the target shard.
   *
   * @param ts Transaction scope.
   * @return Result of the operation.
   */
  public StoreResults DoLocalTargetExecute(IStoreTransactionScope ts) {
    return new StoreResults();
  }

  /**
   * Performs the final GSM operation after the LSM operations.
   *
   * @param result Operation result.
   */
  public void HandleDoLocalTargetExecuteError(StoreResults result) {
    assert result.getResult() == StoreResult.Success;
  }

  /**
   * Performs the final GSM operation after the LSM operations.
   *
   * @param ts Transaction scope.
   * @return Pending operations on the target objects if any.
   */
  public abstract StoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts);

  /**
   * Handles errors from the final GSM operation after the LSM operations.
   *
   * @param result Operation result.
   */
  public abstract void HandleDoGlobalPostLocalExecuteError(StoreResults result);

  /**
   * Refreshes the cache on successful commit of the final GSM operation after the LSM operations.
   *
   * @param result Operation result.
   */
  public void DoGlobalPostLocalUpdateCache(StoreResults result) {
  }

  /**
   * Performs undo of LSM operation on the target shard.
   *
   * @param ts Transaction scope.
   * @return Result of the operation.
   */
  public StoreResults UndoLocalTargetExecute(IStoreTransactionScope ts) {
    return new StoreResults();
  }

  /**
   * Performs undo of LSM operation on the target shard.
   *
   * @param result Operation result.
   */
  public void HandleUndoLocalTargetExecuteError(StoreResults result) {
    assert result.getResult() == StoreResult.Success;
  }

  /**
   * Performs the undo of GSM operation prior to LSM operations.
   *
   * @param ts Transaction scope.
   * @return Result of the operation.
   */
  public StoreResults UndoGlobalPreLocalExecute(IStoreTransactionScope ts) {
    return ts
        .ExecuteOperation(
            StoreOperationRequestBuilder.SP_FIND_AND_UPDATE_OPERATION_LOG_ENTRY_BY_ID_GLOBAL,
            StoreOperationRequestBuilder
                .findAndUpdateOperationLogEntryByIdGlobal(this.getId(), this.getUndoStartState()));
  }

  /**
   * Handles errors from the undo of GSM operation prior to LSM operations.
   *
   * @param result Operation result.
   */
  public void HandleUndoGlobalPreLocalExecuteError(StoreResults result) {
    // Possible errors are:
    // StoreResult.StoreVersionMismatch
    // StoreResult.MissingParametersForStoredProcedure
    throw StoreOperationErrorHandler.OnCommonErrorGlobal(result,
        StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.getOperationCode()),
        StoreOperationRequestBuilder.SP_FIND_AND_UPDATE_OPERATION_LOG_ENTRY_BY_ID_GLOBAL);
  }

  /**
   * Performs the undo of LSM operation on the source shard.
   *
   * @param ts Transaction scope.
   * @return Result of the operation.
   */
  public abstract StoreResults UndoLocalSourceExecute(IStoreTransactionScope ts);

  /**
   * Handles errors from the undo of LSM operation on the source shard.
   *
   * @param result Operation result.
   */
  public abstract void HandleUndoLocalSourceExecuteError(StoreResults result);

  /**
   * Performs the undo of GSM operation after LSM operations.
   *
   * @param ts Transaction scope.
   * @return Pending operations on the target objects if any.
   */
  public abstract StoreResults UndoGlobalPostLocalExecute(IStoreTransactionScope ts);

  /**
   * Handles errors from the undo of GSM operation after LSM operations.
   *
   * @param result Operation result.
   */
  public abstract void HandleUndoGlobalPostLocalExecuteError(StoreResults result);

  /**
   * Returns the ShardManagementException to be thrown corresponding to a StoreException.
   *
   * @param se Store Exception that has been raised.
   * @param state SQL operation state.
   * @return ShardManagementException to be thrown.
   */
  public ShardManagementException OnStoreException(StoreException se, StoreOperationState state) {
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
        return ExceptionUtils
            .GetStoreExceptionGlobal(this.getErrorCategory(), se, this.getOperationName());

      case DoLocalSourceConnect:
      case DoLocalSourceBeginTransaction:
      case DoLocalSourceExecute:
      case DoLocalSourceCommitTransaction:

      case UndoLocalSourceConnect:
      case UndoLocalSourceBeginTransaction:
      case UndoLocalSourceExecute:
      case UndoLocalSourceCommitTransaction:
        return ExceptionUtils
            .GetStoreExceptionLocal(this.getErrorCategory(), se, this.getOperationName(),
                this.getErrorSourceLocation());

      case DoLocalTargetConnect:
      case DoLocalTargetBeginTransaction:
      case DoLocalTargetExecute:
      case DoLocalTargetCommitTransaction:

      case UndoLocalTargetConnect:
      case UndoLocalTargetBeginTransaction:
      case UndoLocalTargetExecute:
      case UndoLocalTargetCommitTransaction:

      default:
        return ExceptionUtils
            .GetStoreExceptionLocal(this.getErrorCategory(), se, this.getOperationName(),
                this.getErrorTargetLocation());
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
  protected final String GetConnectionStringForShardLocation(ShardLocation location) {
    SqlConnectionStringBuilder tempVar = new SqlConnectionStringBuilder(
        this.getShardMapManager().getCredentials().getConnectionStringShard());
    tempVar.setDataSource(location.getDataSource());
    tempVar.setDatabaseName(location.getDatabase());
    return tempVar.getConnectionString();
  }

  /**
   * Attempts to Undo the current operation which actually had caused an exception.
   * <p>
   * This is basically a best effort attempt.
   */
  private void AttemptUndo() {
    // Identify the point from which we shall start the undo operation.
    this.setUndoStartState(UndoStateForDoState(_maxDoState));

    // If there is something to Undo.
    if (this.getUndoStartState().getValue() < StoreOperationState.UndoEnd.getValue()) {
      try {
        this.Undo();
      } catch (StoreException e) {
        // Do nothing, since we are raising the original Do operation exception.
      } catch (ShardManagementException e2) {
        // Do nothing, since we are raising the original Do operation exception.
      }
    }
  }

  /**
   * Established connections to the target databases.
   *
   * @param undo Is this undo operation.
   */
  private void EstablishConnnections(boolean undo) {
    _operationState =
        undo ? StoreOperationState.UndoGlobalConnect : StoreOperationState.DoGlobalConnect;

    // Find the necessary information for connections.
    StoreConnectionInfo sci = this.GetStoreConnectionInfo();

    assert sci != null;

    // Open global & local connections and acquire application level locks for the corresponding scope.
    _globalConnection = this.getShardMapManager().getStoreConnectionFactory()
        .GetConnection(StoreConnectionKind.Global,
            this.getShardMapManager().getCredentials().getConnectionStringShardMapManager());

    _globalConnection.OpenWithLock(this.getId());

    if (sci.getSourceLocation() != null) {
      _operationState = undo ? StoreOperationState.UndoLocalSourceConnect
          : StoreOperationState.DoLocalSourceConnect;

      _localConnectionSource = this.getShardMapManager().getStoreConnectionFactory()
          .GetConnection(StoreConnectionKind.LocalSource,
              this.GetConnectionStringForShardLocation(sci.getSourceLocation()));

      _localConnectionSource.OpenWithLock(this.getId());
    }

    if (sci.getTargetLocation() != null) {
      assert sci.getSourceLocation() != null;

      _operationState = undo ? StoreOperationState.UndoLocalTargetConnect
          : StoreOperationState.DoLocalTargetConnect;

      _localConnectionTarget = this.getShardMapManager().getStoreConnectionFactory()
          .GetConnection(StoreConnectionKind.LocalTarget,
              this.GetConnectionStringForShardLocation(sci.getTargetLocation()));

      _localConnectionTarget.OpenWithLock(this.getId());
    }
  }

  /**
   * Acquires the transaction scope.
   *
   * @return Transaction scope, operations within the scope excute atomically.
   */
  private IStoreTransactionScope GetTransactionScope(StoreOperationTransactionScopeKind scopeKind) {
    switch (scopeKind) {
      case Global:
        return _globalConnection.GetTransactionScope(StoreTransactionScopeKind.ReadWrite);

      case LocalSource:
        return _localConnectionSource.GetTransactionScope(StoreTransactionScopeKind.ReadWrite);

      default:
        assert scopeKind == StoreOperationTransactionScopeKind.LocalTarget;
        return _localConnectionTarget.GetTransactionScope(StoreTransactionScopeKind.ReadWrite);
    }
  }

  /**
   * Performs the initial GSM operation prior to LSM operations.
   *
   * @return Pending operations on the target objects if any.
   */
  private StoreResults DoGlobalPreLocal() {
    StoreResults result = null;

    _operationState = StoreOperationState.DoGlobalPreLocalBeginTransaction;

    try (IStoreTransactionScope ts = this
        .GetTransactionScope(StoreOperationTransactionScopeKind.Global)) {
      _operationState = StoreOperationState.DoGlobalPreLocalExecute;

      result = this.DoGlobalPreLocalExecute(ts);

      if (result.getResult() == StoreResult.Success) {
        ts.setSuccess(true);
        _operationState = StoreOperationState.DoGlobalPreLocalCommitTransaction;
      }
    } catch (Exception e) {
      e.printStackTrace();
      return null;
      //TODO: Handle Exception
    }

    if (result.getResult() != StoreResult.Success
        && result.getResult() != StoreResult.ShardPendingOperation) {
      this.HandleDoGlobalPreLocalExecuteError(result);
    }

    return result;
  }

  /**
   * Performs the LSM operation on the source shard.
   */
  private void DoLocalSource() {
    StoreResults result;

    _operationState = StoreOperationState.DoLocalSourceBeginTransaction;

    try (IStoreTransactionScope ts = this
        .GetTransactionScope(StoreOperationTransactionScopeKind.LocalSource)) {
      _operationState = StoreOperationState.DoLocalSourceExecute;

      result = this.DoLocalSourceExecute(ts);

      if (result.getResult() == StoreResult.Success) {
        ts.setSuccess(true);
        _operationState = StoreOperationState.DoLocalSourceCommitTransaction;
      }
    } catch (Exception e) {
      e.printStackTrace();
      result = null;
      //TODO: Handle Exception
    }

    if (result.getResult() != StoreResult.Success) {
      this.HandleDoLocalSourceExecuteError(result);
    }
  }

  /**
   * Performs the LSM operation on the target shard.
   */
  private void DoLocalTarget() {
    if (_localConnectionTarget != null) {
      StoreResults result;

      _operationState = StoreOperationState.DoLocalTargetBeginTransaction;

      try (IStoreTransactionScope ts = this
          .GetTransactionScope(StoreOperationTransactionScopeKind.LocalTarget)) {
        _operationState = StoreOperationState.DoLocalTargetExecute;

        result = this.DoLocalTargetExecute(ts);

        if (result.getResult() == StoreResult.Success) {
          ts.setSuccess(true);
          _operationState = StoreOperationState.DoLocalTargetCommitTransaction;
        }
      } catch (Exception e) {
        e.printStackTrace();
        result = null;
        //TODO: Handle Exception
      }

      if (result.getResult() != StoreResult.Success) {
        this.HandleDoLocalTargetExecuteError(result);
      }
    }
  }

  /**
   * Performs the final GSM operation after the LSM operations.
   *
   * @return Results of the GSM operation.
   */
  private StoreResults DoGlobalPostLocal() {
    StoreResults result;

    _operationState = StoreOperationState.DoGlobalPostLocalBeginTransaction;

    try (IStoreTransactionScope ts = this
        .GetTransactionScope(StoreOperationTransactionScopeKind.Global)) {
      _operationState = StoreOperationState.DoGlobalPostLocalExecute;

      result = this.DoGlobalPostLocalExecute(ts);

      if (result.getResult() == StoreResult.Success) {
        ts.setSuccess(true);
        _operationState = StoreOperationState.DoGlobalPostLocalCommitTransaction;
      }
    } catch (Exception e) {
      e.printStackTrace();
      return null;
      //TODO: Handle Exception
    }

    if (result.getResult() != StoreResult.Success) {
      this.HandleDoGlobalPostLocalExecuteError(result);
    } else {
      this.DoGlobalPostLocalUpdateCache(result);
    }

    return result;
  }

  /**
   * Perform undo of GSM operations before LSM operations. Basically checks if the operation
   * to be undone is still present in the log.
   *
   * @return <c>true</c> if further undo operations are necessary, <c>false</c> otherwise.
   */
  private boolean UndoGlobalPreLocal() {
    StoreResults result;

    _operationState = StoreOperationState.UndoGlobalPreLocalBeginTransaction;

    try (IStoreTransactionScope ts = this
        .GetTransactionScope(StoreOperationTransactionScopeKind.Global)) {
      _operationState = StoreOperationState.UndoGlobalPreLocalExecute;

      result = this.UndoGlobalPreLocalExecute(ts);

      if (result.getResult() == StoreResult.Success) {
        ts.setSuccess(true);

        if (!result.getStoreOperations().isEmpty()) {
          this.setOriginalShardVersionRemoves(
              result.getStoreOperations().get(0).getOriginalShardVersionRemoves());
          this.setOriginalShardVersionAdds(
              result.getStoreOperations().get(0).getOriginalShardVersionAdds());
          _operationState = StoreOperationState.UndoGlobalPreLocalCommitTransaction;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      return false;
      //TODO: Handle Exception
    }

    if (result.getResult() != StoreResult.Success) {
      this.HandleUndoGlobalPreLocalExecuteError(result);
    }

    return !result.getStoreOperations().isEmpty();
  }

  /**
   * Performs the undo of LSM operation on the target shard.
   */
  private void UndoLocalTarget() {
    if (_localConnectionTarget != null) {
      StoreResults result;

      _operationState = StoreOperationState.UndoLocalTargetBeginTransaction;

      try (IStoreTransactionScope ts = this
          .GetTransactionScope(StoreOperationTransactionScopeKind.LocalTarget)) {
        _operationState = StoreOperationState.UndoLocalTargetExecute;

        result = this.UndoLocalTargetExecute(ts);

        if (result.getResult() == StoreResult.Success) {
          ts.setSuccess(true);
          _operationState = StoreOperationState.UndoLocalTargetCommitTransaction;
        }
      } catch (Exception e) {
        e.printStackTrace();
        result = null;
        //TODO: Handle Exception
      }

      if (result.getResult() != StoreResult.Success) {
        this.HandleUndoLocalTargetExecuteError(result);
      }
    }
  }

  /**
   * Performs the undo of LSM operation on the source shard.
   */
  private void UndoLocalSource() {
    StoreResults result;

    _operationState = StoreOperationState.UndoLocalSourceBeginTransaction;

    try (IStoreTransactionScope ts = this
        .GetTransactionScope(StoreOperationTransactionScopeKind.LocalSource)) {
      _operationState = StoreOperationState.UndoLocalSourceExecute;

      result = this.UndoLocalSourceExecute(ts);

      if (result.getResult() == StoreResult.Success) {
        ts.setSuccess(true);
        _operationState = StoreOperationState.UndoLocalSourceCommitTransaction;
      }
    } catch (Exception e) {
      e.printStackTrace();
      result = null;
      //TODO: Handle Exception
    }

    if (result.getResult() != StoreResult.Success) {
      this.HandleUndoLocalSourceExecuteError(result);
    }
  }

  /**
   * Performs the undo of GSM operation after LSM operations.
   */
  private void UndoGlobalPostLocal() {
    StoreResults result;

    _operationState = StoreOperationState.UndoGlobalPostLocalBeginTransaction;

    try (IStoreTransactionScope ts = this
        .GetTransactionScope(StoreOperationTransactionScopeKind.Global)) {
      _operationState = StoreOperationState.UndoGlobalPostLocalExecute;

      result = this.UndoGlobalPostLocalExecute(ts);

      if (result.getResult() == StoreResult.Success) {
        ts.setSuccess(true);
        _operationState = StoreOperationState.UndoGlobalPostLocalCommitTransaction;
      }
    } catch (Exception e) {
      e.printStackTrace();
      result = null;
      //TODO: Handle Exception
    }

    if (result.getResult() != StoreResult.Success) {
      this.HandleUndoGlobalPostLocalExecuteError(result);
    }
  }

  /**
   * Terminates connections to target databases.
   */
  private void TeardownConnections() {
    if (_localConnectionTarget != null) {
      _localConnectionTarget.CloseWithUnlock(this.getId());
    }

    if (_localConnectionSource != null) {
      _localConnectionSource.CloseWithUnlock(this.getId());
    }

    if (_globalConnection != null) {
      _globalConnection.CloseWithUnlock(this.getId());
    }
  }
}
