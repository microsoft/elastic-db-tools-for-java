package com.microsoft.azure.elasticdb.shard.stubs;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreException;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreShard;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreConnectionInfo;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationState;
import com.microsoft.azure.elasticdb.shard.storeops.map.AddShardOperation;
import com.microsoft.azure.elasticdb.shard.stubhelper.Action1Param;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func0Param;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func1Param;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func2Param;

/**
 * Stub type of AddShardOperation.
 */
public class StubAddShardOperation extends AddShardOperation {

  /**
   * Sets the stub of StoreOperation.dispose(Boolean disposing)
   */
  public Action1Param<Boolean> disposeBoolean;
  /**
   * Sets the stub of AddShardOperation.doGlobalPostLocalExecute(IStoreTransactionScope ts)
   */
  public Func1Param<IStoreTransactionScope, StoreResults>
      doGlobalPostLocalExecuteIStoreTransactionScope;
  /**
   * Sets the stub of StoreOperation.doGlobalPostLocalUpdateCache(StoreResults result)
   */
  public Action1Param<StoreResults> doGlobalPostLocalUpdateCacheIStoreResults;
  /**
   * Sets the stub of AddShardOperation.doGlobalPreLocalExecute(IStoreTransactionScope ts)
   */
  public Func1Param<IStoreTransactionScope, StoreResults>
      doGlobalPreLocalExecuteIStoreTransactionScope;
  /**
   * Sets the stub of AddShardOperation.doLocalSourceExecute(IStoreTransactionScope ts)
   */
  public Func1Param<IStoreTransactionScope, StoreResults>
      doLocalSourceExecuteIStoreTransactionScope;
  /**
   * Sets the stub of StoreOperation.doLocalTargetExecute(IStoreTransactionScope ts)
   */
  public Func1Param<IStoreTransactionScope, StoreResults>
      doLocalTargetExecuteIStoreTransactionScope;
  /**
   * Sets the stub of AddShardOperation.get_ErrorCategory()
   */
  public Func0Param<ShardManagementErrorCategory> errorCategoryGet;
  /**
   * Sets the stub of AddShardOperation.get_ErrorSourceLocation()
   */
  public Func0Param<ShardLocation> errorSourceLocationGet;
  /**
   * Sets the stub of AddShardOperation.get_ErrorTargetLocation()
   */
  public Func0Param<ShardLocation> errorTargetLocationGet;
  /**
   * Sets the stub of AddShardOperation.getStoreConnectionInfo()
   */
  public Func0Param<StoreConnectionInfo> getStoreConnectionInfo01;
  /**
   * Sets the stub of AddShardOperation.handleDoGlobalPostLocalExecuteError(StoreResults result)
   */
  public Action1Param<StoreResults> handleDoGlobalPostLocalExecuteErrorIStoreResults;
  /**
   * Sets the stub of AddShardOperation.handleDoGlobalPreLocalExecuteError(StoreResults result)
   */
  public Action1Param<StoreResults> handleDoGlobalPreLocalExecuteErrorIStoreResults;
  /**
   * Sets the stub of AddShardOperation.handleDoLocalSourceExecuteError(StoreResults result)
   */
  public Action1Param<StoreResults> handleDoLocalSourceExecuteErrorIStoreResults;
  /**
   * Sets the stub of StoreOperation.handleDoLocalTargetExecuteError(StoreResults result)
   */
  public Action1Param<StoreResults> handleDoLocalTargetExecuteErrorIStoreResults;
  /**
   * Sets the stub of AddShardOperation.handleUndoGlobalPostLocalExecuteError(StoreResults result)
   */
  public Action1Param<StoreResults> handleUndoGlobalPostLocalExecuteErrorIStoreResults;
  /**
   * Sets the stub of StoreOperation.handleUndoGlobalPreLocalExecuteError(StoreResults result)
   */
  public Action1Param<StoreResults> handleUndoGlobalPreLocalExecuteErrorIStoreResults;
  /**
   * Sets the stub of AddShardOperation.handleUndoLocalSourceExecuteError(StoreResults result)
   */
  public Action1Param<StoreResults> handleUndoLocalSourceExecuteErrorIStoreResults;
  /**
   * Sets the stub of StoreOperation.handleUndoLocalTargetExecuteError(StoreResults result)
   */
  public Action1Param<StoreResults> handleUndoLocalTargetExecuteErrorIStoreResults;
  /**
   * Sets the stub of StoreOperation.onStoreException(StoreException se, StoreOperationState state)
   */
  public Func2Param<StoreException, StoreOperationState, ShardManagementException>
      onStoreExceptionStoreExceptionStoreOperationState;
  /**
   * Sets the stub of AddShardOperation.undoGlobalPostLocalExecute(IStoreTransactionScope ts)
   */
  public Func1Param<IStoreTransactionScope, StoreResults>
      undoGlobalPostLocalExecuteIStoreTransactionScope;
  /**
   * Sets the stub of StoreOperation.undoGlobalPreLocalExecute(IStoreTransactionScope ts)
   */
  public Func1Param<IStoreTransactionScope, StoreResults>
      undoGlobalPreLocalExecuteIStoreTransactionScope;
  /**
   * Sets the stub of AddShardOperation.undoLocalSourceExecute(IStoreTransactionScope ts)
   */
  public Func1Param<IStoreTransactionScope, StoreResults>
      undoLocalSourceExecuteIStoreTransactionScope;
  /**
   * Sets the stub of StoreOperation.undoLocalTargetExecute(IStoreTransactionScope ts)
   */
  public Func1Param<IStoreTransactionScope, StoreResults>
      undoLocalTargetExecuteIStoreTransactionScope;

  private boolean callBase;
  private IStubBehavior instanceBehavior;

  /**
   * Initializes a new instance.
   */
  public StubAddShardOperation(ShardMapManager shardMapManager, StoreShardMap shardMap,
      StoreShard shard) {
    super(shardMapManager, shardMap, shard);
    this.initializeStub();
  }

  /**
   * Gets or sets a value that indicates if the base method should be called instead of the fallback
   * behavior.
   */
  public final boolean getCallBase() {
    return this.callBase;
  }

  public final void setCallBase(boolean value) {
    this.callBase = value;
  }

  /**
   * Sets the stub of AddShardOperation.get_ErrorCategory()
   */
  @Override
  protected ShardManagementErrorCategory getErrorCategory() {
    Func0Param<ShardManagementErrorCategory> func1 = () -> errorCategoryGet.invoke();
    if (func1 != null) {
      return func1.invoke();
    }
    if (this.callBase) {
      return super.getErrorCategory();
    }
    return this.getInstanceBehavior().result(
        this, "get_ErrorCategory");
  }

  /**
   * Sets the stub of AddShardOperation.get_ErrorSourceLocation()
   */
  @Override
  protected ShardLocation getErrorSourceLocation() {
    Func0Param<ShardLocation> func1 = () -> errorSourceLocationGet.invoke();
    if (func1 != null) {
      return func1.invoke();
    }
    if (this.callBase) {
      return super.getErrorSourceLocation();
    }
    return this.getInstanceBehavior().result(this,
        "get_ErrorSourceLocation");
  }

  /**
   * Sets the stub of AddShardOperation.get_ErrorTargetLocation()
   */
  @Override
  protected ShardLocation getErrorTargetLocation() {
    Func0Param<ShardLocation> func1 = () -> errorTargetLocationGet.invoke();
    if (func1 != null) {
      return func1.invoke();
    }
    if (this.callBase) {
      return super.getErrorTargetLocation();
    }
    return this.getInstanceBehavior().result(this,
        "get_ErrorTargetLocation");
  }

  /**
   * Gets or sets the instance behavior.
   */
  public final IStubBehavior getInstanceBehavior() {
    return StubBehaviors.getValueOrCurrent(this.instanceBehavior);
  }

  public final void setInstanceBehavior(IStubBehavior value) {
    this.instanceBehavior = value;
  }

  /**
   * Sets the stub of AddShardOperation.doGlobalPostLocalExecute(IStoreTransactionScope ts)
   */
  @Override
  public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
    Func1Param<IStoreTransactionScope, StoreResults> func1 = (IStoreTransactionScope arg) ->
        doGlobalPostLocalExecuteIStoreTransactionScope.invoke(arg);
    if (func1 != null) {
      return func1.invoke(ts);
    }
    if (this.callBase) {
      return super.doGlobalPostLocalExecute(ts);
    }
    return this.getInstanceBehavior().result(this, "doGlobalPostLocalExecute");
  }

  /**
   * Sets the stub of StoreOperation.doGlobalPostLocalUpdateCache(StoreResults result)
   */
  @Override
  public void doGlobalPostLocalUpdateCache(StoreResults result) {
    Action1Param<StoreResults> action1 = (StoreResults obj) ->
        doGlobalPostLocalUpdateCacheIStoreResults.invoke(obj);
    if (action1 != null) {
      action1.invoke(result);
    } else if (this.callBase) {
      super.doGlobalPostLocalUpdateCache(result);
    } else {
      this.getInstanceBehavior().voidResult(this, "doGlobalPostLocalUpdateCache");
    }
  }

  /**
   * Sets the stub of AddShardOperation.doGlobalPreLocalExecute(IStoreTransactionScope ts)
   */
  @Override
  public StoreResults doGlobalPreLocalExecute(IStoreTransactionScope ts) {
    Func1Param<IStoreTransactionScope, StoreResults> func1 = (IStoreTransactionScope arg) ->
        doGlobalPreLocalExecuteIStoreTransactionScope.invoke(arg);
    if (func1 != null) {
      return func1.invoke(ts);
    }
    if (this.callBase) {
      return super.doGlobalPreLocalExecute(ts);
    }
    return this.getInstanceBehavior().result(this,
        "doGlobalPreLocalExecute");
  }

  /**
   * Sets the stub of AddShardOperation.doLocalSourceExecute(IStoreTransactionScope ts)
   */
  @Override
  public StoreResults doLocalSourceExecute(IStoreTransactionScope ts) {
    Func1Param<IStoreTransactionScope, StoreResults> func1 = (IStoreTransactionScope arg) ->
        doLocalSourceExecuteIStoreTransactionScope.invoke(arg);
    if (func1 != null) {
      return func1.invoke(ts);
    }
    if (this.callBase) {
      return super.doLocalSourceExecute(ts);
    }
    return this.getInstanceBehavior().result(this,
        "doLocalSourceExecute");
  }

  /**
   * Sets the stub of StoreOperation.doLocalTargetExecute(IStoreTransactionScope ts)
   */
  @Override
  public StoreResults doLocalTargetExecute(IStoreTransactionScope ts) {
    Func1Param<IStoreTransactionScope, StoreResults> func1 = (IStoreTransactionScope arg) ->
        doLocalTargetExecuteIStoreTransactionScope.invoke(arg);
    if (func1 != null) {
      return func1.invoke(ts);
    }
    if (this.callBase) {
      return super.doLocalTargetExecute(ts);
    }
    return this.getInstanceBehavior().result(this,
        "doLocalTargetExecute");
  }

  /**
   * Sets the stub of AddShardOperation.getStoreConnectionInfo()
   */
  @Override
  public StoreConnectionInfo getStoreConnectionInfo() {
    Func0Param<StoreConnectionInfo> func1 = () -> getStoreConnectionInfo01.invoke();
    if (func1 != null) {
      return func1.invoke();
    }
    if (this.callBase) {
      return super.getStoreConnectionInfo();
    }
    return this.getInstanceBehavior().result(this,
        "getStoreConnectionInfo");
  }

  /**
   * Sets the stub of AddShardOperation.handleDoGlobalPostLocalExecuteError(StoreResults result)
   */
  @Override
  public void handleDoGlobalPostLocalExecuteError(StoreResults result) {
    Action1Param<StoreResults> action1 = (StoreResults obj) ->
        handleDoGlobalPostLocalExecuteErrorIStoreResults.invoke(obj);
    if (action1 != null) {
      action1.invoke(result);
    } else if (this.callBase) {
      super.handleDoGlobalPostLocalExecuteError(result);
    } else {
      this.getInstanceBehavior().voidResult(this,
          "handleDoGlobalPostLocalExecuteError");
    }
  }

  /**
   * Sets the stub of AddShardOperation.handleDoGlobalPreLocalExecuteError(StoreResults result)
   */
  @Override
  public void handleDoGlobalPreLocalExecuteError(StoreResults result) {
    Action1Param<StoreResults> action1 = (StoreResults obj) ->
        handleDoGlobalPreLocalExecuteErrorIStoreResults.invoke(obj);
    if (action1 != null) {
      action1.invoke(result);
    } else if (this.callBase) {
      super.handleDoGlobalPreLocalExecuteError(result);
    } else {
      this.getInstanceBehavior().voidResult(this,
          "handleDoGlobalPreLocalExecuteError");
    }
  }

  /**
   * Sets the stub of AddShardOperation.handleDoLocalSourceExecuteError(StoreResults result)
   */
  @Override
  public void handleDoLocalSourceExecuteError(StoreResults result) {
    Action1Param<StoreResults> action1 = (StoreResults obj) ->
        handleDoLocalSourceExecuteErrorIStoreResults.invoke(obj);
    if (action1 != null) {
      action1.invoke(result);
    } else if (this.callBase) {
      super.handleDoLocalSourceExecuteError(result);
    } else {
      this.getInstanceBehavior().voidResult(this,
          "handleDoLocalSourceExecuteError");
    }
  }

  /**
   * Sets the stub of StoreOperation.handleDoLocalTargetExecuteError(StoreResults result)
   */
  @Override
  public void handleDoLocalTargetExecuteError(StoreResults result) {
    Action1Param<StoreResults> action1 = (StoreResults obj) ->
        handleDoLocalTargetExecuteErrorIStoreResults.invoke(obj);
    if (action1 != null) {
      action1.invoke(result);
    } else if (this.callBase) {
      super.handleDoLocalTargetExecuteError(result);
    } else {
      this.getInstanceBehavior().voidResult(this,
          "handleDoLocalTargetExecuteError");
    }
  }

  /**
   * Sets the stub of AddShardOperation.handleUndoGlobalPostLocalExecuteError(StoreResults result)
   */
  @Override
  public void handleUndoGlobalPostLocalExecuteError(StoreResults result) {
    Action1Param<StoreResults> action1 = (StoreResults obj) ->
        handleUndoGlobalPostLocalExecuteErrorIStoreResults.invoke(obj);
    if (action1 != null) {
      action1.invoke(result);
    } else if (this.callBase) {
      super.handleUndoGlobalPostLocalExecuteError(result);
    } else {
      this.getInstanceBehavior().voidResult(this,
          "handleUndoGlobalPostLocalExecuteError");
    }
  }

  /**
   * Sets the stub of StoreOperation.handleUndoGlobalPreLocalExecuteError(StoreResults result)
   */
  @Override
  public void handleUndoGlobalPreLocalExecuteError(StoreResults result) {
    Action1Param<StoreResults> action1 = (StoreResults obj) ->
        handleUndoGlobalPreLocalExecuteErrorIStoreResults.invoke(obj);
    if (action1 != null) {
      action1.invoke(result);
    } else if (this.callBase) {
      super.handleUndoGlobalPreLocalExecuteError(result);
    } else {
      this.getInstanceBehavior().voidResult(this,
          "handleUndoGlobalPreLocalExecuteError");
    }
  }

  /**
   * Sets the stub of AddShardOperation.handleUndoLocalSourceExecuteError(StoreResults result)
   */
  @Override
  public void handleUndoLocalSourceExecuteError(StoreResults result) {
    Action1Param<StoreResults> action1 = (StoreResults obj) ->
        handleUndoLocalSourceExecuteErrorIStoreResults.invoke(obj);
    if (action1 != null) {
      action1.invoke(result);
    } else if (this.callBase) {
      super.handleUndoLocalSourceExecuteError(result);
    } else {
      this.getInstanceBehavior().voidResult(this,
          "handleUndoLocalSourceExecuteError");
    }
  }

  /**
   * Sets the stub of StoreOperation.handleUndoLocalTargetExecuteError(StoreResults result)
   */
  @Override
  public void handleUndoLocalTargetExecuteError(StoreResults result) {
    Action1Param<StoreResults> action1 = (StoreResults obj) ->
        handleUndoLocalTargetExecuteErrorIStoreResults.invoke(obj);
    if (action1 != null) {
      action1.invoke(result);
    } else if (this.callBase) {
      super.handleUndoLocalTargetExecuteError(result);
    } else {
      this.getInstanceBehavior().voidResult(this,
          "handleUndoLocalTargetExecuteError");
    }
  }

  /**
   * Initializes a new instance of type StubAddShardOperation.
   */
  private void initializeStub() {
  }

  /**
   * Sets the stub of StoreOperation.onStoreException(StoreException se, StoreOperationState state)
   */
  @Override
  public ShardManagementException onStoreException(StoreException se, StoreOperationState state) {
    Func2Param<StoreException, StoreOperationState, ShardManagementException> func1
        = (StoreException arg1, StoreOperationState arg2) ->
        onStoreExceptionStoreExceptionStoreOperationState.invoke(arg1, arg2);
    if (func1 != null) {
      return func1.invoke(se, state);
    }
    if (this.callBase) {
      return super.onStoreException(se, state);
    }
    return this.getInstanceBehavior().result(this,
        "onStoreException");
  }

  /**
   * Sets the stub of AddShardOperation.undoGlobalPostLocalExecute(IStoreTransactionScope ts)
   */
  @Override
  public StoreResults undoGlobalPostLocalExecute(IStoreTransactionScope ts) {
    Func1Param<IStoreTransactionScope, StoreResults> func1 = (IStoreTransactionScope arg) ->
        undoGlobalPostLocalExecuteIStoreTransactionScope.invoke(arg);
    if (func1 != null) {
      return func1.invoke(ts);
    }
    if (this.callBase) {
      return super.undoGlobalPostLocalExecute(ts);
    }
    return this.getInstanceBehavior().result(this,
        "undoGlobalPostLocalExecute");
  }

  /**
   * Sets the stub of StoreOperation.undoGlobalPreLocalExecute(IStoreTransactionScope ts)
   */
  @Override
  public StoreResults undoGlobalPreLocalExecute(IStoreTransactionScope ts) {
    Func1Param<IStoreTransactionScope, StoreResults> func1 = (IStoreTransactionScope arg) ->
        undoGlobalPreLocalExecuteIStoreTransactionScope.invoke(arg);
    if (func1 != null) {
      return func1.invoke(ts);
    }
    if (this.callBase) {
      return super.undoGlobalPreLocalExecute(ts);
    }
    return this.getInstanceBehavior().result(this,
        "undoGlobalPreLocalExecute");
  }

  /**
   * Sets the stub of AddShardOperation.undoLocalSourceExecute(IStoreTransactionScope ts)
   */
  @Override
  public StoreResults undoLocalSourceExecute(IStoreTransactionScope ts) {
    Func1Param<IStoreTransactionScope, StoreResults> func1 = (IStoreTransactionScope arg) ->
        undoLocalSourceExecuteIStoreTransactionScope.invoke(arg);
    if (func1 != null) {
      return func1.invoke(ts);
    }
    if (this.callBase) {
      return super.undoLocalSourceExecute(ts);
    }
    return this.getInstanceBehavior().result(this,
        "undoLocalSourceExecute");
  }

  /**
   * Sets the stub of StoreOperation.undoLocalTargetExecute(IStoreTransactionScope ts)
   */
  @Override
  public StoreResults undoLocalTargetExecute(IStoreTransactionScope ts) {
    Func1Param<IStoreTransactionScope, StoreResults> func1 = (IStoreTransactionScope arg) ->
        undoLocalTargetExecuteIStoreTransactionScope.invoke(arg);
    if (func1 != null) {
      return func1.invoke(ts);
    }
    if (this.callBase) {
      return super.undoLocalTargetExecute(ts);
    }
    return this.getInstanceBehavior().result(this,
        "undoLocalTargetExecute");
  }
}
