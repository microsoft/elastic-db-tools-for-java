package com.microsoft.azure.elasticdb.shard.stubs;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreException;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreConnectionInfo;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationCode;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationState;
import com.microsoft.azure.elasticdb.shard.storeops.mapper.ReplaceMappingsOperation;
import com.microsoft.azure.elasticdb.shard.stubhelper.Action1Param;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func0Param;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func1Param;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func2Param;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Stub type of Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ReplaceMappingsOperation
 */
public class StubReplaceMappingsOperation extends ReplaceMappingsOperation {

  /**
   * Sets the stub of StoreOperation.dispose(Boolean disposing)
   */
  public Action1Param<Boolean> DisposeBoolean;
  /**
   * Sets the stub of ReplaceMappingsOperation.doGlobalPostLocalExecute(IStoreTransactionScope ts)
   */
  public Func1Param<IStoreTransactionScope, StoreResults> DoGlobalPostLocalExecuteIStoreTransactionScope;
  /**
   * Sets the stub of ReplaceMappingsOperation.doGlobalPostLocalUpdateCache(StoreResults result)
   */
  public Action1Param<StoreResults> DoGlobalPostLocalUpdateCacheIStoreResults;
  /**
   * Sets the stub of ReplaceMappingsOperation.doGlobalPreLocalExecute(IStoreTransactionScope ts)
   */
  public Func1Param<IStoreTransactionScope, StoreResults> DoGlobalPreLocalExecuteIStoreTransactionScope;
  /**
   * Sets the stub of ReplaceMappingsOperation.doLocalSourceExecute(IStoreTransactionScope ts)
   */
  public Func1Param<IStoreTransactionScope, StoreResults> DoLocalSourceExecuteIStoreTransactionScope;
  /**
   * Sets the stub of StoreOperation.doLocalTargetExecute(IStoreTransactionScope ts)
   */
  public Func1Param<IStoreTransactionScope, StoreResults> DoLocalTargetExecuteIStoreTransactionScope;
  /**
   * Sets the stub of ReplaceMappingsOperation.get_ErrorCategory()
   */
  public Func0Param<ShardManagementErrorCategory> ErrorCategoryGet;
  /**
   * Sets the stub of ReplaceMappingsOperation.get_ErrorSourceLocation()
   */
  public Func0Param<ShardLocation> ErrorSourceLocationGet;
  /**
   * Sets the stub of ReplaceMappingsOperation.get_ErrorTargetLocation()
   */
  public Func0Param<ShardLocation> ErrorTargetLocationGet;
  /**
   * Sets the stub of ReplaceMappingsOperation.getStoreConnectionInfo()
   */
  public Func0Param<StoreConnectionInfo> GetStoreConnectionInfo01;
  /**
   * Sets the stub of ReplaceMappingsOperation.handleDoGlobalPostLocalExecuteError(StoreResults
   * result)
   */
  public Action1Param<StoreResults> HandleDoGlobalPostLocalExecuteErrorIStoreResults;
  /**
   * Sets the stub of ReplaceMappingsOperation.handleDoGlobalPreLocalExecuteError(StoreResults
   * result)
   */
  public Action1Param<StoreResults> HandleDoGlobalPreLocalExecuteErrorIStoreResults;
  /**
   * Sets the stub of ReplaceMappingsOperation.handleDoLocalSourceExecuteError(StoreResults result)
   */
  public Action1Param<StoreResults> HandleDoLocalSourceExecuteErrorIStoreResults;
  /**
   * Sets the stub of StoreOperation.handleDoLocalTargetExecuteError(StoreResults result)
   */
  public Action1Param<StoreResults> HandleDoLocalTargetExecuteErrorIStoreResults;
  /**
   * Sets the stub of ReplaceMappingsOperation.handleUndoGlobalPostLocalExecuteError(StoreResults
   * result)
   */
  public Action1Param<StoreResults> HandleUndoGlobalPostLocalExecuteErrorIStoreResults;
  /**
   * Sets the stub of StoreOperation.handleUndoGlobalPreLocalExecuteError(StoreResults result)
   */
  public Action1Param<StoreResults> HandleUndoGlobalPreLocalExecuteErrorIStoreResults;
  /**
   * Sets the stub of ReplaceMappingsOperation.handleUndoLocalSourceExecuteError(StoreResults
   * result)
   */
  public Action1Param<StoreResults> HandleUndoLocalSourceExecuteErrorIStoreResults;
  /**
   * Sets the stub of StoreOperation.handleUndoLocalTargetExecuteError(StoreResults result)
   */
  public Action1Param<StoreResults> HandleUndoLocalTargetExecuteErrorIStoreResults;
  /**
   * Sets the stub of StoreOperation.onStoreException(StoreException se, StoreOperationState state)
   */
  public Func2Param<StoreException, StoreOperationState, ShardManagementException> OnStoreExceptionStoreExceptionStoreOperationState;
  /**
   * Sets the stub of ReplaceMappingsOperation.undoGlobalPostLocalExecute(IStoreTransactionScope ts)
   */
  public Func1Param<IStoreTransactionScope, StoreResults> UndoGlobalPostLocalExecuteIStoreTransactionScope;
  /**
   * Sets the stub of StoreOperation.undoGlobalPreLocalExecute(IStoreTransactionScope ts)
   */
  public Func1Param<IStoreTransactionScope, StoreResults> UndoGlobalPreLocalExecuteIStoreTransactionScope;
  /**
   * Sets the stub of ReplaceMappingsOperation.undoLocalSourceExecute(IStoreTransactionScope ts)
   */
  public Func1Param<IStoreTransactionScope, StoreResults> UndoLocalSourceExecuteIStoreTransactionScope;
  /**
   * Sets the stub of StoreOperation.undoLocalTargetExecute(IStoreTransactionScope ts)
   */
  public Func1Param<IStoreTransactionScope, StoreResults> UndoLocalTargetExecuteIStoreTransactionScope;
  private boolean ___callBase;
  private IStubBehavior ___instanceBehavior;

  /**
   * Initializes a new instance
   */
  public StubReplaceMappingsOperation(ShardMapManager shardMapManager,
      StoreOperationCode operationCode, StoreShardMap shardMap,
      List<Pair<StoreMapping, UUID>> mappingsSource,
      List<Pair<StoreMapping, UUID>> mappingsTarget) {
    super(shardMapManager, operationCode, shardMap, mappingsSource, mappingsTarget);
    this.InitializeStub();
  }

  /**
   * Gets or sets a value that indicates if the base method should be called instead of the fallback
   * behavior
   */
  public final boolean getCallBase() {
    return this.___callBase;
  }

  public final void setCallBase(boolean value) {
    this.___callBase = value;
  }

  /**
   * Sets the stub of ReplaceMappingsOperation.get_ErrorCategory()
   */
  @Override
  protected ShardManagementErrorCategory getErrorCategory() {
    Func0Param<ShardManagementErrorCategory> func1 = () -> ErrorCategoryGet.invoke();
    if (func1 != null) {
      return func1.invoke();
    }
    if (this.___callBase) {
      return super.getErrorCategory();
    }
    return this
        .getInstanceBehavior().Result(
            this, "get_ErrorCategory");
  }

  /**
   * Sets the stub of ReplaceMappingsOperation.get_ErrorSourceLocation()
   */
  @Override
  protected ShardLocation getErrorSourceLocation() {
    Func0Param<ShardLocation> func1 = () -> ErrorSourceLocationGet.invoke();
    if (func1 != null) {
      return func1.invoke();
    }
    if (this.___callBase) {
      return super.getErrorSourceLocation();
    }
    return this.getInstanceBehavior().Result(this,
        "get_ErrorSourceLocation");
  }

  /**
   * Sets the stub of ReplaceMappingsOperation.get_ErrorTargetLocation()
   */
  @Override
  protected ShardLocation getErrorTargetLocation() {
    Func0Param<ShardLocation> func1 = () -> ErrorTargetLocationGet.invoke();
    if (func1 != null) {
      return func1.invoke();
    }
    if (this.___callBase) {
      return super.getErrorTargetLocation();
    }
    return this.getInstanceBehavior().Result(this,
        "get_ErrorTargetLocation");
  }

  /**
   * Gets or sets the instance behavior.
   */
  public final IStubBehavior getInstanceBehavior() {
    return StubBehaviors.GetValueOrCurrent(this.___instanceBehavior);
  }

  public final void setInstanceBehavior(IStubBehavior value) {
    this.___instanceBehavior = value;
  }

  /**
   * Sets the stub of ReplaceMappingsOperation.doGlobalPostLocalExecute(IStoreTransactionScope ts)
   */
  @Override
  public StoreResults doGlobalPostLocalExecute(IStoreTransactionScope ts) {
    Func1Param<IStoreTransactionScope, StoreResults> func1 = (IStoreTransactionScope arg) -> DoGlobalPostLocalExecuteIStoreTransactionScope
        .invoke(arg);
    if (func1 != null) {
      return func1.invoke(ts);
    }
    if (this.___callBase) {
      return super.doGlobalPostLocalExecute(ts);
    }
    return this.getInstanceBehavior().Result(this,
        "doGlobalPostLocalExecute");
  }

  /**
   * Sets the stub of ReplaceMappingsOperation.doGlobalPostLocalUpdateCache(StoreResults result)
   */
  @Override
  public void doGlobalPostLocalUpdateCache(StoreResults result) {
    Action1Param<StoreResults> action1 = (StoreResults obj) -> DoGlobalPostLocalUpdateCacheIStoreResults
        .invoke(obj);
    if (action1 != null) {
      action1.invoke(result);
    } else if (this.___callBase) {
      super.doGlobalPostLocalUpdateCache(result);
    } else {
      this.getInstanceBehavior().VoidResult(this,
          "doGlobalPostLocalUpdateCache");
    }
  }

  /**
   * Sets the stub of ReplaceMappingsOperation.doGlobalPreLocalExecute(IStoreTransactionScope ts)
   */
  @Override
  public StoreResults doGlobalPreLocalExecute(IStoreTransactionScope ts) {
    Func1Param<IStoreTransactionScope, StoreResults> func1 = (IStoreTransactionScope arg) -> DoGlobalPreLocalExecuteIStoreTransactionScope
        .invoke(arg);
    if (func1 != null) {
      return func1.invoke(ts);
    }
    if (this.___callBase) {
      return super.doGlobalPreLocalExecute(ts);
    }
    return this.getInstanceBehavior().Result(this,
        "doGlobalPreLocalExecute");
  }

  /**
   * Sets the stub of ReplaceMappingsOperation.doLocalSourceExecute(IStoreTransactionScope ts)
   */
  @Override
  public StoreResults doLocalSourceExecute(IStoreTransactionScope ts) {
    Func1Param<IStoreTransactionScope, StoreResults> func1 = (IStoreTransactionScope arg) -> DoLocalSourceExecuteIStoreTransactionScope
        .invoke(arg);
    if (func1 != null) {
      return func1.invoke(ts);
    }
    if (this.___callBase) {
      return super.doLocalSourceExecute(ts);
    }
    return this.getInstanceBehavior().Result(this,
        "doLocalSourceExecute");
  }

  /**
   * Sets the stub of StoreOperation.doLocalTargetExecute(IStoreTransactionScope ts)
   */
  @Override
  public StoreResults doLocalTargetExecute(IStoreTransactionScope ts) {
    Func1Param<IStoreTransactionScope, StoreResults> func1 = (IStoreTransactionScope arg) -> DoLocalTargetExecuteIStoreTransactionScope
        .invoke(arg);
    if (func1 != null) {
      return func1.invoke(ts);
    }
    if (this.___callBase) {
      return super.doLocalTargetExecute(ts);
    }
    return this.getInstanceBehavior().Result(this,
        "doLocalTargetExecute");
  }

  /**
   * Sets the stub of ReplaceMappingsOperation.getStoreConnectionInfo()
   */
  @Override
  public StoreConnectionInfo getStoreConnectionInfo() {
    Func0Param<StoreConnectionInfo> func1 = () -> GetStoreConnectionInfo01.invoke();
    if (func1 != null) {
      return func1.invoke();
    }
    if (this.___callBase) {
      return super.getStoreConnectionInfo();
    }
    return this.getInstanceBehavior().Result(
        this, "getStoreConnectionInfo");
  }

  /**
   * Sets the stub of ReplaceMappingsOperation.handleDoGlobalPostLocalExecuteError(StoreResults
   * result)
   */
  @Override
  public void handleDoGlobalPostLocalExecuteError(StoreResults result) {
    Action1Param<StoreResults> action1 = (StoreResults obj) -> HandleDoGlobalPostLocalExecuteErrorIStoreResults
        .invoke(obj);
    if (action1 != null) {
      action1.invoke(result);
    } else if (this.___callBase) {
      super.handleDoGlobalPostLocalExecuteError(result);
    } else {
      this.getInstanceBehavior().VoidResult(this,
          "handleDoGlobalPostLocalExecuteError");
    }
  }

  /**
   * Sets the stub of ReplaceMappingsOperation.handleDoGlobalPreLocalExecuteError(StoreResults
   * result)
   */
  @Override
  public void handleDoGlobalPreLocalExecuteError(StoreResults result) {
    Action1Param<StoreResults> action1 = (StoreResults obj) -> HandleDoGlobalPreLocalExecuteErrorIStoreResults
        .invoke(obj);
    if (action1 != null) {
      action1.invoke(result);
    } else if (this.___callBase) {
      super.handleDoGlobalPreLocalExecuteError(result);
    } else {
      this.getInstanceBehavior().VoidResult(this,
          "handleDoGlobalPreLocalExecuteError");
    }
  }

  /**
   * Sets the stub of ReplaceMappingsOperation.handleDoLocalSourceExecuteError(StoreResults result)
   */
  @Override
  public void handleDoLocalSourceExecuteError(StoreResults result) {
    Action1Param<StoreResults> action1 = (StoreResults obj) -> HandleDoLocalSourceExecuteErrorIStoreResults
        .invoke(obj);
    if (action1 != null) {
      action1.invoke(result);
    } else if (this.___callBase) {
      super.handleDoLocalSourceExecuteError(result);
    } else {
      this.getInstanceBehavior().VoidResult(this,
          "handleDoLocalSourceExecuteError");
    }
  }

  /**
   * Sets the stub of StoreOperation.handleDoLocalTargetExecuteError(StoreResults result)
   */
  @Override
  public void handleDoLocalTargetExecuteError(StoreResults result) {
    Action1Param<StoreResults> action1 = (StoreResults obj) -> HandleDoLocalTargetExecuteErrorIStoreResults
        .invoke(obj);
    if (action1 != null) {
      action1.invoke(result);
    } else if (this.___callBase) {
      super.handleDoLocalTargetExecuteError(result);
    } else {
      this.getInstanceBehavior().VoidResult(this,
          "handleDoLocalTargetExecuteError");
    }
  }

  /**
   * Sets the stub of ReplaceMappingsOperation.handleUndoGlobalPostLocalExecuteError(StoreResults
   * result)
   */
  @Override
  public void handleUndoGlobalPostLocalExecuteError(StoreResults result) {
    Action1Param<StoreResults> action1 = (StoreResults obj) -> HandleUndoGlobalPostLocalExecuteErrorIStoreResults
        .invoke(obj);
    if (action1 != null) {
      action1.invoke(result);
    } else if (this.___callBase) {
      super.handleUndoGlobalPostLocalExecuteError(result);
    } else {
      this.getInstanceBehavior().VoidResult(this,
          "handleUndoGlobalPostLocalExecuteError");
    }
  }

  /**
   * Sets the stub of StoreOperation.handleUndoGlobalPreLocalExecuteError(StoreResults result)
   */
  @Override
  public void handleUndoGlobalPreLocalExecuteError(StoreResults result) {
    Action1Param<StoreResults> action1 = (StoreResults obj) -> HandleUndoGlobalPreLocalExecuteErrorIStoreResults
        .invoke(obj);
    if (action1 != null) {
      action1.invoke(result);
    } else if (this.___callBase) {
      super.handleUndoGlobalPreLocalExecuteError(result);
    } else {
      this.getInstanceBehavior().VoidResult(this,
          "handleUndoGlobalPreLocalExecuteError");
    }
  }

  /**
   * Sets the stub of ReplaceMappingsOperation.handleUndoLocalSourceExecuteError(StoreResults
   * result)
   */
  @Override
  public void handleUndoLocalSourceExecuteError(StoreResults result) {
    Action1Param<StoreResults> action1 = (StoreResults obj) -> HandleUndoLocalSourceExecuteErrorIStoreResults
        .invoke(obj);
    if (action1 != null) {
      action1.invoke(result);
    } else if (this.___callBase) {
      super.handleUndoLocalSourceExecuteError(result);
    } else {
      this.getInstanceBehavior().VoidResult(this,
          "handleUndoLocalSourceExecuteError");
    }
  }

  /**
   * Sets the stub of StoreOperation.handleUndoLocalTargetExecuteError(StoreResults result)
   */
  @Override
  public void handleUndoLocalTargetExecuteError(StoreResults result) {
    Action1Param<StoreResults> action1 = (StoreResults obj) -> HandleUndoLocalTargetExecuteErrorIStoreResults
        .invoke(obj);
    if (action1 != null) {
      action1.invoke(result);
    } else if (this.___callBase) {
      super.handleUndoLocalTargetExecuteError(result);
    } else {
      this.getInstanceBehavior().VoidResult(this,
          "handleUndoLocalTargetExecuteError");
    }
  }

  /**
   * Initializes a new instance of type StubReplaceMappingsOperation
   */
  private void InitializeStub() {
  }

  /**
   * Sets the stub of StoreOperation.onStoreException(StoreException se, StoreOperationState state)
   */
  @Override
  public ShardManagementException onStoreException(StoreException se, StoreOperationState state) {
    Func2Param<StoreException, StoreOperationState, ShardManagementException> func1 = (StoreException arg1, StoreOperationState arg2) -> OnStoreExceptionStoreExceptionStoreOperationState
        .invoke(arg1, arg2);
    if (func1 != null) {
      return func1.invoke(se, state);
    }
    if (this.___callBase) {
      return super.onStoreException(se, state);
    }
    return this
        .getInstanceBehavior().Result(this,
            "onStoreException");
  }

  /**
   * Sets the stub of ReplaceMappingsOperation.undoGlobalPostLocalExecute(IStoreTransactionScope ts)
   */
  @Override
  public StoreResults undoGlobalPostLocalExecute(IStoreTransactionScope ts) {
    Func1Param<IStoreTransactionScope, StoreResults> func1 = (IStoreTransactionScope arg) -> UndoGlobalPostLocalExecuteIStoreTransactionScope
        .invoke(arg);
    if (func1 != null) {
      return func1.invoke(ts);
    }
    if (this.___callBase) {
      return super.undoGlobalPostLocalExecute(ts);
    }
    return this.getInstanceBehavior().Result(this,
        "undoGlobalPostLocalExecute");
  }

  /**
   * Sets the stub of StoreOperation.undoGlobalPreLocalExecute(IStoreTransactionScope ts)
   */
  @Override
  public StoreResults undoGlobalPreLocalExecute(IStoreTransactionScope ts) {
    Func1Param<IStoreTransactionScope, StoreResults> func1 = (IStoreTransactionScope arg) -> UndoGlobalPreLocalExecuteIStoreTransactionScope
        .invoke(arg);
    if (func1 != null) {
      return func1.invoke(ts);
    }
    if (this.___callBase) {
      return super.undoGlobalPreLocalExecute(ts);
    }
    return this.getInstanceBehavior().Result(this,
        "undoGlobalPreLocalExecute");
  }

  /**
   * Sets the stub of ReplaceMappingsOperation.undoLocalSourceExecute(IStoreTransactionScope ts)
   */
  @Override
  public StoreResults undoLocalSourceExecute(IStoreTransactionScope ts) {
    Func1Param<IStoreTransactionScope, StoreResults> func1 = (IStoreTransactionScope arg) -> UndoLocalSourceExecuteIStoreTransactionScope
        .invoke(arg);
    if (func1 != null) {
      return func1.invoke(ts);
    }
    if (this.___callBase) {
      return super.undoLocalSourceExecute(ts);
    }
    return this.getInstanceBehavior().Result(this,
        "undoLocalSourceExecute");
  }

  /**
   * Sets the stub of StoreOperation.undoLocalTargetExecute(IStoreTransactionScope ts)
   */
  @Override
  public StoreResults undoLocalTargetExecute(IStoreTransactionScope ts) {
    Func1Param<IStoreTransactionScope, StoreResults> func1 = (IStoreTransactionScope arg) -> UndoLocalTargetExecuteIStoreTransactionScope
        .invoke(arg);
    if (func1 != null) {
      return func1.invoke(ts);
    }
    if (this.___callBase) {
      return super.undoLocalTargetExecute(ts);
    }
    return this.getInstanceBehavior().Result(this,
        "undoLocalTargetExecute");
  }
}
