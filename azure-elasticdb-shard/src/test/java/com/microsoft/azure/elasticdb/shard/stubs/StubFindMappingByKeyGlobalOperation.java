package com.microsoft.azure.elasticdb.shard.stubs;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.cache.CacheStoreMappingUpdatePolicy;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreException;
import com.microsoft.azure.elasticdb.shard.store.StoreLogEntry;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.storeops.mapper.FindMappingByKeyGlobalOperation;
import com.microsoft.azure.elasticdb.shard.stubhelper.Action1Param;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func0Param;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func1Param;
import java.util.concurrent.Callable;

/**
 * Stub type of Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.FindMappingByKeyGlobalOperation
 */
public class StubFindMappingByKeyGlobalOperation extends FindMappingByKeyGlobalOperation {

  /**
   * Sets the stub of StoreOperationGlobal.dispose(Boolean disposing)
   */
  public Action1Param<Boolean> DisposeBoolean;
  /**
   * Sets the stub of FindMappingByKeyGlobalOperation.doGlobalExecuteAsync(IStoreTransactionScope
   * ts)
   */
  public Func1Param<IStoreTransactionScope, Callable<StoreResults>> DoGlobalExecuteAsyncIStoreTransactionScope;
  /**
   * Sets the stub of FindMappingByKeyGlobalOperation.doGlobalExecute(IStoreTransactionScope ts)
   */
  public Func1Param<IStoreTransactionScope, StoreResults> DoGlobalExecuteIStoreTransactionScope;
  /**
   * Sets the stub of FindMappingByKeyGlobalOperation.doGlobalUpdateCachePost(StoreResults result)
   */
  public Action1Param<StoreResults> DoGlobalUpdateCachePostIStoreResults;
  /**
   * Sets the stub of FindMappingByKeyGlobalOperation.doGlobalUpdateCachePre(StoreResults result)
   */
  public Action1Param<StoreResults> DoGlobalUpdateCachePreIStoreResults;
  /**
   * Sets the stub of FindMappingByKeyGlobalOperation.get_ErrorCategory()
   */
  public Func0Param<ShardManagementErrorCategory> ErrorCategoryGet;
  /**
   * Sets the stub of FindMappingByKeyGlobalOperation.handleDoGlobalExecuteError(StoreResults
   * result)
   */
  public Action1Param<StoreResults> HandleDoGlobalExecuteErrorIStoreResults;
  /**
   * Sets the stub of StoreOperationGlobal.onStoreException(StoreException se)
   */
  public Func1Param<StoreException, ShardManagementException> OnStoreExceptionStoreException;
  /**
   * Sets the stub of FindMappingByKeyGlobalOperation.get_ReadOnly()
   */
  public Func0Param<Boolean> ReadOnlyGet;
  /**
   * Sets the stub of StoreOperationGlobal.undoPendingStoreOperationsAsync(StoreLogEntry logEntry)
   */
  public Func1Param<StoreLogEntry, Callable> UndoPendingStoreOperationsAsyncIStoreLogEntry;
  /**
   * Sets the stub of StoreOperationGlobal.undoPendingStoreOperations(StoreLogEntry logEntry)
   */
  public Action1Param<StoreLogEntry> UndoPendingStoreOperationsIStoreLogEntry;
  private boolean ___callBase;
  private IStubBehavior ___instanceBehavior;

  /**
   * Initializes a new instance
   */
  public StubFindMappingByKeyGlobalOperation(ShardMapManager shardMapManager, String operationName,
      StoreShardMap shardMap, ShardKey key, CacheStoreMappingUpdatePolicy policy,
      ShardManagementErrorCategory errorCategory, boolean cacheResults, boolean ignoreFailure) {
    super(shardMapManager, operationName, shardMap, key, policy, errorCategory, cacheResults,
        ignoreFailure);
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
   * Sets the stub of FindMappingByKeyGlobalOperation.get_ErrorCategory()
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
   * Gets or sets the instance behavior.
   */
  public final IStubBehavior getInstanceBehavior() {
    return StubBehaviors.GetValueOrCurrent(this.___instanceBehavior);
  }

  public final void setInstanceBehavior(IStubBehavior value) {
    this.___instanceBehavior = value;
  }

  /**
   * Sets the stub of FindMappingByKeyGlobalOperation.get_ReadOnly()
   */
  @Override
  public boolean getReadOnly() {
    Func0Param<Boolean> func1 = () -> ReadOnlyGet.invoke();
    if (func1 != null) {
      return func1.invoke();
    }
    if (this.___callBase) {
      return super.getReadOnly();
    }
    return this.getInstanceBehavior().<StubFindMappingByKeyGlobalOperation, Boolean>Result(this,
        "get_ReadOnly");
  }

  /**
   * Sets the stub of StoreOperationGlobal.dispose(Boolean disposing)
   */
  @Override
  protected void dispose(boolean disposing) {
    Action1Param<Boolean> action1 = (Boolean obj) -> DisposeBoolean.invoke(obj);
    if (action1 != null) {
      action1.invoke(disposing);
    } else if (this.___callBase) {
      super.dispose(disposing);
    } else {
      this.getInstanceBehavior().VoidResult(this, "dispose");
    }
  }

  /**
   * Sets the stub of FindMappingByKeyGlobalOperation.doGlobalExecute(IStoreTransactionScope ts)
   */
  @Override
  public StoreResults doGlobalExecute(IStoreTransactionScope ts) {
    Func1Param<IStoreTransactionScope, StoreResults> func1 = (IStoreTransactionScope arg) -> DoGlobalExecuteIStoreTransactionScope
        .invoke(arg);
    if (func1 != null) {
      return func1.invoke(ts);
    }
    if (this.___callBase) {
      return super.doGlobalExecute(ts);
    }
    return this.getInstanceBehavior().Result(
        this, "doGlobalExecute");
  }

  /**
   * Sets the stub of FindMappingByKeyGlobalOperation.doGlobalExecuteAsync(IStoreTransactionScope
   * ts)
   */
  @Override
  public Callable<StoreResults> doGlobalExecuteAsync(IStoreTransactionScope ts) {
    Func1Param<IStoreTransactionScope, Callable<StoreResults>> func1 = (IStoreTransactionScope arg) -> DoGlobalExecuteAsyncIStoreTransactionScope
        .invoke(arg);
    if (func1 != null) {
      return func1.invoke(ts);
    }
    if (this.___callBase) {
      return super.doGlobalExecuteAsync(ts);
    }
    return this
        .getInstanceBehavior().Result(
            this,
            "doGlobalExecuteAsync");
  }

  /**
   * Sets the stub of FindMappingByKeyGlobalOperation.doGlobalUpdateCachePost(StoreResults result)
   */
  @Override
  public void doGlobalUpdateCachePost(StoreResults result) {
    Action1Param<StoreResults> action1 = (StoreResults obj) -> DoGlobalUpdateCachePostIStoreResults
        .invoke(obj);
    if (action1 != null) {
      action1.invoke(result);
    } else if (this.___callBase) {
      super.doGlobalUpdateCachePost(result);
    } else {
      this.getInstanceBehavior().VoidResult(this,
          "doGlobalUpdateCachePost");
    }
  }

  /**
   * Sets the stub of FindMappingByKeyGlobalOperation.doGlobalUpdateCachePre(StoreResults result)
   */
  @Override
  public void doGlobalUpdateCachePre(StoreResults result) {
    Action1Param<StoreResults> action1 = (StoreResults obj) -> DoGlobalUpdateCachePreIStoreResults
        .invoke(obj);
    if (action1 != null) {
      action1.invoke(result);
    } else if (this.___callBase) {
      super.doGlobalUpdateCachePre(result);
    } else {
      this.getInstanceBehavior().VoidResult(this,
          "doGlobalUpdateCachePre");
    }
  }

  /**
   * Sets the stub of FindMappingByKeyGlobalOperation.handleDoGlobalExecuteError(StoreResults
   * result)
   */
  @Override
  public void handleDoGlobalExecuteError(StoreResults result) {
    Action1Param<StoreResults> action1 = (StoreResults obj) -> HandleDoGlobalExecuteErrorIStoreResults
        .invoke(obj);
    if (action1 != null) {
      action1.invoke(result);
    } else if (this.___callBase) {
      super.handleDoGlobalExecuteError(result);
    } else {
      this.getInstanceBehavior().VoidResult(this,
          "handleDoGlobalExecuteError");
    }
  }

  /**
   * Initializes a new instance of type StubFindMappingByKeyGlobalOperation
   */
  private void InitializeStub() {
  }

  /**
   * Sets the stub of StoreOperationGlobal.onStoreException(StoreException se)
   */
  @Override
  public ShardManagementException onStoreException(StoreException se) {
    Func1Param<StoreException, ShardManagementException> func1 = (StoreException arg) -> OnStoreExceptionStoreException
        .invoke(arg);
    if (func1 != null) {
      return func1.invoke(se);
    }
    if (this.___callBase) {
      return super.onStoreException(se);
    }
    return this
        .getInstanceBehavior().Result(
            this, "onStoreException");
  }

  /**
   * Sets the stub of StoreOperationGlobal.undoPendingStoreOperations(StoreLogEntry logEntry)
   */
  @Override
  protected void undoPendingStoreOperations(StoreLogEntry logEntry) throws Exception {
    Action1Param<StoreLogEntry> action1 = (StoreLogEntry obj) -> UndoPendingStoreOperationsIStoreLogEntry
        .invoke(obj);
    if (action1 != null) {
      action1.invoke(logEntry);
    } else if (this.___callBase) {
      super.undoPendingStoreOperations(logEntry);
    } else {
      this.getInstanceBehavior().VoidResult(this,
          "undoPendingStoreOperations");
    }
  }

  /**
   * Sets the stub of StoreOperationGlobal.undoPendingStoreOperationsAsync(StoreLogEntry logEntry)
   */
  @Override
  protected Callable undoPendingStoreOperationsAsync(StoreLogEntry logEntry) {
    Func1Param<StoreLogEntry, Callable> func1 = (StoreLogEntry arg) -> UndoPendingStoreOperationsAsyncIStoreLogEntry
        .invoke(arg);
    if (func1 != null) {
      return func1.invoke(logEntry);
    }
    if (this.___callBase) {
      return super.undoPendingStoreOperationsAsync(logEntry);
    }
    return this.getInstanceBehavior().Result(this,
        "undoPendingStoreOperationsAsync");
  }
}
