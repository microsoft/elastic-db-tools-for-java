package com.microsoft.azure.elasticdb.shard.stubs;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.sqlstore.SqlStoreConnectionFactory;
import com.microsoft.azure.elasticdb.shard.store.IStoreConnection;
import com.microsoft.azure.elasticdb.shard.store.IUserStoreConnection;
import com.microsoft.azure.elasticdb.shard.store.StoreConnectionKind;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func1Param;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func2Param;

/**
 * Stub type of Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.SqlStoreConnectionFactory
 */
public class StubSqlStoreConnectionFactory extends SqlStoreConnectionFactory {

  /**
   * Sets the stub of SqlStoreConnectionFactory.getConnection(StoreConnectionKind kind, String
   * connectionString)
   */
  public Func2Param<StoreConnectionKind, String, IStoreConnection> GetConnectionStoreConnectionKindString;
  /**
   * Sets the stub of SqlStoreConnectionFactory.getUserConnection(String connectionString)
   */
  public Func1Param<String, IUserStoreConnection> GetUserConnectionString;
  private boolean ___callBase;
  private IStubBehavior ___instanceBehavior;

  /**
   * Initializes a new instance
   */
  public StubSqlStoreConnectionFactory() {
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
   * Gets or sets the instance behavior.
   */
  public final IStubBehavior getInstanceBehavior() {
    return StubBehaviors.GetValueOrCurrent(this.___instanceBehavior);
  }

  public final void setInstanceBehavior(IStubBehavior value) {
    this.___instanceBehavior = value;
  }

  /**
   * Sets the stub of SqlStoreConnectionFactory.getConnection(StoreConnectionKind kind, String
   * connectionString)
   */
  @Override
  public IStoreConnection getConnection(StoreConnectionKind kind, String connectionString) {
    Func2Param<StoreConnectionKind, String, IStoreConnection> func1 = (StoreConnectionKind arg1, String arg2) -> GetConnectionStoreConnectionKindString
        .invoke(arg1, arg2);
    if (func1 != null) {
      return func1.invoke(kind, connectionString);
    }
    if (this.___callBase) {
      return super.getConnection(kind, connectionString);
    }
    return this.getInstanceBehavior().Result(this,
        "getConnection");
  }

  /**
   * Sets the stub of SqlStoreConnectionFactory.getUserConnection(String connectionString)
   */
  @Override
  public IUserStoreConnection getUserConnection(String connectionString) {
    Func1Param<String, IUserStoreConnection> func1 = (String arg) -> GetUserConnectionString
        .invoke(arg);
    if (func1 != null) {
      return func1.invoke(connectionString);
    }
    if (this.___callBase) {
      return super.getUserConnection(connectionString);
    }
    return this.getInstanceBehavior().Result(
        this, "getUserConnection");
  }

  /**
   * Initializes a new instance of type StubSqlStoreConnectionFactory
   */
  private void InitializeStub() {
  }
}