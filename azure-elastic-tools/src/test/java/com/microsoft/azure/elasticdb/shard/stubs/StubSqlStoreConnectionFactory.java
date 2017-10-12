package com.microsoft.azure.elasticdb.shard.stubs;

/*
 * Copyright (c) Microsoft. All rights reserved. Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

import com.microsoft.azure.elasticdb.shard.sqlstore.SqlStoreConnectionFactory;
import com.microsoft.azure.elasticdb.shard.store.IStoreConnection;
import com.microsoft.azure.elasticdb.shard.store.IUserStoreConnection;
import com.microsoft.azure.elasticdb.shard.store.StoreConnectionKind;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func1Param;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func2Param;

/**
 * Stub type of SqlStoreConnectionFactory.
 */
public class StubSqlStoreConnectionFactory extends SqlStoreConnectionFactory {

    /**
     * Sets the stub of SqlStoreConnectionFactory.getConnection(StoreConnectionKind kind, String connectionString)
     */
    public Func2Param<StoreConnectionKind, String, IStoreConnection> getConnectionStoreConnectionKindString;
    /**
     * Sets the stub of SqlStoreConnectionFactory.getUserConnection(String connectionString)
     */
    public Func1Param<String, IUserStoreConnection> getUserConnectionString;

    private boolean callBase;
    private IStubBehavior instanceBehavior;

    /**
     * Initializes a new instance.
     */
    public StubSqlStoreConnectionFactory() {
        this.initializeStub();
    }

    /**
     * Gets or sets a value that indicates if the base method should be called instead of the fallback behavior.
     */
    public final boolean getCallBase() {
        return this.callBase;
    }

    public final void setCallBase(boolean value) {
        this.callBase = value;
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
     * Sets the stub of SqlStoreConnectionFactory.getConnection(StoreConnectionKind kind, String connectionString)
     */
    @Override
    public IStoreConnection getConnection(StoreConnectionKind kind,
            String connectionString) {
        Func2Param<StoreConnectionKind, String, IStoreConnection> func1 = (StoreConnectionKind arg1,
                String arg2) -> getConnectionStoreConnectionKindString.invoke(arg1, arg2);
        if (getConnectionStoreConnectionKindString != null) {
            return func1.invoke(kind, connectionString);
        }
        if (this.callBase) {
            return super.getConnection(kind, connectionString);
        }
        return this.getInstanceBehavior().result(this, "getConnection");
    }

    /**
     * Sets the stub of SqlStoreConnectionFactory.getUserConnection(String connectionString)
     */
    @Override
    public IUserStoreConnection getUserConnection(String connectionString) {
        Func1Param<String, IUserStoreConnection> func1 = (String arg) -> getUserConnectionString.invoke(arg);
        if (getUserConnectionString != null) {
            return func1.invoke(connectionString);
        }
        if (this.callBase) {
            return super.getUserConnection(connectionString);
        }
        return this.getInstanceBehavior().result(this, "getUserConnection");
    }

    /**
     * Initializes a new instance of type StubSqlStoreConnectionFactory.
     */
    private void initializeStub() {
    }
}