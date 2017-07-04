package com.microsoft.azure.elasticdb.shard.sqlstore;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.google.common.base.Preconditions;
import com.microsoft.azure.elasticdb.shard.store.IStoreConnection;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreConnectionKind;
import com.microsoft.azure.elasticdb.shard.store.StoreException;
import com.microsoft.azure.elasticdb.shard.store.StoreTransactionScopeKind;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Instance of a Sql Store Connection.
 */
public class SqlStoreConnection implements IStoreConnection {

  /**
   * Underlying SQL connection object.
   */
  private Connection conn;
  /**
   * Type of store connection.
   */
  private StoreConnectionKind kind;

  /**
   * Constructs an instance of Sql Store Connection.
   *
   * @param kind Type of store connection.
   */
  public SqlStoreConnection(StoreConnectionKind kind, String connectionString) {
    this.kind = Preconditions.checkNotNull(kind);
    try {
      conn = DriverManager.getConnection(connectionString);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public StoreConnectionKind getKind() {
    return kind;
  }

  /**
   * Open the store connection.
   */
  public void open() {
    // Java doesn't have concept of opening connection. Remove this method.
  }

  /**
   * Asynchronously open the store connection.
   *
   * @return A task to await completion of the Open
   */
  public Callable openAsync() {
    // Java doesn't have concept of opening connection. Remove this method.
    return null;
  }

  /**
   * Open the store connection, and acquire a lock on the store.
   *
   * @param lockId Lock Id.
   */
  public void openWithLock(UUID lockId) {
    /*SqlUtils.WithSqlExceptionHandling(() -> {
      conn.Open();
      this.GetAppLock(lockId);
    });*/
  }

  /**
   * Closes the store connection.
   */
  public void close() {
    if (conn != null) {
      try {
        conn.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
      conn = null;
    }
  }

  /**
   * Closes the store connection after releasing lock.
   *
   * @param lockId Lock Id.
   */
  public void closeWithUnlock(UUID lockId) {
    try {
      if (conn != null && !conn.isClosed()) {
        this.releaseAppLock(lockId);
        close();
      }
    } catch (SQLException e) {
      throw new StoreException(Errors._Store_StoreException, e);
    }
  }

  /**
   * Acquires a transactional scope on the connection.
   *
   * @param kind Type of transaction scope.
   * @return Transaction scope on the store connection.
   */
  @Override
  public IStoreTransactionScope getTransactionScope(StoreTransactionScopeKind kind) {
    return new SqlStoreTransactionScope(kind, conn);
  }

  /**
   * Acquires an application level lock on the connection which is session scoped.
   *
   * @param lockId Identity of the lock.
   */
  private void getAppLock(UUID lockId) {
    /*try (SqlCommand cmdGetAppLock = conn.CreateCommand()) {
      cmdGetAppLock.CommandText = "sp_getapplock";
      cmdGetAppLock.CommandType = CommandType.StoredProcedure;

      SqlUtils.AddCommandParameter(cmdGetAppLock, "@Resource", SqlDbType.NVarChar,
          ParameterDirection.Input, 255 * 2, lockId.toString());

      SqlUtils.AddCommandParameter(cmdGetAppLock, "@LockMode", SqlDbType.NVarChar,
          ParameterDirection.Input, 32 * 2, "Exclusive");

      SqlUtils.AddCommandParameter(cmdGetAppLock, "@LockOwner", SqlDbType.NVarChar,
          ParameterDirection.Input, 32 * 2, "Session");

      SqlUtils.AddCommandParameter(cmdGetAppLock, "@LockTimeout", SqlDbType.Int,
          ParameterDirection.Input, 0, GlobalConstants.DefaultLockTimeOut);

      SqlParameter returnValue = SqlUtils
          .AddCommandParameter(cmdGetAppLock, "@RETURN_VALUE", SqlDbType.Int,
              ParameterDirection.ReturnValue, 0, 0);

      cmdGetAppLock.ExecuteNonQuery();

      // If time-out or other errors happen.
      if ((int) returnValue.Value < 0) {
        throw new ShardManagementException(ShardManagementErrorCategory.General,
            ShardManagementErrorCode.LockNotAcquired, Errors._Store_SqlOperation_LockNotAcquired,
            lockId);
      }
    }*/
  }

  /**
   * Releases an application level lock on the connection which is session scoped.
   *
   * @param lockId Identity of the lock.
   */
  private void releaseAppLock(UUID lockId) {
    /*try (SqlCommand cmdReleaseAppLock = conn.CreateCommand()) {
      cmdReleaseAppLock.CommandText = "sp_releaseapplock";
      cmdReleaseAppLock.CommandType = CommandType.StoredProcedure;

      SqlUtils.AddCommandParameter(cmdReleaseAppLock, "@Resource", SqlDbType.NVarChar,
          ParameterDirection.Input, 255 * 2, lockId.toString());

      SqlUtils.AddCommandParameter(cmdReleaseAppLock, "@LockOwner", SqlDbType.NVarChar,
          ParameterDirection.Input, 32 * 2, "Session");

      SqlParameter returnValue = SqlUtils
          .AddCommandParameter(cmdReleaseAppLock, "@RETURN_VALUE", SqlDbType.Int,
              ParameterDirection.ReturnValue, 0, 0);

      try {
        cmdReleaseAppLock.ExecuteNonQuery();
      } catch (RuntimeException e) {
        // ignore all exceptions.
        return;
      }

      // If parameter validation or other errors happen.
      if ((int) returnValue.Value < 0) {
        throw new ShardManagementException(ShardManagementErrorCategory.General,
            ShardManagementErrorCode.LockNotReleased, Errors._Store_SqlOperation_LockNotReleased,
            lockId);
      }
    }*/
  }

}
