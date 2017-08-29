package com.microsoft.azure.elasticdb.shard.sqlstore;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.google.common.base.Preconditions;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.store.IStoreConnection;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreConnectionKind;
import com.microsoft.azure.elasticdb.shard.store.StoreException;
import com.microsoft.azure.elasticdb.shard.store.StoreTransactionScopeKind;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.GlobalConstants;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

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
   * Open the store connection, and acquire a lock on the store.
   *
   * @param lockId Lock Id.
   */
  public void openWithLock(UUID lockId) {
    this.getAppLock(lockId);
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
    try (CallableStatement cmdGetAppLock = conn.prepareCall("{CALL sp_getapplock (@Resource=?, "
        + "@LockMode='Exclusive', @LockOwner='Session', @LockTimeout=?)}")) {
      cmdGetAppLock.setNString(1, lockId.toString());
      cmdGetAppLock.setNString(2, Integer.toString(GlobalConstants.DefaultLockTimeOut));

      boolean hasResult = cmdGetAppLock.execute();

      // If time-out or other errors happen.
      if (hasResult) {
        ResultSet rs = cmdGetAppLock.getResultSet();
        if (rs.next() && rs.getInt(1) < 0) {
          throw new ShardManagementException(ShardManagementErrorCategory.General,
              ShardManagementErrorCode.LockNotAcquired, Errors._Store_SqlOperation_LockNotAcquired,
              lockId);
        }
      }
    } catch (SQLException e) {
      throw new ShardManagementException(ShardManagementErrorCategory.General,
          ShardManagementErrorCode.LockNotAcquired, Errors._Store_SqlOperation_LockNotAcquired,
          lockId);
    }
  }

  /**
   * Releases an application level lock on the connection which is session scoped.
   *
   * @param lockId Identity of the lock.
   */
  private void releaseAppLock(UUID lockId) {
    try (CallableStatement cmdReleaseAppLock = conn.prepareCall("{CALL sp_releaseapplock "
        + "(@Resource=?, @LockOwner='Session')}")) {
      cmdReleaseAppLock.setNString(1, lockId.toString());

      boolean hasResult = cmdReleaseAppLock.execute();

      // If time-out or other errors happen.
      if (hasResult) {
        ResultSet rs = cmdReleaseAppLock.getResultSet();
        if (rs.next() && rs.getInt(1) < 0) {
          throw new ShardManagementException(ShardManagementErrorCategory.General,
              ShardManagementErrorCode.LockNotReleased, Errors._Store_SqlOperation_LockNotReleased,
              lockId);
        }
      }
    } catch (SQLException e) {
      // ignore all exceptions.
    }
  }
}
