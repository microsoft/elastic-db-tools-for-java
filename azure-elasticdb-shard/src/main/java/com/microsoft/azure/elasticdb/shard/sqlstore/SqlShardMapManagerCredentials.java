package com.microsoft.azure.elasticdb.shard.sqlstore;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.helpers.ApplicationNameHelper;
import com.microsoft.azure.elasticdb.shard.map.ShardMapUtils;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import com.microsoft.azure.elasticdb.shard.utils.GlobalConstants;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;

/**
 * Container for the credentials for SQL Server backed ShardMapManager.
 */
public final class SqlShardMapManagerCredentials {

  /**
   * Connection string for shard map manager database.
   */
  private SqlConnectionStringBuilder connectionStringShardMapManager;

  /**
   * Connection string for individual shards.
   */
  private SqlConnectionStringBuilder connectionStringShard;

  /**
   * Instantiates the object that holds the credentials for accessing SQL Servers
   * containing the shard map manager data.
   *
   * @param connectionString Connection string for ShardId map manager data source.
   */
  public SqlShardMapManagerCredentials(String connectionString) {
    ExceptionUtils.disallowNullArgument(connectionString, "connectionString");

    // Devnote: If connection string specifies Active Directory authentication and runtime is not
    // .NET 4.6 or higher, then below call will throw.
    SqlConnectionStringBuilder connectionStringBuilder = new SqlConnectionStringBuilder(
        connectionString);

    ///#region GSM Validation

    // DataSource must be set.
    if (StringUtilsLocal.isNullOrEmpty(connectionStringBuilder.getDataSource())) {
      throw new IllegalArgumentException(StringUtilsLocal
          .formatInvariant(Errors._SqlShardMapManagerCredentials_ConnectionStringPropertyRequired,
              "DataSource"), new Throwable("connectionString"));
    }

    // DatabaseName must be set.
    if (StringUtilsLocal.isNullOrEmpty(connectionStringBuilder.getDatabaseName())) {
      throw new IllegalArgumentException(StringUtilsLocal
          .formatInvariant(Errors._SqlShardMapManagerCredentials_ConnectionStringPropertyRequired,
              "Initial Catalog"), new Throwable("connectionString"));
    }

    // Ensure credentials are specified for GSM connectivity.
    SqlShardMapManagerCredentials.ensureCredentials(connectionStringBuilder, "connectionString");

    ///#endregion GSM Validation

    // Copy the input connection strings.
    connectionStringShardMapManager = new SqlConnectionStringBuilder(
        connectionStringBuilder.getConnectionString());

    connectionStringShardMapManager.setApplicationName(ApplicationNameHelper
        .addApplicationNameSuffix(connectionStringShardMapManager.getApplicationName(),
            GlobalConstants.ShardMapManagerInternalConnectionSuffixGlobal));

    connectionStringShard = new SqlConnectionStringBuilder(
        connectionStringBuilder.getConnectionString());

    connectionStringShard.remove("Data Source");
    connectionStringShard.remove("Initial Catalog");

    connectionStringShard.setApplicationName(ApplicationNameHelper
        .addApplicationNameSuffix(connectionStringShard.getApplicationName(),
            GlobalConstants.ShardMapManagerInternalConnectionSuffixLocal));
  }

  /**
   * Ensures that credentials are provided for the given connection string object.
   *
   * @param connectionString Input connection string object.
   * @param parameterName Parameter name of the connection string object.
   */
  public static void ensureCredentials(SqlConnectionStringBuilder connectionString,
      String parameterName) {
    // Check for integrated authentication
    if (connectionString.getIntegratedSecurity()) {
      return;
    }

    //TODO: Check for active directory integrated authentication (if supported)
    if (connectionString.containsKey(ShardMapUtils.Authentication)
        && connectionString.getItem(ShardMapUtils.Authentication).toString()
        .equals(ShardMapUtils.ActiveDirectoryIntegratedStr)) {
      return;
    }

    // UserID must be set when integrated authentication is disabled.
    if (StringUtilsLocal.isNullOrEmpty(connectionString.getUser())) {
      throw new IllegalArgumentException(StringUtilsLocal
          .formatInvariant(Errors._SqlShardMapManagerCredentials_ConnectionStringPropertyRequired,
              "UserID"), new Throwable(parameterName));
    }

    // Password must be set when integrated authentication is disabled.
    if (StringUtilsLocal.isNullOrEmpty(connectionString.getPassword())) {
      throw new IllegalArgumentException(StringUtilsLocal
          .formatInvariant(Errors._SqlShardMapManagerCredentials_ConnectionStringPropertyRequired,
              "Password"), new Throwable(parameterName));
    }
  }

  /**
   * Connection string for shard map manager database.
   */
  public String getConnectionStringShardMapManager() {
    return connectionStringShardMapManager.getConnectionString();
  }

  /**
   * Connection string for shards.
   */
  public String getConnectionStringShard() {
    return connectionStringShard.getConnectionString();
  }

  /**
   * Location of ShardId Map shardMapManager used for logging purpose.
   */
  public String getShardMapManagerLocation() {
    return StringUtilsLocal.formatInvariant("[DataSource=%1$s Database=%2$s]",
        connectionStringShardMapManager.getDataSource(),
        connectionStringShardMapManager.getDatabaseName());
  }
}