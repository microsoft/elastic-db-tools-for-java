package com.microsoft.azure.elasticdb.shard.sqlstore;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.helpers.ApplicationNameHelper;
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
    private SqlConnectionStringBuilder _connectionStringShardMapManager;

    /**
     * Connection string for individual shards.
     */
    private SqlConnectionStringBuilder _connectionStringShard;

    /**
     * Instantiates the object that holds the credentials for accessing SQL Servers
     * containing the shard map manager data.
     *
     * @param connectionString Connection string for ShardId map manager data source.
     */
    public SqlShardMapManagerCredentials(String connectionString) {
        ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");

        // Devnote: If connection string specifies Active Directory authentication and runtime is not
        // .NET 4.6 or higher, then below call will throw.
        SqlConnectionStringBuilder connectionStringBuilder = new SqlConnectionStringBuilder(connectionString);

        ///#region GSM Validation

        // DataSource must be set.
        if (StringUtilsLocal.isNullOrEmpty(connectionStringBuilder.getDataSource())) {
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._SqlShardMapManagerCredentials_ConnectionStringPropertyRequired, "DataSource"), new Throwable("connectionString"));
        }

        // DatabaseName must be set.
        if (StringUtilsLocal.isNullOrEmpty(connectionStringBuilder.getDatabaseName())) {
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._SqlShardMapManagerCredentials_ConnectionStringPropertyRequired, "Initial Catalog"), new Throwable("connectionString"));
        }

        // Ensure credentials are specified for GSM connectivity.
        SqlShardMapManagerCredentials.EnsureCredentials(connectionStringBuilder, "connectionString");

        ///#endregion GSM Validation

        // Copy the input connection strings.
        _connectionStringShardMapManager = new SqlConnectionStringBuilder(connectionStringBuilder.getConnectionString());

        _connectionStringShardMapManager.setApplicationName(ApplicationNameHelper.AddApplicationNameSuffix(_connectionStringShardMapManager.getApplicationName(), GlobalConstants.ShardMapManagerInternalConnectionSuffixGlobal));

        _connectionStringShard = new SqlConnectionStringBuilder(connectionStringBuilder.getConnectionString());

        _connectionStringShard.Remove("Data Source");
        _connectionStringShard.Remove("Initial Catalog");

        _connectionStringShard.setApplicationName(ApplicationNameHelper.AddApplicationNameSuffix(_connectionStringShard.getApplicationName(), GlobalConstants.ShardMapManagerInternalConnectionSuffixLocal));
    }

    /**
     * Ensures that credentials are provided for the given connection string object.
     *
     * @param connectionString Input connection string object.
     * @param parameterName    Parameter name of the connection string object.
     */
    public static void EnsureCredentials(SqlConnectionStringBuilder connectionString, String parameterName) {
        // Check for integrated authentication
        if (connectionString.getIntegratedSecurity()) {
            return;
        }

        //TODO: Check for active directory integrated authentication (if supported)
        /*if (connectionString.ContainsKey(ShardMapUtils.Authentication) && connectionString[ShardMapUtils.Authentication].toString().equals(ShardMapUtils.ActiveDirectoryIntegratedStr)) {
            return;
        }*/

        // UserID must be set when integrated authentication is disabled.
        if (StringUtilsLocal.isNullOrEmpty(connectionString.getUser())) {
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._SqlShardMapManagerCredentials_ConnectionStringPropertyRequired, "UserID"), new Throwable(parameterName));
        }

        // Password must be set when integrated authentication is disabled.
        if (StringUtilsLocal.isNullOrEmpty(connectionString.getPassword())) {
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._SqlShardMapManagerCredentials_ConnectionStringPropertyRequired, "Password"), new Throwable(parameterName));
        }
    }

    /**
     * Connection string for shard map manager database.
     */
    public String getConnectionStringShardMapManager() {
        return _connectionStringShardMapManager.getConnectionString();
    }

    /**
     * Connection string for shards.
     */
    public String getConnectionStringShard() {
        return _connectionStringShard.getConnectionString();
    }

    /**
     * Location of ShardId Map shardMapManager used for logging purpose.
     */
    public String getShardMapManagerLocation() {
        return StringUtilsLocal.FormatInvariant("[DataSource=%1$s Database=%2$s]", _connectionStringShardMapManager.getDataSource(), _connectionStringShardMapManager.getDatabaseName());
    }
}