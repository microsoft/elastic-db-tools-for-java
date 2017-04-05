package com.microsoft.azure.elasticdb.shard.sqlstore;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;

/**
 * Summary:
 * Provides a simple way to create and manage the contents of connection strings
 * used by the System.Data.SqlClient.SqlConnection class.
 */
public final class SqlConnectionStringBuilder {
    /**
     * Summary:
     * Gets or sets the name of the application associated with the connection string.
     * <p>
     * Returns:
     * The name of the application, or ".NET SqlClient Data Provider" if no name has
     * been supplied.
     * <p>
     * Exceptions:
     * T:System.ArgumentNullException:
     * To set the value to null, use System.DBNull.Value.
     */
    private String ApplicationName;

    /**
     * Summary:
     * Gets or sets the name or network address of the instance of SQL Server to connect
     * to.
     * <p>
     * Returns:
     * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.DataSource
     * property, or String.Empty if none has been supplied.
     * <p>
     * Exceptions:
     * T:System.ArgumentNullException:
     * To set the value to null, use System.DBNull.Value.
     */
    private String DataSource;

    /**
     * Summary:
     * Gets or sets the length of time (in seconds) to wait for a connection to the
     * server before terminating the attempt and generating an error.
     * <p>
     * Returns:
     * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.ConnectTimeout
     * property, or 15 seconds if no value has been supplied.
     */
    private int ConnectTimeout;

    /**
     * Summary:
     * Gets or sets the name of the database associated with the connection.
     * <p>
     * Returns:
     * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.InitialCatalog
     * property, or String.Empty if none has been supplied.
     * <p>
     * Exceptions:
     * T:System.ArgumentNullException:
     * To set the value to null, use System.DBNull.Value.
     */
    private String InitialCatalog;

    /**
     * Summary:
     * Gets or sets a Boolean value that indicates whether User ID and Password are
     * specified in the connection (when false) or whether the current Windows account
     * credentials are used for authentication (when true).
     * <p>
     * Returns:
     * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.IntegratedSecurity
     * property, or false if none has been supplied.
     */
    private boolean IntegratedSecurity;

    /**
     * Summary:
     * Gets or sets the password for the SQL Server account.
     * <p>
     * Returns:
     * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.Password property,
     * or String.Empty if none has been supplied.
     * <p>
     * Exceptions:
     * T:System.ArgumentNullException:
     * The password was incorrectly set to null. See code sample below.
     */
    private String Password;

    /**
     * Summary:
     * Gets or sets a Boolean value that indicates if security-sensitive information,
     * such as the password, is not returned as part of the connection if the connection
     * is open or has ever been in an open state.
     * <p>
     * Returns:
     * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.PersistSecurityInfo
     * property, or false if none has been supplied.
     */
    private boolean PersistSecurityInfo;

    /**
     * Summary:
     * Gets or sets the user ID to be used when connecting to SQL Server.
     * <p>
     * Returns:
     * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.User property,
     * or String.Empty if none has been supplied.
     * <p>
     * Exceptions:
     * T:System.ArgumentNullException:
     * To set the value to null, use System.DBNull.Value.
     */
    private String User;

    /**
     * Summary:
     * Gets or sets the connection string associated with the System.Data.Common.DbConnectionStringBuilder.
     * <p>
     * Returns:
     * The current connection string, created from the key/value pairs that are contained
     * within the System.Data.Common.DbConnectionStringBuilder. The default value is
     * an empty string.
     * <p>
     * Exceptions:
     * T:System.ArgumentException:
     * An invalid connection string argument has been supplied.
     */
    private String ConnectionString;

    /**
     * Summary:
     * Initializes a new instance of the System.Data.SqlClient.SqlConnectionStringBuilder
     * class.
     */
    public SqlConnectionStringBuilder() {
    }

    /**
     * Summary:
     * Initializes a new instance of the System.Data.SqlClient.SqlConnectionStringBuilder
     * class. The provided connection string provides the data for the instance's internal
     * connection information.
     * <p>
     * Parameters:
     * connectionString:
     * The basis for the object's internal connection information. Parsed into name/value
     * pairs. Invalid key names raise System.Collections.Generic.KeyNotFoundException.
     * <p>
     * Exceptions:
     * T:System.Collections.Generic.KeyNotFoundException:
     * Invalid key name within the connection string.
     * <p>
     * T:System.FormatException:
     * Invalid value within the connection string (specifically, when a Boolean or numeric
     * value was expected but not supplied).
     * <p>
     * T:System.ArgumentException:
     * The supplied connectionString is not valid.
     */
    public SqlConnectionStringBuilder(String connectionString) {
        String[] parts = connectionString.split(";");
        for (String s : parts) {
            if (s.contains("jdbc:sqlserver://")) {
                s = s.replace("jdbc:sqlserver://", "");
            }
            if (s.contains("=")) {
                String[] keyValue = s.split("=");
                switch (keyValue[0]) {
                    case "ApplicationName":
                        this.ApplicationName = keyValue[1];
                        break;
                    case "ConnectTimeout":
                        this.ConnectTimeout = Integer.parseInt(keyValue[1]);
                        break;
                    case "InitialCatalog":
                        this.InitialCatalog = keyValue[1];
                        break;
                    case "IntegratedSecurity":
                        this.IntegratedSecurity = Boolean.parseBoolean(keyValue[1]);
                        break;
                    case "Password":
                        this.Password = keyValue[1];
                        break;
                    case "PersistSecurityInfo":
                        this.PersistSecurityInfo = Boolean.parseBoolean(keyValue[1]);
                        break;
                    case "User":
                        this.User = keyValue[1];
                        break;
                }
            } else {
                this.DataSource = s;
            }
        }
        this.setConnectionString(connectionString);
    }

    public String getApplicationName() {
        return ApplicationName;
    }

    public void setApplicationName(String value) {
        this.ApplicationName = value;
    }

    public String getDataSource() {
        return DataSource;
    }

    public void setDataSource(String value) {
        this.DataSource = value;
    }

    public int getConnectTimeout() {
        return ConnectTimeout;
    }

    public void setConnectTimeout(int value) {
        this.ConnectTimeout = value;
    }

    public String getInitialCatalog() {
        return InitialCatalog;
    }

    public void setInitialCatalog(String value) {
        this.InitialCatalog = value;
    }

    public boolean getIntegratedSecurity() {
        return IntegratedSecurity;
    }

    public void setIntegratedSecurity(boolean value) {
        this.IntegratedSecurity = value;
    }

    public String getPassword() {
        return Password;
    }

    public void setPassword(String value) {
        this.Password = value;
    }

    public boolean getPersistSecurityInfo() {
        return PersistSecurityInfo;
    }

    public void setPersistSecurityInfo(boolean value) {
        this.PersistSecurityInfo = value;
    }

    public String getUser() {
        return User;
    }

    public void setUser(String value) {
        this.User = value;
    }

    public final String getConnectionString() {
        return ConnectionString;
    }

    public final void setConnectionString(String value) {
        this.ConnectionString = value;
    }

    /**
     * Summary:
     * Removes the entry with the specified key from the System.Data.SqlClient.SqlConnectionStringBuilder
     * instance.
     * <p>
     * Parameters:
     * keyword:
     * The key of the key/value pair to be removed from the connection string in this
     * System.Data.SqlClient.SqlConnectionStringBuilder.
     * <p>
     * Returns:
     * true if the key existed within the connection string and was removed; false if
     * the key did not exist.
     * <p>
     * Exceptions:
     * T:System.ArgumentNullException:
     * keyword is null (Nothing in Visual Basic)
     */
    public boolean Remove(String keyword) {
        return true;
    }

    /**
     * Summary:
     * Determines whether the System.Data.SqlClient.SqlConnectionStringBuilder contains
     * a specific key.
     * <p>
     * Parameters:
     * keyword:
     * The key to locate in the System.Data.SqlClient.SqlConnectionStringBuilder.
     * <p>
     * Returns:
     * true if the System.Data.SqlClient.SqlConnectionStringBuilder contains an element
     * that has the specified key; otherwise, false.
     * <p>
     * Exceptions:
     * T:System.ArgumentNullException:
     * keyword is null (Nothing in Visual Basic)
     */
    public boolean ContainsKey(String keyword) {
        return false;
    }

    @Override
    public String toString() {
        String dataSource = StringUtilsLocal.isNullOrEmpty(this.getDataSource()) ? "" : this.getDataSource() + ";";
        String initialCatalog = StringUtilsLocal.isNullOrEmpty(this.getInitialCatalog()) ? "" : "InitialCatalog=" + this.getInitialCatalog() + ";";
        String integratedSecurity = this.getIntegratedSecurity() ? "" : "IntegratedSecurity=" + this.getIntegratedSecurity() + ";";
        String persistSecurityInfo = this.getPersistSecurityInfo() ? "" : "PersistSecurityInfo=" + this.getPersistSecurityInfo() + ";";
        String appName = StringUtilsLocal.isNullOrEmpty(this.getApplicationName()) ? "" : "ApplicationName=" + this.getApplicationName() + ";";
        String timeout = this.getConnectTimeout() == 0 ? "" : "ConnectTimeout=" + this.getConnectTimeout() + ";";
        String pass = StringUtilsLocal.isNullOrEmpty(this.getPassword()) ? "" : "Password=" + this.getPassword() + ";";
        String user = StringUtilsLocal.isNullOrEmpty(this.getUser()) ? "" : "User=" + this.getUser() + ";";

        return "jdbc:sqlserver://" + dataSource + initialCatalog + user + pass + appName + timeout + integratedSecurity + persistSecurityInfo;
    }
}