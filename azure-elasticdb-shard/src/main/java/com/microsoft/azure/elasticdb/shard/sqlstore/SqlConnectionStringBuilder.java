package com.microsoft.azure.elasticdb.shard.sqlstore;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

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
   * Returns:
   * The name of the application, or ".NET SqlClient Data Provider" if no name has
   * been supplied.
   * Exceptions:
   * T:System.ArgumentNullException:
   * To set the value to null, use System.DBNull.Value.
   */
  private String applicationName;

  /**
   * Summary:
   * Gets or sets the name or network address of the instance of SQL Server to connect
   * to.
   * Returns:
   * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.DataSource
   * property, or String.Empty if none has been supplied.
   * Exceptions:
   * T:System.ArgumentNullException:
   * To set the value to null, use System.DBNull.Value.
   */
  private String dataSource;

  /**
   * Summary:
   * Gets or sets the length of time (in seconds) to wait for a connection to the
   * server before terminating the attempt and generating an error.
   * Returns:
   * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.ConnectTimeout
   * property, or 15 seconds if no value has been supplied.
   */
  private int connectTimeout;

  private int connectRetryCount;

  /**
   * Summary:
   * Gets or sets the name of the database associated with the connection.
   * Returns:
   * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.DatabaseName
   * property, or String.Empty if none has been supplied.
   * Exceptions:
   * T:System.ArgumentNullException:
   * To set the value to null, use System.DBNull.Value.
   */
  private String databaseName;

  /**
   * Summary:
   * Gets or sets a Boolean value that indicates whether User ID and Password are
   * specified in the connection (when false) or whether the current Windows account
   * credentials are used for authentication (when true).
   * Returns:
   * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.IntegratedSecurity
   * property, or false if none has been supplied.
   */
  private boolean integratedSecurity;

  /**
   * Summary:
   * Gets or sets the password for the SQL Server account.
   * Returns:
   * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.Password property,
   * or String.Empty if none has been supplied.
   * Exceptions:
   * T:System.ArgumentNullException:
   * The password was incorrectly set to null. See code sample below.
   */
  private String password;

  /**
   * Summary:
   * Gets or sets a Boolean value that indicates if security-sensitive information,
   * such as the password, is not returned as part of the connection if the connection
   * is open or has ever been in an open state.
   * Returns:
   * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.PersistSecurityInfo
   * property, or false if none has been supplied.
   */
  private boolean persistSecurityInfo;

  /**
   * Summary:
   * Gets or sets the user ID to be used when connecting to SQL Server.
   * Returns:
   * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.User property,
   * or String.Empty if none has been supplied.
   * Exceptions:
   * T:System.ArgumentNullException:
   * To set the value to null, use System.DBNull.Value.
   */
  private String user;

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
   * Parameters:
   * connectionString:
   * The basis for the object's internal connection information. Parsed into name/value
   * pairs. Invalid key names raise System.Collections.Generic.KeyNotFoundException.
   * Exceptions:
   * T:System.Collections.Generic.KeyNotFoundException:
   * Invalid key name within the connection string.
   * T:System.FormatException:
   * Invalid value within the connection string (specifically, when a Boolean or numeric
   * value was expected but not supplied).
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
        this.setItem(keyValue[0], keyValue[1]);
      } else {
        this.dataSource = s;
      }
    }
  }

  public String getApplicationName() {
    return applicationName;
  }

  public void setApplicationName(String value) {
    this.applicationName = value;
  }

  public String getDataSource() {
    return dataSource;
  }

  public void setDataSource(String value) {
    this.dataSource = value;
  }

  public int getConnectTimeout() {
    return connectTimeout;
  }

  public void setConnectTimeout(int value) {
    this.connectTimeout = value;
  }

  public int getConnectRetryCount() {
    return connectRetryCount;
  }

  public void setConnectRetryCount(int value) {
    this.connectRetryCount = value;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String value) {
    this.databaseName = value;
  }

  public boolean getIntegratedSecurity() {
    return integratedSecurity;
  }

  public void setIntegratedSecurity(boolean value) {
    this.integratedSecurity = value;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String value) {
    this.password = value;
  }

  public boolean getPersistSecurityInfo() {
    return persistSecurityInfo;
  }

  public void setPersistSecurityInfo(boolean value) {
    this.persistSecurityInfo = value;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String value) {
    this.user = value;
  }

  public final String getConnectionString() {
    return this.toString();
  }

  /**
   * Summary: Removes the entry with the specified key from the SqlConnectionStringBuilder
   * instance. Parameters: keyword: The key of the key/value pair to be removed from the connection
   * string in this System.Data.SqlClient.SqlConnectionStringBuilder.  Returns: true if the key
   * existed within the connection string and was removed; false if the key did not exist.
   * Exceptions: T:System.ArgumentNullException: keyword is null (Nothing in Visual Basic)
   */
  public boolean remove(String keyword) {
    return this.setItem(keyword, "");
  }

  /**
   * Summary:
   * Determines whether the System.Data.SqlClient.SqlConnectionStringBuilder contains
   * a specific key.
   * Parameters:
   * keyword:
   * The key to locate in the System.Data.SqlClient.SqlConnectionStringBuilder.
   * Returns:
   * true if the System.Data.SqlClient.SqlConnectionStringBuilder contains an element
   * that has the specified key; otherwise, false.
   * Exceptions:
   * T:System.ArgumentNullException:
   * keyword is null (Nothing in Visual Basic)
   */
  public boolean containsKey(String keyword) {
    return this.getItem(keyword) != null;
  }

  @Override
  public String toString() {
    String dataSource =
        StringUtilsLocal.isNullOrEmpty(this.getDataSource()) ? "" : this.getDataSource() + ";";
    String databaseName = StringUtilsLocal.isNullOrEmpty(this.getDatabaseName()) ? ""
        : "DatabaseName=" + this.getDatabaseName() + ";";
    String integratedSecurity = this.getIntegratedSecurity() ? ""
        : "IntegratedSecurity=" + this.getIntegratedSecurity() + ";";
    String persistSecurityInfo = this.getPersistSecurityInfo() ? ""
        : "PersistSecurityInfo=" + this.getPersistSecurityInfo() + ";";
    String appName = StringUtilsLocal.isNullOrEmpty(this.getApplicationName()) ? ""
        : "ApplicationName=" + this.getApplicationName() + ";";
    String timeout =
        this.getConnectTimeout() == 0 ? "" : "ConnectTimeout=" + this.getConnectTimeout() + ";";
    String pass = StringUtilsLocal.isNullOrEmpty(this.getPassword()) ? ""
        : "Password=" + this.getPassword() + ";";
    String user =
        StringUtilsLocal.isNullOrEmpty(this.getUser()) ? "" : "User=" + this.getUser() + ";";

    return "jdbc:sqlserver://" + dataSource + databaseName + user + pass + appName + timeout
        + integratedSecurity + persistSecurityInfo;
  }

  /**
   * Set a specific Property of connection string.
   *
   * @param key Property Name
   * @param value Property Value
   * @return true if Property was set else false
   */
  public boolean setItem(String key, String value) {
    switch (key) {
      case "ApplicationName":
        this.applicationName = value;
        break;
      case "ConnectTimeout":
        this.connectTimeout = Integer.parseInt(value);
        break;
      case "DatabaseName":
        this.databaseName = value;
        break;
      case "IntegratedSecurity":
        this.integratedSecurity = Boolean.parseBoolean(value);
        break;
      case "Password":
        this.password = value;
        break;
      case "PersistSecurityInfo":
        this.persistSecurityInfo = Boolean.parseBoolean(value);
        break;
      case "User":
        this.user = value;
        break;
      default:
        return false;
    }
    return true;
  }

  /**
   * Get the value of a specific Property of the connection string.
   *
   * @param key Property Name
   * @return Property Value
   */
  public Object getItem(String key) {
    switch (key) {
      case "ApplicationName":
        return StringUtilsLocal.isNullOrEmpty(this.applicationName) ? null : this.applicationName;
      case "ConnectTimeout":
        return this.connectTimeout == 0 ? null : this.connectTimeout;
      case "DatabaseName":
        return StringUtilsLocal.isNullOrEmpty(this.databaseName) ? null : this.databaseName;
      case "IntegratedSecurity":
        return this.integratedSecurity;
      case "Password":
        return null;
      case "PersistSecurityInfo":
        return this.persistSecurityInfo;
      case "User":
        return StringUtilsLocal.isNullOrEmpty(this.user) ? null : this.user;
      default:
        return null;
    }
  }
}