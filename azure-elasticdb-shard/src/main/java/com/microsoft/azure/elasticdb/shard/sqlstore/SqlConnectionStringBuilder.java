package com.microsoft.azure.elasticdb.shard.sqlstore;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.helpers.ApplicationNameHelper;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;

/**
 * Provides a simple way to create and manage the contents of connection strings used by the
 * Connection class.
 */
public final class SqlConnectionStringBuilder {

  /**
   * Application name associated with the connection string.
   */
  private String applicationName;

  /**
   * Name or network address of the instance of SQL Server to connect to.
   */
  private String dataSource;

  /**
   * Length of time (in seconds) to wait for a connection to the server before terminating the
   * attempt and generating an error.
   */
  private int connectTimeout;

  /**
   * Number of times to retry for a connection to the server before terminating the attempt and
   * generating an error.
   */
  private int connectRetryCount;

  /**
   * Name of the database associated with the connection.
   */
  private String databaseName;

  /**
   * Indicates whether User ID and Password are specified in the connection (when false) or whether
   * the current Windows account credentials are used for authentication (when true).
   */
  private boolean integratedSecurity;

  /**
   * Password for the SQL Server account.
   */
  private String password;

  /**
   * Indicates if security-sensitive information, such as the password, is not returned as part of
   * the connection if the connection is open or has ever been in an open state.
   */
  private boolean persistSecurityInfo;

  /**
   * User ID to be used when connecting to SQL Server.
   */
  private String user;

  /**
   * Initializes a new instance of the SqlConnectionStringBuilder class.
   */
  public SqlConnectionStringBuilder() {
  }

  /**
   * Initializes a new instance of the SqlConnectionStringBuilder class. The provided connection
   * string provides the data for the instance's internal connection information.
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
   * Removes the entry with the specified key from the SqlConnectionStringBuilder instance.
   *
   * @param keyword The key to remove in the SqlConnectionStringBuilder.
   * @return true if key is removed false if not.
   */
  public boolean remove(String keyword) {
    return this.setItem(keyword, "");
  }

  /**
   * Determines whether the SqlConnectionStringBuilder contains a specific key.
   *
   * @param keyword The key to locate in the SqlConnectionStringBuilder.
   * @return true if key is located false if not.
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
    switch (key.toLowerCase()) {
      case "applicationname":
        this.applicationName = value;
        break;
      case "connecttimeout":
        this.connectTimeout = Integer.parseInt(value);
        break;
      case "databasename":
        this.databaseName = value;
        break;
      case "integratedsecurity":
        this.integratedSecurity = Boolean.parseBoolean(value);
        break;
      case "password":
        this.password = value;
        break;
      case "persistsecurityinfo":
        this.persistSecurityInfo = Boolean.parseBoolean(value);
        break;
      case "user":
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

  /**
   * Add MSQ Library Specific suffix to Application Name of the connection string.
   *
   * @param applicationNameSuffix MSQ Library Specific suffix
   * @return Connection string with application name and suffix attached
   */
  public SqlConnectionStringBuilder withApplicationNameSuffix(String applicationNameSuffix) {
    this.applicationName = ApplicationNameHelper.addApplicationNameSuffix(this.applicationName,
        applicationNameSuffix);
    return this;
  }
}