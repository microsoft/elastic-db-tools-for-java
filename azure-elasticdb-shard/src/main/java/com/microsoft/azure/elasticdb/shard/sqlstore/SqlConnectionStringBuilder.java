package com.microsoft.azure.elasticdb.shard.sqlstore;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import java.util.Collection;

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
     * Gets or sets a Boolean value that indicates whether asynchronous processing is
     * allowed by the connection created by using this connection string.
     * <p>
     * Returns:
     * CautionThis property is ignored beginning in .NET Framework 4.5. For more information
     * about SqlClient support for asynchronous programming, see Asynchronous Programming.The
     * value of the System.Data.SqlClient.SqlConnectionStringBuilder.AsynchronousProcessing
     * property, or false if no value has been supplied.
     */
    private boolean AsynchronousProcessing;
    /**
     * Summary:
     * Gets or sets a string that contains the name of the primary data file. This includes
     * the full path name of an attachable database.
     * <p>
     * Returns:
     * The value of the AttachDBFilename property, or String.Empty if no value has been
     * supplied.
     * <p>
     * Exceptions:
     * T:System.ArgumentNullException:
     * To set the value to null, use System.DBNull.Value.
     */
    private String AttachDBFilename;
    /**
     * Summary:
     * Obsolete. Gets or sets a Boolean value that indicates whether the connection
     * is reset when drawn from the connection pool.
     * <p>
     * Returns:
     * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.ConnectionReset
     * property, or true if no value has been supplied.
     */
    private boolean ConnectionReset;
    private int ConnectRetryCount;
    private int ConnectRetryInterval;
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
     * Gets or sets a value that indicates whether a client/server or in-process connection
     * to SQL Server should be made.
     * <p>
     * Returns:
     * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.ContextConnection
     * property, or False if none has been supplied.
     */

    private boolean ContextConnection;
    /**
     * Summary:
     * Gets or sets the SQL Server Language record name.
     * <p>
     * Returns:
     * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.CurrentLanguage
     * property, or String.Empty if no value has been supplied.
     * <p>
     * Exceptions:
     * T:System.ArgumentNullException:
     * To set the value to null, use System.DBNull.Value.
     */

    private String CurrentLanguage;
    /**
     * Summary:
     * Gets or sets a Boolean value that indicates whether SQL Server uses SSL encryption
     * for all data sent between the client and server if the server has a certificate
     * installed.
     * <p>
     * Returns:
     * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.Encrypt property,
     * or false if none has been supplied.
     */

    private boolean Encrypt;
    /**
     * Summary:
     * Gets or sets a Boolean value that indicates whether the SQL Server connection
     * pooler automatically enlists the connection in the creation thread's current
     * transaction context.
     * <p>
     * Returns:
     * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.Enlist property,
     * or true if none has been supplied.
     */

    private boolean Enlist;
    /**
     * Summary:
     * Gets or sets the name or address of the partner server to connect to if the primary
     * server is down.
     * <p>
     * Returns:
     * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.FailoverPartner
     * property, or String.Empty if none has been supplied.
     * <p>
     * Exceptions:
     * T:System.ArgumentNullException:
     * To set the value to null, use System.DBNull.Value.
     */

    private String FailoverPartner;
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
     * Gets a value that indicates whether the System.Data.SqlClient.SqlConnectionStringBuilder
     * has a fixed size.
     * <p>
     * Returns:
     * true in every case, because the System.Data.SqlClient.SqlConnectionStringBuilder
     * supplies a fixed-size collection of key/value pairs.
     */
    private boolean IsFixedSize;
    /**
     * Summary:
     * Gets an System.Collections.ICollection that contains the keys in the System.Data.SqlClient.SqlConnectionStringBuilder.
     * <p>
     * Returns:
     * An System.Collections.ICollection that contains the keys in the System.Data.SqlClient.SqlConnectionStringBuilder.
     */
    private Collection Keys;
    /**
     * Summary:
     * Gets or sets the minimum time, in seconds, for the connection to live in the
     * connection pool before being destroyed.
     * <p>
     * Returns:
     * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.LoadBalanceTimeout
     * property, or 0 if none has been supplied.
     */

    private int LoadBalanceTimeout;
    /**
     * Summary:
     * Gets or sets the maximum number of connections allowed in the connection pool
     * for this specific connection string.
     * <p>
     * Returns:
     * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.MaxPoolSize
     * property, or 100 if none has been supplied.
     */

    private int MaxPoolSize;
    /**
     * Summary:
     * Gets or sets the minimum number of connections allowed in the connection pool
     * for this specific connection string.
     * <p>
     * Returns:
     * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.MinPoolSize
     * property, or 0 if none has been supplied.
     */
    private int MinPoolSize;
    /**
     * Summary:
     * When true, an application can maintain multiple active result sets (MARS). When
     * false, an application must process or cancel all result sets from one batch before
     * it can execute any other batch on that connection.For more information, see Multiple
     * Active Result Sets (MARS).
     * <p>
     * Returns:
     * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.MultipleActiveResultSets
     * property, or false if none has been supplied.
     */

    private boolean MultipleActiveResultSets;
    /**
     * Summary:
     * If your application is connecting to an AlwaysOn availability group (AG) on different
     * subnets, setting MultiSubnetFailover=true provides faster detection of and connection
     * to the (currently) active server. For more information about SqlClient support
     * for Always On Availability Groups, see SqlClient Support for High Availability,
     * Disaster Recovery.
     * <p>
     * Returns:
     * Returns System.Boolean indicating the current value of the property.
     */

    private boolean MultiSubnetFailover;
    /**
     * Summary:
     * Gets or sets a string that contains the name of the network library used to establish
     * a connection to the SQL Server.
     * <p>
     * Returns:
     * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.NetworkLibrary
     * property, or String.Empty if none has been supplied.
     * <p>
     * Exceptions:
     * T:System.ArgumentNullException:
     * To set the value to null, use System.DBNull.Value.
     */

    private String NetworkLibrary;
    /**
     * Summary:
     * Gets or sets the size in bytes of the network packets used to communicate with
     * an instance of SQL Server.
     * <p>
     * Returns:
     * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.PacketSize
     * property, or 8000 if none has been supplied.
     */

    private int PacketSize;
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
     * Gets or sets a Boolean value that indicates whether the connection will be pooled
     * or explicitly opened every time that the connection is requested.
     * <p>
     * Returns:
     * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.Pooling property,
     * or true if none has been supplied.
     */

    private boolean Pooling;
    /**
     * Summary:
     * Gets or sets a Boolean value that indicates whether replication is supported
     * using the connection.
     * <p>
     * Returns:
     * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.Replication
     * property, or false if none has been supplied.
     */

    private boolean Replication;
    /**
     * Summary:
     * Gets or sets a string value that indicates how the connection maintains its association
     * with an enlisted System.Transactions transaction.
     * <p>
     * Returns:
     * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.TransactionBinding
     * property, or String.Empty if none has been supplied.
     */

    private String TransactionBinding;
    /**
     * Summary:
     * Gets or sets a value that indicates whether the channel will be encrypted while
     * bypassing walking the certificate chain to validate trust.
     * <p>
     * Returns:
     * A Boolean. Recognized values are true, false, yes, and no.
     */

    private boolean TrustServerCertificate;
    /**
     * Summary:
     * Gets or sets a string value that indicates the type system the application expects.
     * <p>
     * Returns:
     * The following table shows the possible values for the System.Data.SqlClient.SqlConnectionStringBuilder.TypeSystemVersion
     * property:ValueDescriptionSQL Server 2005Uses the SQL Server 2005 type system.
     * No conversions are made for the current version of ADO.NET.SQL Server 2008Uses
     * the SQL Server 2008 type system.LatestUse the latest version than this client-server
     * pair can handle. This will automatically move forward as the client and server
     * components are upgraded.
     */
    private String TypeSystemVersion;
    /**
     * Summary:
     * Gets or sets the user ID to be used when connecting to SQL Server.
     * <p>
     * Returns:
     * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.UserID property,
     * or String.Empty if none has been supplied.
     * <p>
     * Exceptions:
     * T:System.ArgumentNullException:
     * To set the value to null, use System.DBNull.Value.
     */

    private String UserID;
    /**
     * Summary:
     * Gets or sets a value that indicates whether to redirect the connection from the
     * default SQL Server Express instance to a runtime-initiated instance running under
     * the account of the caller.
     * <p>
     * Returns:
     * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.UserInstance
     * property, or False if none has been supplied.
     * <p>
     * Exceptions:
     * T:System.ArgumentNullException:
     * To set the value to null, use System.DBNull.Value.
     */

    private boolean UserInstance;
    /**
     * Summary:
     * Gets an System.Collections.ICollection that contains the values in the System.Data.SqlClient.SqlConnectionStringBuilder.
     * <p>
     * Returns:
     * An System.Collections.ICollection that contains the values in the System.Data.SqlClient.SqlConnectionStringBuilder.
     */
    private Collection Values;
    /**
     * Summary:
     * Gets or sets the name of the workstation connecting to SQL Server.
     * <p>
     * Returns:
     * The value of the System.Data.SqlClient.SqlConnectionStringBuilder.WorkstationID
     * property, or String.Empty if none has been supplied.
     * <p>
     * Exceptions:
     * T:System.ArgumentNullException:
     * To set the value to null, use System.DBNull.Value.
     */

    private String WorkstationID;
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
    }

    public String getApplicationName() {
        return ApplicationName;
    }

    public void setApplicationName(String value) {
        ApplicationName = value;
    }

    public String getDataSource() {
        return DataSource;
    }

    public void setDataSource(String value) {
        DataSource = value;
    }

    public boolean getAsynchronousProcessing() {
        return AsynchronousProcessing;
    }

    public void setAsynchronousProcessing(boolean value) {
        AsynchronousProcessing = value;
    }

    public String getAttachDBFilename() {
        return AttachDBFilename;
    }

    public void setAttachDBFilename(String value) {
        AttachDBFilename = value;
    }

    public boolean getConnectionReset() {
        return ConnectionReset;
    }

    public void setConnectionReset(boolean value) {
        ConnectionReset = value;
    }

    public int getConnectRetryCount() {
        return ConnectRetryCount;
    }

    public void setConnectRetryCount(int value) {
        ConnectRetryCount = value;
    }

    public int getConnectRetryInterval() {
        return ConnectRetryInterval;
    }

    public void setConnectRetryInterval(int value) {
        ConnectRetryInterval = value;
    }

    public int getConnectTimeout() {
        return ConnectTimeout;
    }

    public void setConnectTimeout(int value) {
        ConnectTimeout = value;
    }

    public boolean getContextConnection() {
        return ContextConnection;
    }

    public void setContextConnection(boolean value) {
        ContextConnection = value;
    }

    public String getCurrentLanguage() {
        return CurrentLanguage;
    }

    public void setCurrentLanguage(String value) {
        CurrentLanguage = value;
    }

    public boolean getEncrypt() {
        return Encrypt;
    }

    public void setEncrypt(boolean value) {
        Encrypt = value;
    }

    public boolean getEnlist() {
        return Enlist;
    }

    public void setEnlist(boolean value) {
        Enlist = value;
    }

    public String getFailoverPartner() {
        return FailoverPartner;
    }

    public void setFailoverPartner(String value) {
        FailoverPartner = value;
    }

    public String getInitialCatalog() {
        return InitialCatalog;
    }

    public void setInitialCatalog(String value) {
        InitialCatalog = value;
    }

    public boolean getIntegratedSecurity() {
        return IntegratedSecurity;
    }

    public void setIntegratedSecurity(boolean value) {
        IntegratedSecurity = value;
    }

    public boolean getIsFixedSize() {
        return IsFixedSize;
    }

    public Collection getKeys() {
        return Keys;
    }

    public int getLoadBalanceTimeout() {
        return LoadBalanceTimeout;
    }

    public void setLoadBalanceTimeout(int value) {
        LoadBalanceTimeout = value;
    }

    public int getMaxPoolSize() {
        return MaxPoolSize;
    }

    public void setMaxPoolSize(int value) {
        MaxPoolSize = value;
    }

    public int getMinPoolSize() {
        return MinPoolSize;
    }

    public void setMinPoolSize(int value) {
        MinPoolSize = value;
    }

    public boolean getMultipleActiveResultSets() {
        return MultipleActiveResultSets;
    }

    public void setMultipleActiveResultSets(boolean value) {
        MultipleActiveResultSets = value;
    }

    public boolean getMultiSubnetFailover() {
        return MultiSubnetFailover;
    }

    public void setMultiSubnetFailover(boolean value) {
        MultiSubnetFailover = value;
    }

    public String getNetworkLibrary() {
        return NetworkLibrary;
    }

    public void setNetworkLibrary(String value) {
        NetworkLibrary = value;
    }

    public int getPacketSize() {
        return PacketSize;
    }

    public void setPacketSize(int value) {
        PacketSize = value;
    }

    public String getPassword() {
        return Password;
    }

    public void setPassword(String value) {
        Password = value;
    }

    public boolean getPersistSecurityInfo() {
        return PersistSecurityInfo;
    }

    public void setPersistSecurityInfo(boolean value) {
        PersistSecurityInfo = value;
    }

    public boolean getPooling() {
        return Pooling;
    }

    public void setPooling(boolean value) {
        Pooling = value;
    }

    public boolean getReplication() {
        return Replication;
    }

    public void setReplication(boolean value) {
        Replication = value;
    }

    public String getTransactionBinding() {
        return TransactionBinding;
    }

    public void setTransactionBinding(String value) {
        TransactionBinding = value;
    }

    public boolean getTrustServerCertificate() {
        return TrustServerCertificate;
    }

    public void setTrustServerCertificate(boolean value) {
        TrustServerCertificate = value;
    }

    public String getTypeSystemVersion() {
        return TypeSystemVersion;
    }

    public void setTypeSystemVersion(String value) {
        TypeSystemVersion = value;
    }

    public String getUserID() {
        return UserID;
    }

    public void setUserID(String value) {
        UserID = value;
    }

    public boolean getUserInstance() {
        return UserInstance;
    }

    public void setUserInstance(boolean value) {
        UserInstance = value;
    }

    public Collection getValues() {
        return Values;
    }

    public String getWorkstationID() {
        return WorkstationID;
    }

    public void setWorkstationID(String value) {
        WorkstationID = value;
    }

    public final String getConnectionString() {
        return ConnectionString;
    }

    public final void setConnectionString(String value) {
        ConnectionString = value;
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
}