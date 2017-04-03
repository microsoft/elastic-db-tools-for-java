package com.microsoft.azure.elasticdb.shard.storeops.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.cache.ErrorsCache;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import com.microsoft.azure.elasticdb.shard.utils.GlobalConstants;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;

public class ShardLocation {

    private final SqlProtocol protocol;
    private final String server;
    private final String database;
    private final int port;

    private int _hashCode;
    private ErrorsCache errorCache = ErrorsCache.getInstance();

    public ShardLocation(
            String server,
            String database,
            SqlProtocol protocol,
            int port) {
        if (SqlProtocol.valueOf(protocol.name()) == null) {
            throw new IllegalArgumentException(
                    StringUtilsLocal.FormatInvariant(
                            errorCache.getProperty("_ShardLocation_UnsupportedProtocol"), protocol));
        }

        if (port < 0 || 65535 < port) {
            throw new IllegalArgumentException(
                    StringUtilsLocal.FormatInvariant(
                            errorCache.getProperty("_ShardLocation_InvalidPort"),
                            port));
        }

        ExceptionUtils.DisallowNullOrEmptyStringArgument(server, "server");
        ExceptionUtils.DisallowNullOrEmptyStringArgument(database, "database");

        if (server.length() > GlobalConstants.MaximumServerLength) {
            throw new IllegalArgumentException(
                    StringUtilsLocal.FormatInvariant(
                            errorCache.getProperty("_ShardLocation_InvalidServerOrDatabase"),
                            "Server",
                            GlobalConstants.MaximumServerLength,
                            "server"));
        }

        if (database.length() > GlobalConstants.MaximumDatabaseLength) {
            throw new IllegalArgumentException(
                    StringUtilsLocal.FormatInvariant(
                            errorCache.getProperty("_ShardLocation_InvalidServerOrDatabase"),
                            "Database",
                            GlobalConstants.MaximumDatabaseLength,
                            "database"));
        }
        this.protocol = protocol;
        this.server = server;
        this.port = port;
        this.database = database;
        _hashCode = this.CalculateHashCode();
    }

    /// <summary>
    /// Constructor that allows specification of address and database to identify a shard.
    /// </summary>
    /// <param name="server">Fully qualified hostname of the server for the shard database.</param>
    /// <param name="database">Name of the shard database.</param>
    public ShardLocation(String server, String database) {
        this(server, database, SqlProtocol.Default, 0);
    }

    /// <summary>
    /// Constructor that allows specification of address and database to identify a shard.
    /// </summary>
    /// <param name="server">Fully qualified hostname of the server for the shard database.</param>
    /// <param name="database">Name of the shard database.</param>
    /// <param name="protocol">Transport protcol used for the connection.</param>
    public ShardLocation(String server, String database, SqlProtocol protocol) {
        this(server, database, protocol, 0);
    }

    public SqlProtocol getProtocol() {
        return protocol;
    }

    public String getServer() {
        return server;
    }

    public String getDatabase() {
        return database;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return StringUtilsLocal.FormatInvariant(
                "[DataSource={0} Database={1}]",
                getDataSource(),
                getDatabase());
    }

    @Override
    public int hashCode() {
        return _hashCode;
    }

    public String getDataSource() {
        return StringUtilsLocal.FormatInvariant(
                "{0}{1}{2}",
                this.GetProtocolPrefix(),
                getServer(),
                this.GetPortSuffix());
    }

    /// <summary>
    /// Gets the connection string data source prefix for the supported protocol.
    /// </summary>
    /// <returns>Connection string prefix containing string representation of protocol.</returns>
    private String GetProtocolPrefix() {
        switch (this.protocol) {
            case Tcp:
                return "tcp:";
            case NamedPipes:
                return "np:";
            case SharedMemory:
                return "lpc:";
            default:
                assert (this.protocol == SqlProtocol.Default);
                return "";
        }
    }

    /// <summary>
    /// Gets the connection string data source suffix for supplied port number.
    /// </summary>
    /// <returns>Connection string suffix containing string representation of port.</returns>
    private String GetPortSuffix() {
        if (this.port != 0) {
            return StringUtilsLocal.FormatInvariant(",{0}", this.port);
        } else {
            return "";
        }
    }

    /// <summary>
    /// Calculates the hash code for the object.
    /// </summary>
    /// <returns>Hash code for the object.</returns>
    private int CalculateHashCode() {
        int h;

//        h = ShardKey.QPHash(this.protocol.hashCode(), getDataSource().toUpperCase(Locale.getDefault()).hashCode());
//        h = ShardKey.QPHash(h, String.valueOf(port).hashCode());
//        h = ShardKey.QPHash(h, this.database.toUpperCase(Locale.getDefault()).hashCode());

        return 0;// h; TODO: ShardKey implementation
    }

}
