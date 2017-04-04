package com.microsoft.azure.elasticdb.shard.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import com.microsoft.azure.elasticdb.shard.utils.GlobalConstants;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;

import java.io.Serializable;
import java.util.Locale;

/**
 * Represents the location of a shard in terms of its server name and database name.
 * This is used to manage connections to the shard and to support other operations on shards.
 * As opposed to a <see cref="Shard"/>, a shard location is not registered with the shard map.
 */
public final class ShardLocation implements Serializable {
    /**
     * Hashcode for the shard location.
     */
    private int _hashCode;
    /**
     * Protocol name prefix.
     */
    private SqlProtocol Protocol = SqlProtocol.values()[0];
    /**
     * Gets the fully qualified hostname of the server for the shard database.
     */
    private String Server;
    /**
     * Communication port for TCP/IP protocol. If no port is specified, the property returns 0.
     */
    private int Port;
    /**
     * Gets the database name of the shard.
     */
    private String Database;

    /**
     * Constructor that allows specification of protocol, address, port and database to identify a shard.
     *
     * @param server   Fully qualified hostname of the server for the shard database.
     * @param database Name of the shard database.
     * @param protocol Transport protcol used for the connection.
     * @param port     Port number for TCP/IP connections. Specify 0 to use the default port for the specified <paramref name="protocol"/>.
     */
    public ShardLocation(String server, String database, SqlProtocol protocol, int port) {
        if (protocol.getValue() < SqlProtocol.Default.getValue() || protocol.getValue() > SqlProtocol.SharedMemory.getValue()) {
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._ShardLocation_UnsupportedProtocol, protocol), "protocol");
        }

        if (port < 0 || 65535 < port) {
            throw new IllegalArgumentException("port", StringUtilsLocal.FormatInvariant(Errors._ShardLocation_InvalidPort, port));
        }

        ExceptionUtils.DisallowNullOrEmptyStringArgument(server, "server");
        ExceptionUtils.DisallowNullOrEmptyStringArgument(database, "database");

        if (server.length() > GlobalConstants.MaximumServerLength) {
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._ShardLocation_InvalidServerOrDatabase, "Server", GlobalConstants.MaximumServerLength), "server");
        }

        if (database.length() > GlobalConstants.MaximumDatabaseLength) {
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._ShardLocation_InvalidServerOrDatabase, "Database", GlobalConstants.MaximumDatabaseLength), "database");
        }

        this.setProtocol(protocol);
        this.setServer(server);
        this.setPort(port);
        this.setDatabase(database);
        _hashCode = this.CalculateHashCode();
    }

    /**
     * Constructor that allows specification of address and database to identify a shard.
     *
     * @param server   Fully qualified hostname of the server for the shard database.
     * @param database Name of the shard database.
     */
    public ShardLocation(String server, String database) {
        this(server, database, SqlProtocol.Default, 0);
    }

    /**
     * Constructor that allows specification of address and database to identify a shard.
     *
     * @param server   Fully qualified hostname of the server for the shard database.
     * @param database Name of the shard database.
     * @param protocol Transport protcol used for the connection.
     */
    public ShardLocation(String server, String database, SqlProtocol protocol) {
        this(server, database, protocol, 0);
    }

    public SqlProtocol getProtocol() {
        return Protocol;
    }

    private void setProtocol(SqlProtocol value) {
        Protocol = value;
    }

    public String getServer() {
        return Server;
    }

    private void setServer(String value) {
        Server = value;
    }

    public int getPort() {
        return Port;
    }

    private void setPort(int value) {
        Port = value;
    }

    /**
     * DataSource name which can be used to construct connection string Data Source property.
     */
    public String getDataSource() {
        return StringUtilsLocal.FormatInvariant("{0}{1}{2}", this.GetProtocolPrefix(), this.getServer(), this.GetPortSuffix());
    }

    public String getDatabase() {
        return Database;
    }

    private void setDatabase(String value) {
        Database = value;
    }

    /**
     * Converts the shard location to its string representation.
     *
     * @return String representation of shard location.
     */
    @Override
    public String toString() {
        return StringUtilsLocal.FormatInvariant("[DataSource={0} Database={1}]", this.getDataSource(), this.getDatabase());
    }

    /**
     * Calculates the hash code for this instance.
     *
     * @return Hash code for the object.
     */
    @Override
    public int hashCode() {
        return _hashCode;
    }

    /**
     * Determines whether the specified object is equal to the current object.
     *
     * @param obj The object to compare with the current object.
     * @return True if the specified object is equal to the current object; otherwise, false.
     */
    @Override
    public boolean equals(Object obj) {
        return this.equals((ShardLocation) ((obj instanceof ShardLocation) ? obj : null));
    }

    /**
     * Performs equality comparison with another given ShardLocation.
     *
     * @param other ShardLocation to compare with.
     * @return True if same locations, false otherwise.
     */
    public boolean equals(ShardLocation other) {
        if (other == null) {
            return false;
        } else {
            if (this.hashCode() != other.hashCode()) {
                return false;
            } else {
                return false;
                //TODO (this.getProtocol() == other.getProtocol() && String.Compare(this.getDataSource(), other.getDataSource(), StringComparison.OrdinalIgnoreCase) == 0 && this.getPort() == other.getPort() && String.Compare(this.getDatabase(), other.getDatabase(), StringComparison.OrdinalIgnoreCase) == 0);
            }
        }
    }

    /**
     * Calculates the hash code for the object.
     *
     * @return Hash code for the object.
     */
    private int CalculateHashCode() {
        int h;

        h = ShardKey.QPHash(this.getProtocol().hashCode(), this.getDataSource().toUpperCase(Locale.ROOT).hashCode());
        h = ShardKey.QPHash(h, (new Integer(this.getPort())).hashCode());
        h = ShardKey.QPHash(h, this.getDatabase().toUpperCase(Locale.ROOT).hashCode());

        return h;
    }

    /**
     * Gets the connection string data source prefix for the supported protocol.
     *
     * @return Connection string prefix containing string representation of protocol.
     */
    private String GetProtocolPrefix() {
        switch (this.getProtocol()) {
            case Tcp:
                return "tcp:";
            case NamedPipes:
                return "np:";
            case SharedMemory:
                return "lpc:";
            default:
                assert this.getProtocol() == SqlProtocol.Default;
                return "";
        }
    }

    /**
     * Gets the connection string data source suffix for supplied port number.
     *
     * @return Connection string suffix containing string representation of port.
     */
    private String GetPortSuffix() {
        if (this.getPort() != 0) {
            return StringUtilsLocal.FormatInvariant(",{0}", this.getPort());
        } else {
            return "";
        }
    }
}
