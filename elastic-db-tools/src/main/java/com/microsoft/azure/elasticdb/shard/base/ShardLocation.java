package com.microsoft.azure.elasticdb.shard.base;

import java.io.Serializable;
import java.util.Locale;

import javax.xml.bind.annotation.XmlElement;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import com.microsoft.azure.elasticdb.shard.utils.GlobalConstants;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;

/**
 * Represents the location of a shard in terms of its server name and database name. This is used to manage connections to the shard and to support
 * other operations on shards. As opposed to a <see cref="Shard"/>, a shard location is not registered with the shard map.
 */
public final class ShardLocation implements Serializable {

    /**
     * Hashcode for the shard location.
     */
    private int hashCode;

    /**
     * Protocol name prefix.
     */
    @XmlElement(name = "Protocol")
    private SqlProtocol protocol = SqlProtocol.values()[0];

    /**
     * Gets the fully qualified hostname of the server for the shard database.
     */
    @XmlElement(name = "ServerName")
    private String server;

    /**
     * Communication port for TCP/IP protocol. If no port is specified, the property returns 0.
     */
    @XmlElement(name = "Port")
    private int port;

    /**
     * Gets the database name of the shard.
     */
    @XmlElement(name = "DatabaseName")
    private String database;

    public ShardLocation() {
    }

    /**
     * Constructor that allows specification of protocol, address, port and database to identify a shard.
     *
     * @param server
     *            Fully qualified hostname of the server for the shard database.
     * @param database
     *            Name of the shard database.
     * @param protocol
     *            Transport protcol used for the connection.
     * @param port
     *            Port number for TCP/IP connections. Specify 0 to use the default port for the specified <paramref name="protocol"/>.
     */
    public ShardLocation(String server,
            String database,
            SqlProtocol protocol,
            int port) {
        if (protocol.getValue() < SqlProtocol.Default.getValue() || protocol.getValue() > SqlProtocol.SharedMemory.getValue()) {
            throw new IllegalArgumentException(StringUtilsLocal.formatInvariant(Errors._ShardLocation_UnsupportedProtocol, protocol),
                    new Throwable("protocol"));
        }

        if (port < 0 || 65535 < port) {
            throw new IllegalArgumentException("port", new Throwable(StringUtilsLocal.formatInvariant(Errors._ShardLocation_InvalidPort, port)));
        }

        ExceptionUtils.disallowNullOrEmptyStringArgument(server, "server");
        ExceptionUtils.disallowNullOrEmptyStringArgument(database, "database");

        if (server.length() > GlobalConstants.MaximumServerLength) {
            throw new IllegalArgumentException(
                    StringUtilsLocal.formatInvariant(Errors._ShardLocation_InvalidServerOrDatabase, "Server", GlobalConstants.MaximumServerLength),
                    new Throwable("server"));
        }

        if (database.length() > GlobalConstants.MaximumDatabaseLength) {
            throw new IllegalArgumentException(StringUtilsLocal.formatInvariant(Errors._ShardLocation_InvalidServerOrDatabase, "Database",
                    GlobalConstants.MaximumDatabaseLength), new Throwable("database"));
        }

        this.setProtocol(protocol);
        this.setServer(server);
        this.setPort(port);
        this.setDatabase(database);
        hashCode = this.calculateHashCode();
    }

    /**
     * Constructor that allows specification of address and database to identify a shard.
     *
     * @param server
     *            Fully qualified hostname of the server for the shard database.
     * @param database
     *            Name of the shard database.
     */
    public ShardLocation(String server,
            String database) {
        this(server, database, SqlProtocol.Default, 0);
    }

    /**
     * Constructor that allows specification of address and database to identify a shard.
     *
     * @param server
     *            Fully qualified hostname of the server for the shard database.
     * @param database
     *            Name of the shard database.
     * @param protocol
     *            Transport protcol used for the connection.
     */
    public ShardLocation(String server,
            String database,
            SqlProtocol protocol) {
        this(server, database, protocol, 0);
    }

    public SqlProtocol getProtocol() {
        return protocol;
    }

    private void setProtocol(SqlProtocol value) {
        protocol = value;
    }

    public String getServer() {
        return server;
    }

    private void setServer(String value) {
        server = value;
    }

    public int getPort() {
        return port;
    }

    private void setPort(int value) {
        port = value;
    }

    /**
     * DataSource name which can be used to construct connection string Data Source property.
     */
    public String getDataSource() {
        return StringUtilsLocal.formatInvariant("%s%s%s", this.getProtocolPrefix(), this.getServer(), this.getPortSuffix());
    }

    public String getDatabase() {
        return database;
    }

    private void setDatabase(String value) {
        database = value;
    }

    /**
     * Converts the shard location to its string representation.
     *
     * @return String representation of shard location.
     */
    @Override
    public String toString() {
        return StringUtilsLocal.formatInvariant("[DataSource=%s Database=%s]", this.getDataSource(), this.getDatabase());
    }

    /**
     * Calculates the hash code for this instance.
     *
     * @return Hash code for the object.
     */
    @Override
    public int hashCode() {
        return hashCode;
    }

    /**
     * Determines whether the specified object is equal to the current object.
     *
     * @param obj
     *            The object to compare with the current object.
     * @return True if the specified object is equal to the current object; otherwise, false.
     */
    @Override
    public boolean equals(Object obj) {
        return this.equals((ShardLocation) ((obj instanceof ShardLocation) ? obj : null));
    }

    /**
     * Performs equality comparison with another given ShardLocation.
     *
     * @param other
     *            ShardLocation to compare with.
     * @return True if same locations, false otherwise.
     */
    public boolean equals(ShardLocation other) {
        return other != null && this.hashCode() == other.hashCode() && (this.getProtocol() == other.getProtocol() && this.getPort() == other.getPort()
                && this.getDataSource().equalsIgnoreCase(other.getDataSource()) && this.getDatabase().equalsIgnoreCase(other.getDatabase()));
    }

    /**
     * Calculates the hash code for the object.
     *
     * @return Hash code for the object.
     */
    private int calculateHashCode() {
        int h;

        h = ShardKey.qpHash(this.getProtocol().hashCode(), this.getDataSource().toUpperCase(Locale.ROOT).hashCode());
        h = ShardKey.qpHash(h, (new Integer(this.getPort())).hashCode());
        h = ShardKey.qpHash(h, this.getDatabase().toUpperCase(Locale.ROOT).hashCode());

        return h;
    }

    /**
     * Gets the connection string data source prefix for the supported protocol.
     *
     * @return Connection string prefix containing string representation of protocol.
     */
    private String getProtocolPrefix() {
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
    private String getPortSuffix() {
        if (this.getPort() != 0) {
            return StringUtilsLocal.formatInvariant(",%s", this.getPort());
        }
        else {
            return "";
        }
    }
}
