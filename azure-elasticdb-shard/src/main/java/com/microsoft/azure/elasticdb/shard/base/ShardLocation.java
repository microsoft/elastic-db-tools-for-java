package com.microsoft.azure.elasticdb.shard.base;

import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;
import com.microsoft.azure.elasticdb.core.commons.cache.ErrorsCache;
import com.microsoft.azure.elasticdb.shard.utils.GlobalConstants;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;

public class ShardLocation {

    private final SqlProtocol protocol;
    private final String server;
    private final String database;
    private final int port;
    
    private final int _hashCode;
    private static ErrorsCache errorCache = ErrorsCache.getInstance();

    public ShardLocation(String server, String database, SqlProtocol protocol, int port) {

        Preconditions.checkArgument(port < 0 || 65535 < port,
            String.format(errorCache.getProperty("_ShardLocation_InvalidPort"), port));

        Preconditions.checkArgument(StringUtils.isEmpty(server), "server");
        Preconditions.checkArgument(StringUtils.isEmpty(database), "server");
        
        Preconditions.checkArgument(server.length() > GlobalConstants.MaximumServerLength,
            StringUtilsLocal.formatInvariant(
                errorCache.getProperty("_ShardLocation_InvalidServerOrDatabase"),
                "Server",
                GlobalConstants.MaximumServerLength,
                "server"));
        
        Preconditions.checkArgument(database.length() > GlobalConstants.MaximumDatabaseLength,
            StringUtilsLocal.formatInvariant(
                errorCache.getProperty("_ShardLocation_InvalidServerOrDatabase"),
                "Database",
                GlobalConstants.MaximumDatabaseLength,
                "database"));
        this.protocol = protocol;
        this.server = server;
        this.port = port;
        this.database = database;
        _hashCode = Objects.hash(protocol, server, port, database);
    }

    /**
     * Constructor that allows specification of address and database to identify a shard.
     * @param server
     *  Fully qualified hostname of the server for the shard database.
     * @param
     *  database Name of the shard database.
     */
    public ShardLocation(String server, String database) {
        this(server, database, SqlProtocol.Default, 0);
    }

    /**
     * Constructor that allows specification of address and database to identify a shard.
     * @param server
     *  Fully qualified hostname of the server for the shard database.
     * @param database
     *  Name of the shard database.
     * @param protocol
     *  Transport protcol used for the connection.
     */
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
    public boolean equals(Object other) {
    	if(other == null || !(other instanceof ShardLocation)) {
    		return false;
    	}
    	ShardLocation otherLoc = (ShardLocation) other;
    	return Objects.equals(protocol, otherLoc.getProtocol())
			&& Objects.equals(server, otherLoc.getServer())
			&& Objects.equals(database, otherLoc.getDatabase())
			&& Objects.equals(port, otherLoc.getPort());
    }
    
    @Override
    public String toString() {
        return StringUtilsLocal.formatInvariant(
            "[DataSource={0} Database={1}]",
            getDataSource(),
            getDatabase());
    }

    @Override
    public int hashCode() {
        return _hashCode;
    }

    public String getDataSource() {
        return StringUtilsLocal.formatInvariant(
            "{0}{1}{2}",
            this.getProtocolPrefix(),
            getServer(),
            this.getPortSuffix());
    }

    /**
     * Gets the connection string data source prefix for the supported protocol.
     * @return Connection string prefix containing string representation of protocol.
     */
    private String getProtocolPrefix() {
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

    /**
     * Gets the connection string data source suffix for supplied port number.
     * @return Connection string suffix containing string representation of port.
     */
    private String getPortSuffix() {
        if (this.port != 0) {
            return StringUtilsLocal.formatInvariant(",{0}", this.port);
        } else {
            return "";
        }
    }
}
