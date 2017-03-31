package com.microsoft.azure.elasticdb.shard.map;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.core.commons.logging.ILogger;
import com.microsoft.azure.elasticdb.shard.base.IShardProvider;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapper.ConnectionOptions;
import com.microsoft.azure.elasticdb.shard.mapper.IShardMapper;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlShardMapManagerCredentials;
import com.microsoft.azure.elasticdb.shard.store.IStoreShardMap;
import com.microsoft.azure.elasticdb.shard.store.IUserStoreConnection;
import com.microsoft.azure.elasticdb.shard.storeops.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.utils.*;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import javafx.concurrent.Task;

import java.util.*;

/**
 * Represents a collection of shards and mappings between keys and shards in the collection.
 */
public abstract class ShardMap implements ICloneable<ShardMap> {
    /**
     * The mapper belonging to the ShardMap.
     */
    //private DefaultShardMapper _defaultMapper;

    /**
     * Reference to ShardMapManager.
     */
    private ShardMapManager Manager;
    /**
     * Storage representation.
     */
    private IStoreShardMap StoreShardMap;
    /**
     * Suffix added to application name in connections.
     */
    private String ApplicationNameSuffix;

    /**
     * Constructs an instance of ShardMap.
     *
     * @param manager Reference to ShardMapManager.
     * @param ssm     Storage representation.
     */
    public ShardMap(ShardMapManager manager, IStoreShardMap ssm) {
        assert manager != null;
        assert ssm != null;

        this.setManager(manager);
        this.setStoreShardMap(ssm);

        this.setApplicationNameSuffix(GlobalConstants.ShardMapManagerPrefix + ssm.getId().toString());

        //TODO: _defaultMapper = new DefaultShardMapper(this.getManager(), this);
    }

    /**
     * The tracer
     */
    private static ILogger getTracer() {
        return null;//TODO: TraceHelper.Tracer;
    }

    /**
     * Shard map name.
     */
    public final String getName() {
        return this.getStoreShardMap().getName();
    }

    /**
     * Shard map type.
     */
    public final ShardMapType getMapType() {
        return this.getStoreShardMap().getMapType();
    }

    /**
     * Shard map key type.
     */
    public final ShardKeyType getKeyType() {
        return this.getStoreShardMap().getKeyType();
    }

    /**
     * Identity.
     */
    public final UUID getId() {
        return this.getStoreShardMap().getId();
    }

    public final ShardMapManager getManager() {
        return Manager;
    }

    public final void setManager(ShardMapManager value) {
        Manager = value;
    }

    public final IStoreShardMap getStoreShardMap() {
        return StoreShardMap;
    }

    public final void setStoreShardMap(IStoreShardMap value) {
        StoreShardMap = value;
    }

    public final String getApplicationNameSuffix() {
        return ApplicationNameSuffix;
    }

    public final void setApplicationNameSuffix(String value) {
        ApplicationNameSuffix = value;
    }

    /**
     * Converts the object to its string representation.
     *
     * @return String representation of the object.
     */
    @Override
    public String toString() {
        return StringUtilsLocal.FormatInvariant("SM[{0}:{1}:{2}]", this.getStoreShardMap().getMapType(), this.getStoreShardMap().getKeyType(), this.getStoreShardMap().getName());
    }

    /**
     * Opens a regular <see cref="SqlConnection"/> to the shard
     * to which the specified key value is mapped, with <see cref="ConnectionOptions.Validate"/>.
     * <p>
     * <typeparam name="TKey">Type of the key.</typeparam>
     *
     * @param key              Input key value.
     * @param connectionString Connection string with credential information such as SQL Server credentials or Integrated Security settings.
     *                         The hostname of the server and the database name for the shard are obtained from the lookup operation for key.
     * @return An opened SqlConnection.
     * <p>
     * Note that the <see cref="SqlConnection"/> object returned by this call is not protected against transient faults.
     * Callers should follow best practices to protect the connection against transient faults
     * in their application code, e.g., by using the transient fault handling
     * functionality in the Enterprise Library from Microsoft Patterns and Practices team.
     * This call only works if there is a single default mapping.
     */
    public final <TKey> SQLServerConnection OpenConnectionForKey(TKey key, String connectionString) {
        return this.OpenConnectionForKey(key, connectionString, ConnectionOptions.Validate);
    }

    /**
     * Opens a regular <see cref="SqlConnection"/> to the shard
     * to which the specified key value is mapped.
     * <p>
     * <typeparam name="TKey">Type of the key.</typeparam>
     *
     * @param key              Input key value.
     * @param connectionString Connection string with credential information such as SQL Server credentials or Integrated Security settings.
     *                         The hostname of the server and the database name for the shard are obtained from the lookup operation for key.
     * @param options          Options for validation operations to perform on opened connection.
     * @return An opened SqlConnection.
     * <p>
     * Note that the <see cref="SqlConnection"/> object returned by this call is not protected against transient faults.
     * Callers should follow best practices to protect the connection against transient faults
     * in their application code, e.g., by using the transient fault handling
     * functionality in the Enterprise Library from Microsoft Patterns and Practices team.
     * This call only works if there is a single default mapping.
     */
    public final <TKey> SQLServerConnection OpenConnectionForKey(TKey key, String connectionString, ConnectionOptions options) {
        ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");

        assert this.getStoreShardMap().getKeyType() != ShardKeyType.None;

        //TODO:
        /*try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.NewGuid())) {
            IShardMapper<TKey> mapper = this.<TKey>GetMapper();

            if (mapper == null) {
                throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._ShardMap_OpenConnectionForKey_KeyTypeNotSupported, TKey.class, this.getStoreShardMap().getName(), ShardKey.TypeFromShardKeyType(this.getStoreShardMap().getKeyType())), "key");
            }

            assert mapper != null;

            return mapper.OpenConnectionForKey(key, connectionString, options);
        }*/
        return null;
    }

    /**
     * Asynchronously opens a regular <see cref="SqlConnection"/> to the shard
     * to which the specified key value is mapped, with <see cref="ConnectionOptions.Validate"/>.
     * <p>
     * <typeparam name="TKey">Type of the key.</typeparam>
     *
     * @param key              Input key value.
     * @param connectionString Connection string with credential information such as SQL Server credentials or Integrated Security settings.
     *                         The hostname of the server and the database name for the shard are obtained from the lookup operation for key.
     * @return A Task encapsulating an opened SqlConnection.
     * <p>
     * Note that the <see cref="SqlConnection"/> object returned by this call is not protected against transient faults.
     * Callers should follow best practices to protect the connection against transient faults
     * in their application code, e.g., by using the transient fault handling
     * functionality in the Enterprise Library from Microsoft Patterns and Practices team.
     * This call only works if there is a single default mapping.
     */
    public final <TKey> Task<SQLServerConnection> OpenConnectionForKeyAsync(TKey key, String connectionString) {
        return this.OpenConnectionForKeyAsync(key, connectionString, ConnectionOptions.Validate);
    }

    /**
     * Asynchronously opens a regular <see cref="SqlConnection"/> to the shard
     * to which the specified key value is mapped.
     * <p>
     * <typeparam name="TKey">Type of the key.</typeparam>
     *
     * @param key              Input key value.
     * @param connectionString Connection string with credential information such as SQL Server credentials or Integrated Security settings.
     *                         The hostname of the server and the database name for the shard are obtained from the lookup operation for key.
     * @param options          Options for validation operations to perform on opened connection.
     * @return A Task encapsulating an opened SqlConnection.
     * <p>
     * Note that the <see cref="SqlConnection"/> object returned by this call is not protected against transient faults.
     * Callers should follow best practices to protect the connection against transient faults
     * in their application code, e.g., by using the transient fault handling
     * functionality in the Enterprise Library from Microsoft Patterns and Practices team.
     * This call only works if there is a single default mapping.
     */
    public final <TKey> Task<SQLServerConnection> OpenConnectionForKeyAsync(TKey key, String connectionString, ConnectionOptions options) {
        ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");

        assert this.getStoreShardMap().getKeyType() != ShardKeyType.None;

        //TODO:
        /*try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.NewGuid())) {
            IShardMapper<TKey> mapper = this.<TKey>GetMapper();

            if (mapper == null) {
                throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._ShardMap_OpenConnectionForKey_KeyTypeNotSupported, TKey.class, this.getStoreShardMap().getName(), ShardKey.TypeFromShardKeyType(this.getStoreShardMap().getKeyType())), "key");
            }

            assert mapper != null;

            return mapper.OpenConnectionForKeyAsync(key, connectionString, options);
        }*/
        return null;
    }

    /**
     * Gets all shards from the shard map.
     *
     * @return All shards belonging to the shard map.
     */
    public final List<Shard> GetShards() {
        /*try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.NewGuid())) {
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMap, "GetShards", "Start; ");

            Stopwatch stopwatch = Stopwatch.StartNew();

            List<Shard> shards = _defaultMapper.GetShards();

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMap, "GetShards", "Complete; Duration: {0}", stopwatch.Elapsed);

            return shards;
        }*/
        return null;
    }

    /**
     * Obtains the shard for the specified location.
     *
     * @param location Location of the shard.
     * @return Shard which has the specified location.
     */
    public final Shard GetShard(ShardLocation location) {
        ExceptionUtils.DisallowNullArgument(location, "location");

        /*try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.NewGuid())) {
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMap, "GetShard", "Start; Shard Location: {0} ", location);

            Stopwatch stopwatch = Stopwatch.StartNew();

            Shard shard = _defaultMapper.GetShardByLocation(location);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMap, "GetShard", "Complete; Shard Location: {0}; Duration: {1}", location, stopwatch.Elapsed);

            if (shard == null) {
                throw new ShardManagementException(ShardManagementErrorCategory.ShardMap, ShardManagementErrorCode.ShardDoesNotExist, Errors._ShardMap_GetShard_ShardDoesNotExist, location, this.getName());
            }

            return shard;
        }*/

        return null;
    }

    /**
     * Tries to obtains the shard for the specified location.
     *
     * @param location Location of the shard.
     * @param shard    Shard which has the specified location.
     * @return <c>true</c> if shard with specified location is found, <c>false</c> otherwise.
     */
    public final boolean TryGetShard(ShardLocation location, ReferenceObjectHelper<Shard> shard) {
        ExceptionUtils.DisallowNullArgument(location, "location");

        /*try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.NewGuid())) {
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMap, "TryGetShard", "Start; Shard Location: {0} ", location);

            Stopwatch stopwatch = Stopwatch.StartNew();

            shard.argValue = _defaultMapper.GetShardByLocation(location);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMap, "TryGetShard", "Complete; Shard Location: {0}; Duration: {1}", location, stopwatch.Elapsed);

            return shard.argValue != null;
        }*/

        return false;
    }

    /**
     * Creates a new shard and registers it with the shard map.
     *
     * @param shardCreationArgs Information about shard to be added.
     * @return A new shard registered with this shard map.
     */
    /*public final Shard CreateShard(ShardCreationInfo shardCreationArgs) {
        ExceptionUtils.DisallowNullArgument(shardCreationArgs, "shardCreationArgs");

        try (ActivityIdScope activityId = new ActivityIdScope(UUID.NewGuid())) {
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMap, "CreateShard", "Start; Shard: {0} ", shardCreationArgs.Location);

            Stopwatch stopwatch = Stopwatch.StartNew();

            Shard shard = _defaultMapper.Add(new Shard(this.getManager(), this, shardCreationArgs));

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMap, "CreateShard", "Complete; Shard: {0}; Duration: {1}", shard.Location, stopwatch.Elapsed);

            return shard;
        }
    }*/

    /**
     * Atomically adds a shard to ShardMap using the specified location.
     *
     * @param location Location of shard to be added.
     * @return A shard attached to this shard map.
     */
    public final Shard CreateShard(ShardLocation location) {
        ExceptionUtils.DisallowNullArgument(location, "location");

        /*try (ActivityIdScope activityId = new ActivityIdScope(UUID.NewGuid())) {
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMap, "CreateShard", "Start; Shard: {0} ", location);

            Stopwatch stopwatch = Stopwatch.StartNew();

            Shard shard = _defaultMapper.Add(new Shard(this.getManager(), this, new ShardCreationInfo(location)));

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMap, "CreateShard", "Complete; Shard: {0}; Duration: {1}", location, stopwatch.Elapsed);

            return shard;
        }*/
        return null;
    }

    /**
     * Removes a shard from ShardMap.
     *
     * @param shard Shard to remove.
     */
    public final void DeleteShard(Shard shard) {
        ExceptionUtils.DisallowNullArgument(shard, "shard");

        /*try (ActivityIdScope activityId = new ActivityIdScope(UUID.NewGuid())) {
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMap, "DeleteShard", "Start; Shard: {0} ", shard.Location);

            Stopwatch stopwatch = Stopwatch.StartNew();

            _defaultMapper.Remove(shard);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMap, "DeleteShard", "Complete; Shard: {0}; Duration: {1}", shard.Location, stopwatch.Elapsed);
        }*/
    }

    /**
     * Updates a shard with the changes specified in the <paramref name="update"/> parameter.
     *
     * @param //currentShard Shard being updated.
     * @param //update       Updated properties of the shard.
     * @return New Shard with updated information.
     */
    /*public final Shard UpdateShard(Shard currentShard, ShardUpdate update) {
        ExceptionUtils.DisallowNullArgument(currentShard, "currentShard");
        ExceptionUtils.DisallowNullArgument(update, "update");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.NewGuid())) {
            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMap, "UpdateShard", "Start; Shard: {0}", currentShard.Location);

            Stopwatch stopwatch = Stopwatch.StartNew();

            Shard shard = _defaultMapper.UpdateShard(currentShard, update);

            stopwatch.Stop();

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMap, "UpdateShard", "Complete; Shard: {0}; Duration: {1}", currentShard.Location, stopwatch.Elapsed);

            return shard;
        }
    }*/
    public final SQLServerConnection OpenConnection(IShardProvider shardProvider, String connectionString) {
        return OpenConnection(shardProvider, connectionString, ConnectionOptions.Validate);
    }

    /**
     * Opens a connection to the given shard provider.
     *
     * @param shardProvider    Shard provider containing shard to be connected to.
     * @param connectionString Connection string for connection. Must have credentials.
     * @param options          Options for validation operations to perform on opened connection.
     */
    public final SQLServerConnection OpenConnection(IShardProvider shardProvider, String connectionString, ConnectionOptions options) {
        //Debug.Assert(shardProvider != null, "Expecting IShardProvider.");
        ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");

        String connectionStringFinal = this.ValidateAndPrepareConnectionString(shardProvider, connectionString);

        ExceptionUtils.EnsureShardBelongsToShardMap(this.getManager(), this, shardProvider.getShardInfo(), "OpenConnection", "Shard");

        /*IUserStoreConnection conn = this.getManager().StoreConnectionFactory.GetUserConnection(connectionStringFinal);

        getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMap, "OpenConnection", "Start; Shard: {0}; Options: {1}; ConnectionString: {2}", shardProvider.ShardInfo.Location, options, connectionStringFinal);

        try (ConditionalDisposable<IUserStoreConnection> cd = new ConditionalDisposable<IUserStoreConnection>(conn)) {
            Stopwatch stopwatch = Stopwatch.StartNew();

            conn.Open();

            stopwatch.Stop();

            // If validation is requested.
            if ((options & ConnectionOptions.Validate) == ConnectionOptions.Validate) {
                shardProvider.Validate(this.getStoreShardMap(), conn.Connection);
            }

            cd.DoNotDispose = true;

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMap, "OpenConnection", "Complete; Shard: {0}; Options: {1}; Open Duration: {2}", shardProvider.ShardInfo.Location, options, stopwatch.Elapsed);
        }*/

        return null; //TODO: conn.Connection;
    }

    public final Task<SQLServerConnection> OpenConnectionAsync(IShardProvider shardProvider, String connectionString) {
        return OpenConnectionAsync(shardProvider, connectionString, ConnectionOptions.Validate);
    }

    /**
     * Asynchronously opens a connection to the given shard provider.
     *
     * @param shardProvider    Shard provider containing shard to be connected to.
     * @param connectionString Connection string for connection. Must have credentials.
     * @param options          Options for validation operations to perform on opened connection.
     * @return A task encapsulating the SqlConnection
     * All exceptions are reported via the returned task.
     */
    public final Task<SQLServerConnection> OpenConnectionAsync(IShardProvider shardProvider, String connectionString, ConnectionOptions options) {
        //Debug.Assert(shardProvider != null, "Expecting IShardProvider.");
        ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");

        String connectionStringFinal = this.ValidateAndPrepareConnectionString(shardProvider, connectionString);

        ExceptionUtils.EnsureShardBelongsToShardMap(this.getManager(), this, shardProvider.getShardInfo(), "OpenConnectionAsync", "Shard");

        /*IUserStoreConnection conn = this.getManager().StoreConnectionFactory.GetUserConnection(connectionStringFinal);

        getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMap, "OpenConnectionAsync", "Start; Shard: {0}; Options: {1}; ConnectionString: {2}", shardProvider.ShardInfo.Location, options, connectionStringFinal);

        try (ConditionalDisposable<IUserStoreConnection> cd = new ConditionalDisposable<IUserStoreConnection>(conn)) {
            Stopwatch stopwatch = Stopwatch.StartNew();

            //await conn.OpenAsync().ConfigureAwait(false);

            stopwatch.Stop();

            // If validation is requested.
            if ((options & ConnectionOptions.Validate) == ConnectionOptions.Validate) {
                //await shardProvider.ValidateAsync(this.getStoreShardMap(), conn.Connection).ConfigureAwait(false);
            }

            cd.DoNotDispose = true;

            getTracer().TraceInfo(TraceSourceConstants.ComponentNames.ShardMap, "OpenConnectionAsync", "Complete; Shard: {0}; Options: {1}; Open Duration: {2}", shardProvider.ShardInfo.Location, options, stopwatch.Elapsed);
        }*/

        return null; //TODO: conn.Connection;
    }

    /**
     * Gets the mapper. This method is used by OpenConnection and Lookup of V.
     * <p>
     * <typeparam name="V">Shard provider type.</typeparam>
     *
     * @return Appropriate mapper for the given shard map.
     */
    /*public abstract <V> IShardMapper<V> GetMapper();*/

    ///#region ICloneable<ShardMap>

    /**
     * Clones the given shard map.
     *
     * @return A cloned instance of the shard map.
     */
    public final ShardMap Clone() {
        return this.CloneCore();
    }

    /**
     * Clones the current shard map instance.
     *
     * @return Cloned shard map instance.
     */
    protected abstract ShardMap CloneCore();

//TODO TASK: There is no preprocessor in Java:
    ///#endregion ICloneable<ShardMap>

    /**
     * Ensures that the provided connection string is valid and builds the connection string
     * to be used for DDR connection to the given shard provider.
     *
     * @param shardProvider    Shard provider containing shard to be connected to.
     * @param connectionString Input connection string.
     * @return Connection string for DDR connection.
     */
    private String ValidateAndPrepareConnectionString(IShardProvider shardProvider, String connectionString) {
        assert shardProvider != null;
        assert connectionString != null;

        // Devnote: If connection string specifies Active Directory authentication and runtime is not
        // .NET 4.6 or higher, then below call will throw.
        /*SqlConnectionStringBuilder connectionStringBuilder = new SqlConnectionStringBuilder(connectionString);

        // DataSource must not be set.
        if (!StringUtilsLocal.isNullOrEmpty(connectionStringBuilder.DataSource)) {
            throw new IllegalArgumentException(StringUtils.FormatInvariant(Errors._ShardMap_OpenConnection_ConnectionStringPropertyDisallowed, "DataSource"), "connectionString");
        }

        // InitialCatalog must not be set.
        if (!StringUtilsLocal.isNullOrEmpty(connectionStringBuilder.InitialCatalog)) {
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._ShardMap_OpenConnection_ConnectionStringPropertyDisallowed, "Initial Catalog"), "connectionString");
        }

        // ConnectRetryCount must not be set (default value is 1)
        if (ShardMapUtils.IsConnectionResiliencySupported && (Integer) connectionStringBuilder.getItem(ShardMapUtils.ConnectRetryCount) > 1) {
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._ShardMap_OpenConnection_ConnectionStringPropertyDisallowed, ShardMapUtils.ConnectRetryCount), "connectionString");
        }

        // Verify that either UserID/Password or provided or integrated authentication is enabled.
        SqlShardMapManagerCredentials.EnsureCredentials(connectionStringBuilder, "connectionString");

        Shard s = shardProvider.ShardInfo;

        connectionStringBuilder.DataSource = s.Location.DataSource;
        connectionStringBuilder.InitialCatalog = s.Location.Database;

        // Append the proper post-fix for ApplicationName
        connectionStringBuilder.ApplicationName = ApplicationNameHelper.AddApplicationNameSuffix(connectionStringBuilder.ApplicationName, this.getApplicationNameSuffix());

        // Disable connection resiliency if necessary
        if (ShardMapUtils.IsConnectionResiliencySupported) {
            connectionStringBuilder.setItem(ShardMapUtils.ConnectRetryCount, 0);
        }*/

        return null; //TODO: connectionStringBuilder.ConnectionString;
    }
}