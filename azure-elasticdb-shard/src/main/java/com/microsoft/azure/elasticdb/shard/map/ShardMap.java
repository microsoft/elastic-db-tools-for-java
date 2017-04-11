package com.microsoft.azure.elasticdb.shard.map;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.microsoft.azure.elasticdb.core.commons.helpers.ApplicationNameHelper;
import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.core.commons.logging.ActivityIdScope;
import com.microsoft.azure.elasticdb.shard.base.*;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapper.ConnectionOptions;
import com.microsoft.azure.elasticdb.shard.mapper.DefaultShardMapper;
import com.microsoft.azure.elasticdb.shard.mapper.IShardMapper1;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlConnectionStringBuilder;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlShardMapManagerCredentials;
import com.microsoft.azure.elasticdb.shard.store.IUserStoreConnection;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.utils.*;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Represents a collection of shards and mappings between keys and shards in the collection.
 */
public abstract class ShardMap implements ICloneable<ShardMap> {

    final static Logger log = LoggerFactory.getLogger(ShardMap.class);

    /**
     * Reference to ShardMapManager.
     */
    protected ShardMapManager shardMapManager;
    /**
     * Storage representation.
     */
    protected StoreShardMap storeShardMap;
    /**
     * The mapper belonging to the ShardMap.
     */
    private DefaultShardMapper _defaultMapper;
    /**
     * Suffix added to application name in connections.
     */
    private String ApplicationNameSuffix;

    /**
     * Constructs an instance of ShardMap.
     *
     * @param shardMapManager Reference to ShardMapManager.
     * @param ssm             Storage representation.
     */
    public ShardMap(ShardMapManager shardMapManager, StoreShardMap ssm) {
        this.shardMapManager = Preconditions.checkNotNull(shardMapManager);
        this.storeShardMap = Preconditions.checkNotNull(ssm);

        this.setApplicationNameSuffix(GlobalConstants.ShardMapManagerPrefix + ssm.getId().toString());

        _defaultMapper = new DefaultShardMapper(this.getShardMapManager(), this);
    }

    /**
     * Shard map name.
     */
    public String getName() {
        return storeShardMap.getName();
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

    public final ShardMapManager getShardMapManager() {
        return shardMapManager;
    }

    public final void setShardMapManager(ShardMapManager value) {
        shardMapManager = value;
    }

    public final StoreShardMap getStoreShardMap() {
        return storeShardMap;
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
    public <TKey> SQLServerConnection OpenConnectionForKey(TKey key, String connectionString) {
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
    public <TKey> SQLServerConnection OpenConnectionForKey(TKey key, String connectionString, ConnectionOptions options) {
        ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");

        assert this.getStoreShardMap().getKeyType() != ShardKeyType.None;

        /*try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            IShardMapper<TKey> mapper = this.<TKey>GetMapper();

            if (mapper == null) {
                throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._ShardMap_OpenConnectionForKey_KeyTypeNotSupported, TKey.class, this.getStoreShardMap().getName(), ShardKey.TypeFromShardKeyType(this.getStoreShardMap().getKeyType())), "key");
            }

            assert mapper != null;

            return mapper.OpenConnectionForKey(key, connectionString, options);
        }*/
        return null; //TODO
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
    public <TKey> Callable<SQLServerConnection> OpenConnectionForKeyAsync(TKey key, String connectionString) {
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
    public <TKey> Callable<SQLServerConnection> OpenConnectionForKeyAsync(TKey key, String connectionString, ConnectionOptions options) {
        ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");

        assert this.getStoreShardMap().getKeyType() != ShardKeyType.None;

        /*try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            IShardMapper<TKey> mapper = this.<TKey>GetMapper();

            if (mapper == null) {
                throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._ShardMap_OpenConnectionForKey_KeyTypeNotSupported, TKey.class, this.getStoreShardMap().getName(), ShardKey.TypeFromShardKeyType(this.getStoreShardMap().getKeyType())), "key");
            }

            assert mapper != null;

            return mapper.OpenConnectionForKeyAsync(key, connectionString, options);
        }*/
        return null; //TODO
    }

    /**
     * Gets all shards from the shard map.
     *
     * @return All shards belonging to the shard map.
     */
    public final List<Shard> GetShards() {
        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("GetShards", "Start; ");

            Stopwatch stopwatch = Stopwatch.createStarted();

            List<Shard> shards = _defaultMapper.GetShards();

            stopwatch.stop();

            log.info("GetShards", "Complete; Duration: {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return shards;
        }
    }

    /**
     * Obtains the shard for the specified location.
     *
     * @param location Location of the shard.
     * @return Shard which has the specified location.
     */
    public final Shard GetShard(ShardLocation location) {
        ExceptionUtils.DisallowNullArgument(location, "location");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("GetShard", "Start; Shard Location: {} ", location);

            Stopwatch stopwatch = Stopwatch.createStarted();

            Shard shard = _defaultMapper.GetShardByLocation(location);

            stopwatch.stop();

            log.info("GetShard", "Complete; Shard Location: {}; Duration: {}", location, stopwatch.elapsed(TimeUnit.MILLISECONDS));

            if (shard == null) {
                throw new ShardManagementException(ShardManagementErrorCategory.ShardMap, ShardManagementErrorCode.ShardDoesNotExist, Errors._ShardMap_GetShard_ShardDoesNotExist, location, this.getName());
            }

            return shard;
        }
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

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("TryGetShard", "Start; Shard Location: {0} ", location);

            Stopwatch stopwatch = Stopwatch.createStarted();

            shard.argValue = _defaultMapper.GetShardByLocation(location);

            stopwatch.stop();

            log.info("TryGetShard", "Complete; Shard Location: {0}; Duration: {1}", location, stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return shard.argValue != null;
        }
    }

    /**
     * Creates a new shard and registers it with the shard map.
     *
     * @param shardCreationArgs Information about shard to be added.
     * @return A new shard registered with this shard map.
     */
    public final Shard CreateShard(ShardCreationInfo shardCreationArgs) {
        ExceptionUtils.DisallowNullArgument(shardCreationArgs, "shardCreationArgs");

        try (ActivityIdScope activityId = new ActivityIdScope(UUID.randomUUID())) {
            log.info("CreateShard", "Start; Shard: {} ", shardCreationArgs.getLocation());

            Stopwatch stopwatch = Stopwatch.createStarted();

            Shard shard = _defaultMapper.Add(new Shard(this.getShardMapManager(), this, shardCreationArgs));

            stopwatch.stop();

            log.info("CreateShard", "Complete; Shard: {0}; Duration: {1}", shard.getLocation(), stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return shard;
        }
    }

    /**
     * Atomically adds a shard to ShardMap using the specified location.
     *
     * @param location Location of shard to be added.
     * @return A shard attached to this shard map.
     */
    public final Shard CreateShard(ShardLocation location) {
        ExceptionUtils.DisallowNullArgument(location, "location");

        try (ActivityIdScope activityId = new ActivityIdScope(UUID.randomUUID())) {
            log.info("CreateShard", "Start; Shard: {} ", location);

            Stopwatch stopwatch = Stopwatch.createStarted();

            Shard shard = _defaultMapper.Add(new Shard(this.getShardMapManager(), this, new ShardCreationInfo(location)));

            stopwatch.stop();

            log.info("CreateShard", "Complete; Shard: {}; Duration: {}", location, stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return shard;
        }
    }

    /**
     * Removes a shard from ShardMap.
     *
     * @param shard Shard to remove.
     */
    public final void DeleteShard(Shard shard) {
        ExceptionUtils.DisallowNullArgument(shard, "shard");

        try (ActivityIdScope activityId = new ActivityIdScope(UUID.randomUUID())) {
            log.info("DeleteShard", "Start; Shard: {0} ", shard.getLocation());

            Stopwatch stopwatch = Stopwatch.createStarted();

            _defaultMapper.Remove(shard);

            stopwatch.stop();

            log.info("DeleteShard", "Complete; Shard: {0}; Duration: {1}", shard.getLocation(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    /**
     * Updates a shard with the changes specified in the <paramref name="update"/> parameter.
     *
     * @param currentShard Shard being updated.
     * @param update       Updated properties of the shard.
     * @return New Shard with updated information.
     */
    public final Shard UpdateShard(Shard currentShard, ShardUpdate update) {
        ExceptionUtils.DisallowNullArgument(currentShard, "currentShard");
        ExceptionUtils.DisallowNullArgument(update, "update");

        try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
            log.info("UpdateShard", "Start; Shard: {0}", currentShard.getLocation());

            Stopwatch stopwatch = Stopwatch.createStarted();

            Shard shard = _defaultMapper.UpdateShard(currentShard, update);

            stopwatch.stop();

            log.info("UpdateShard", "Complete; Shard: {0}; Duration: {1}", currentShard.getLocation(), stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return shard;
        }
    }

    /**
     * Opens a connection to the given shard provider.
     *
     * @param shardProvider    Shard provider containing shard to be connected to.
     * @param connectionString Connection string for connection. Must have credentials.
     */
    public final SQLServerConnection OpenConnection(IShardProvider shardProvider, String connectionString) {
        return OpenConnection(shardProvider, connectionString, ConnectionOptions.Validate);
    }

    public final SQLServerConnection OpenConnection(IShardProvider shardProvider, String connectionString, ConnectionOptions options) {
        assert shardProvider != null;
        ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");

        String connectionStringFinal = this.ValidateAndPrepareConnectionString(shardProvider, connectionString);

        ExceptionUtils.EnsureShardBelongsToShardMap(this.getShardMapManager(), this, shardProvider.getShardInfo(), "OpenConnection", "Shard");

        IUserStoreConnection conn = this.getShardMapManager().getStoreConnectionFactory().GetUserConnection(connectionStringFinal);

        log.info("OpenConnection", "Start; Shard: {0}; Options: {1}; ConnectionString: {2}", shardProvider.getShardInfo().getLocation(), options, connectionStringFinal);

        return null; //TODO
        /*try (ConditionalDisposable<IUserStoreConnection> cd = new ConditionalDisposable<IUserStoreConnection>(conn)) {
            Stopwatch stopwatch = Stopwatch.createStarted();

            conn.Open();

            stopwatch.stop();

            // If validation is requested.
            if ((options & ConnectionOptions.Validate) == ConnectionOptions.Validate) {
                shardProvider.Validate(this.getStoreShardMap(), conn.getConnection());
            }

            cd.DoNotDispose = true;

            log.info("OpenConnection", "Complete; Shard: {0}; Options: {1}; Open Duration: {2}", shardProvider.getShardInfo().getLocation(), options, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }

        return (SQLServerConnection) conn.getConnection();*/
    }

    /**
     * Asynchronously opens a connection to the given shard provider.
     *
     * @param shardProvider    Shard provider containing shard to be connected to.
     * @param connectionString Connection string for connection. Must have credentials.
     * @return A task encapsulating the SqlConnection
     * All exceptions are reported via the returned task.
     */

    public final Callable<SQLServerConnection> OpenConnectionAsync(IShardProvider shardProvider, String connectionString) {
        return OpenConnectionAsync(shardProvider, connectionString, ConnectionOptions.Validate);
    }

    public final Callable<SQLServerConnection> OpenConnectionAsync(IShardProvider shardProvider, String connectionString, ConnectionOptions options) {
        assert shardProvider != null;
        ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");

        String connectionStringFinal = this.ValidateAndPrepareConnectionString(shardProvider, connectionString);

        ExceptionUtils.EnsureShardBelongsToShardMap(this.getShardMapManager(), this, shardProvider.getShardInfo(), "OpenConnectionAsync", "Shard");

        IUserStoreConnection conn = this.getShardMapManager().getStoreConnectionFactory().GetUserConnection(connectionStringFinal);

        log.info("OpenConnectionAsync", "Start; Shard: {0}; Options: {1}; ConnectionString: {2}", shardProvider.getShardInfo().getLocation(), options, connectionStringFinal);

        return null; //TODO
        /*try (ConditionalDisposable<IUserStoreConnection> cd = new ConditionalDisposable<IUserStoreConnection>(conn)) {
            Stopwatch stopwatch = Stopwatch.createStarted();

//TODO TASK: There is no equivalent to 'await' in Java:
            await conn.OpenAsync().ConfigureAwait(false);

            stopwatch.stop();

            // If validation is requested.
            if ((options & ConnectionOptions.Validate) == ConnectionOptions.Validate) {
//TODO TASK: There is no equivalent to 'await' in Java:
                await shardProvider.ValidateAsync(this.getStoreShardMap(), conn.Connection).ConfigureAwait(false);
            }

            cd.DoNotDispose = true;

            log.info("OpenConnectionAsync", "Complete; Shard: {0}; Options: {1}; Open Duration: {2}", shardProvider.getShardInfo().getLocation(), options, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }

        return conn.Connection;*/
    }

    /**
     * Gets the mapper. This method is used by OpenConnection and Lookup of V.
     * <p>
     * <typeparam name="V">Shard provider type.</typeparam>
     *
     * @return Appropriate mapper for the given shard map.
     */
    public abstract <V> IShardMapper1<V> GetMapper();

    ///#region ICloneable<ShardMap>

    /**
     * Clones the given shard map.
     *
     * @return A cloned instance of the shard map.
     */
    public ShardMap Clone() {
        return this.CloneCore();
    }

    /**
     * Clones the current shard map instance.
     *
     * @return Cloned shard map instance.
     */
    protected abstract ShardMap CloneCore();

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
        SqlConnectionStringBuilder connectionStringBuilder = new SqlConnectionStringBuilder(connectionString);

        // DataSource must not be set.
        if (!Strings.isNullOrEmpty(connectionStringBuilder.getDataSource())) {
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._ShardMap_OpenConnection_ConnectionStringPropertyDisallowed, "DataSource"), new Throwable("connectionString"));
        }

        // DatabaseName must not be set.
        if (!Strings.isNullOrEmpty(connectionStringBuilder.getDatabaseName())) {
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._ShardMap_OpenConnection_ConnectionStringPropertyDisallowed, "Initial Catalog"), new Throwable("connectionString"));
        }

        // ConnectRetryCount must not be set (default value is 1)
        if (ShardMapUtils.getIsConnectionResiliencySupported() && connectionStringBuilder.getConnectRetryCount() > 1) {
            throw new IllegalArgumentException(StringUtilsLocal.FormatInvariant(Errors._ShardMap_OpenConnection_ConnectionStringPropertyDisallowed, ShardMapUtils.ConnectRetryCount), new Throwable("connectionString"));
        }

        // Verify that either UserID/Password or provided or integrated authentication is enabled.
        SqlShardMapManagerCredentials.EnsureCredentials(connectionStringBuilder, "connectionString");

        Shard s = shardProvider.getShardInfo();

        connectionStringBuilder.setDataSource(s.getLocation().getDataSource());
        connectionStringBuilder.setDatabaseName(s.getLocation().getDatabase());

        // Append the proper post-fix for ApplicationName
        connectionStringBuilder.setApplicationName(ApplicationNameHelper.AddApplicationNameSuffix(connectionStringBuilder.getApplicationName(), this.getApplicationNameSuffix()));

        // Disable connection resiliency if necessary
        if (ShardMapUtils.getIsConnectionResiliencySupported()) {
            //TODO: connectionStringBuilder.setItem(ShardMapUtils.ConnectRetryCount, 0);
        }

        return connectionStringBuilder.getConnectionString();
    }
}