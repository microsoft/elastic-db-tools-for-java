package com.microsoft.azure.elasticdb.shard.mapmanager;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.google.common.base.Stopwatch;
import com.microsoft.azure.elasticdb.core.commons.helpers.EventHandler;
import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.core.commons.logging.ActivityIdScope;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryBehavior;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryingEventArgs;
import com.microsoft.azure.elasticdb.shard.cache.CacheStore;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlShardMapManagerCredentials;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlStoreConnectionFactory;
import com.microsoft.azure.elasticdb.shard.store.StoreResult;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.Version;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationFactory;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import com.microsoft.azure.elasticdb.shard.utils.ExceptionUtils;
import com.microsoft.azure.elasticdb.shard.utils.GlobalConstants;
import com.microsoft.azure.elasticdb.shard.utils.StringUtilsLocal;
import java.lang.invoke.MethodHandles;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for <see cref="ShardMapManager"/>s facilitates the creation and management of shard map
 * manager persistent state. Use this class as the entry point to the library's object hierarchy.
 */
public final class ShardMapManagerFactory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Creates a <see cref="ShardMapManager"/> and its corresponding storage structures in the
   * specified SQL Server database, with <see cref="ShardMapManagerCreateMode.KeepExisting"/> and
   * <see cref="RetryBehavior.DefaultRetryBehavior"/>.
   *
   * @param connectionString Connection parameters used for creating shard map manager database.
   * @return A shard map manager object used for performing management and read operations for shard
   * maps, shards and shard mappings.
   */
  public static ShardMapManager createSqlShardMapManager(String connectionString) {
    return createSqlShardMapManager(connectionString, ShardMapManagerCreateMode.KeepExisting,
        RetryBehavior.getDefaultRetryBehavior());
  }

  /**
   * Creates a <see cref="ShardMapManager"/> and its corresponding storage structures in the
   * specified SQL Server database, with <see cref="RetryBehavior.DefaultRetryBehavior"/>.
   *
   * @param connectionString Connection parameters used for creating shard map manager database.
   * @param createMode Describes the option selected by the user for creating shard map manager
   * database.
   * @return A shard map manager object used for performing management and read operations for shard
   * maps, shards and shard mappings.
   */
  public static ShardMapManager createSqlShardMapManager(String connectionString,
      ShardMapManagerCreateMode createMode) {
    return createSqlShardMapManager(connectionString, createMode,
        RetryBehavior.getDefaultRetryBehavior());
  }

  /**
   * Creates a <see cref="ShardMapManager"/> and its corresponding storage structures in the
   * specified SQL Server database, with <see cref="RetryPolicy.getDefaultRetryPolicy()"/>.
   *
   * @param connectionString Connection parameters used for creating shard map manager database.
   * @param createMode Describes the option selected by the user for creating shard map manager
   * database.
   * @param targetVersion Target version of store to create.
   */
  public static ShardMapManager createSqlShardMapManager(String connectionString,
      ShardMapManagerCreateMode createMode, Version targetVersion) {
    return createSqlShardMapManagerImpl(connectionString, createMode,
        RetryBehavior.getDefaultRetryBehavior(), null, targetVersion);
  }

  /**
   * Creates a <see cref="ShardMapManager"/> and its corresponding storage structures in the
   * specified SQL Server database, with <see cref="ShardMapManagerCreateMode.KeepExisting"/>.
   *
   * @param connectionString Connection parameters used for creating shard map manager database.
   * @param retryBehavior Behavior for detecting transient exceptions in the store.
   * @return A shard map manager object used for performing management and read operations for shard
   * maps, shards and shard mappings.
   */
  public static ShardMapManager createSqlShardMapManager(String connectionString,
      RetryBehavior retryBehavior) {
    return createSqlShardMapManager(connectionString, ShardMapManagerCreateMode.KeepExisting,
        retryBehavior);
  }

  /**
   * Creates a <see cref="ShardMapManager"/> and its corresponding storage structures in the
   * specified SQL Server database.
   *
   * @param connectionString Connection parameters used for creating shard map manager database.
   * @param createMode Describes the option selected by the user for creating shard map manager
   * database.
   * @param retryBehavior Behavior for detecting transient exceptions in the store.
   * @return A shard map manager object used for performing management and read operations for shard
   * maps, shards and shard mappings.
   */
  public static ShardMapManager createSqlShardMapManager(String connectionString,
      ShardMapManagerCreateMode createMode, RetryBehavior retryBehavior) {
    return createSqlShardMapManagerImpl(connectionString, createMode, retryBehavior, null,
        GlobalConstants.GsmVersionClient);
  }

  /**
   * Creates a <see cref="ShardMapManager"/> and its corresponding storage structures in the
   * specified SQL Server database.
   *
   * @param connectionString Connection parameters used for creating shard map manager database.
   * @param createMode Describes the option selected by the user for creating shard map manager
   * database.
   * @param retryBehavior Behavior for detecting transient exceptions in the store.
   * @param retryEventHandler Event handler for store operation retry events.
   * @return A shard map manager object used for performing management and read operations for shard
   * maps, shards and shard mappings.
   */
  public static ShardMapManager createSqlShardMapManager(String connectionString,
      ShardMapManagerCreateMode createMode, RetryBehavior retryBehavior,
      EventHandler<RetryingEventArgs> retryEventHandler) {
    return createSqlShardMapManagerImpl(connectionString, createMode, retryBehavior,
        retryEventHandler, GlobalConstants.GsmVersionClient);
  }

  /**
   * Creates a <see cref="ShardMapManager"/> and its corresponding storage structures in the
   * specified SQL Server database.
   *
   * @param connectionString Connection parameters used for creating shard map manager database.
   * @param createMode Describes the option selected by the user for creating shard map manager
   * database.
   * @param retryBehavior Behavior for performing retries on connections to shard map manager
   * database.
   * @param targetVersion Target version of Store to deploy, this is mainly used for upgrade
   * testing.
   * @param retryEventHandler Event handler for store operation retry events.
   * @return A shard map manager object used for performing management and read operations for shard
   * maps, shards and shard mappings.
   */
  private static ShardMapManager createSqlShardMapManagerImpl(String connectionString,
      ShardMapManagerCreateMode createMode, RetryBehavior retryBehavior,
      EventHandler<RetryingEventArgs> retryEventHandler, Version targetVersion) {
    ExceptionUtils.disallowNullArgument(connectionString, "connectionString");
    ExceptionUtils.disallowNullArgument(retryBehavior, "retryBehavior");

    if (createMode != ShardMapManagerCreateMode.KeepExisting
        && createMode != ShardMapManagerCreateMode.ReplaceExisting) {
      throw new IllegalArgumentException(StringUtilsLocal.formatInvariant(
          Errors._General_InvalidArgumentValue, createMode, "createMode"),
          new Throwable("createMode"));
    }

    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      log.info("ShardMapManagerFactory CreateSqlShardMapManager Start; ");

      Stopwatch stopwatch = Stopwatch.createStarted();

      SqlShardMapManagerCredentials credentials = new SqlShardMapManagerCredentials(
          connectionString);

      RetryPolicy retryPolicy = new RetryPolicy(
          new ShardManagementTransientErrorDetectionStrategy(retryBehavior),
          RetryPolicy.getDefaultRetryPolicy().getExponentialRetryStrategy());

      EventHandler<RetryingEventArgs> handler = (sender, args) -> {
        if (retryEventHandler != null) {
          retryEventHandler.invoke(sender, new RetryingEventArgs(args));
        }
      };

      try {
        retryPolicy.retrying.addListener(handler);

        // specifying targetVersion as GlobalConstants.GsmVersionClient
        // to deploy latest store by default.
        try (IStoreOperationGlobal op = (new StoreOperationFactory())
            .createCreateShardMapManagerGlobalOperation(credentials, retryPolicy,
                "CreateSqlShardMapManager", createMode, targetVersion)) {
          op.doGlobal();
        } catch (Exception e) {
          e.printStackTrace();
          ExceptionUtils.throwStronglyTypedException(e);
        }

        stopwatch.stop();

        log.info("ShardMapManagerFactory CreateSqlShardMapManager Complete; Duration:{}",
            stopwatch.elapsed(TimeUnit.MILLISECONDS));
      } finally {
        retryPolicy.retrying.removeListener(handler);
      }

      return new ShardMapManager(credentials, new SqlStoreConnectionFactory(),
          new StoreOperationFactory(), new CacheStore(), ShardMapManagerLoadPolicy.Lazy,
          RetryPolicy.getDefaultRetryPolicy(), retryBehavior, retryEventHandler);
    }
  }

  /**
   * Gets <see cref="ShardMapManager"/> from persisted state in a SQL Server database.
   *
   * @param connectionString Connection parameters used for performing operations against shard map
   * manager database(s).
   * @param loadPolicy Initialization policy.
   * @param shardMapManager Shard map manager object used for performing management and read
   * operations for shard maps, shards and shard mappings or <c>null</c> in case shard map manager
   * does not exist.
   * @return <c>true</c> if a shard map manager object was created, <c>false</c> otherwise.
   */
  public static boolean tryGetSqlShardMapManager(String connectionString,
      ShardMapManagerLoadPolicy loadPolicy,
      ReferenceObjectHelper<ShardMapManager> shardMapManager) {
    return tryGetSqlShardMapManager(connectionString, loadPolicy,
        RetryBehavior.getDefaultRetryBehavior(), shardMapManager);
  }

  /**
   * Gets <see cref="ShardMapManager"/> from persisted state in a SQL Server database.
   *
   * @param connectionString Connection parameters used for performing operations against shard map
   * manager database(s).
   * @param loadPolicy Initialization policy.
   * @param shardMapManager Shard map manager object used for performing management and read
   * operations for shard maps, shards and shard mappings or <c>null</c> in case shard map manager
   * does not exist.
   * @param retryBehavior Behavior for detecting transient exceptions in the store.
   * @return <c>true</c> if a shard map manager object was created, <c>false</c> otherwise.
   */
  public static boolean tryGetSqlShardMapManager(String connectionString,
      ShardMapManagerLoadPolicy loadPolicy, RetryBehavior retryBehavior,
      ReferenceObjectHelper<ShardMapManager> shardMapManager) {
    ExceptionUtils.disallowNullArgument(connectionString, "connectionString");
    ExceptionUtils.disallowNullArgument(retryBehavior, "retryBehavior");

    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      log.info("ShardMapManagerFactory TryGetSqlShardMapManager Start; ");

      Stopwatch stopwatch = Stopwatch.createStarted();

      shardMapManager.argValue = ShardMapManagerFactory.getSqlShardMapManager(connectionString,
          loadPolicy, retryBehavior, null, false);

      stopwatch.stop();

      log.info("ShardMapManagerFactory TryGetSqlShardMapManager Complete; Duration:{}",
          stopwatch.elapsed(TimeUnit.MILLISECONDS));

      return shardMapManager.argValue != null;
    }
  }

  /**
   * Gets <see cref="ShardMapManager"/> from persisted state in a SQL Server database.
   *
   * @param connectionString Connection parameters used for performing operations against shard map
   * manager database(s).
   * @param loadPolicy Initialization policy.
   * @param shardMapManager Shard map manager object used for performing management and read
   * operations for shard maps, shards and shard mappings or <c>null</c> in case shard map manager
   * does not exist.
   * @param retryBehavior Behavior for detecting transient exceptions in the store.
   * @param retryEventHandler Event handler for store operation retry events.
   * @return <c>true</c> if a shard map manager object was created, <c>false</c> otherwise.
   */
  public static boolean tryGetSqlShardMapManager(String connectionString,
      ShardMapManagerLoadPolicy loadPolicy, RetryBehavior retryBehavior,
      EventHandler<RetryingEventArgs> retryEventHandler,
      ReferenceObjectHelper<ShardMapManager> shardMapManager) {
    ExceptionUtils.disallowNullArgument(connectionString, "connectionString");
    ExceptionUtils.disallowNullArgument(retryBehavior, "retryBehavior");

    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      log.info("ShardMapManagerFactory TryGetSqlShardMapManager Start; ");

      Stopwatch stopwatch = Stopwatch.createStarted();

      shardMapManager.argValue = ShardMapManagerFactory.getSqlShardMapManager(connectionString,
          loadPolicy, retryBehavior, retryEventHandler, false);

      stopwatch.stop();

      log.info("ShardMapManagerFactory TryGetSqlShardMapManager Complete; Duration:{}",
          stopwatch.elapsed(TimeUnit.MILLISECONDS));

      return shardMapManager.argValue != null;
    }
  }

  /**
   * Gets <see cref="ShardMapManager"/> from persisted state in a SQL Server database, with <see
   * cref="RetryBehavior.DefaultRetryBehavior"/>.
   *
   * @param connectionString Connection parameters used for performing operations against shard map
   * manager database(s).
   * @param loadPolicy Initialization policy.
   * @return A shard map manager object used for performing management and read operations for shard
   * maps, shards and shard mappings.
   */
  public static ShardMapManager getSqlShardMapManager(String connectionString,
      ShardMapManagerLoadPolicy loadPolicy) {
    return getSqlShardMapManager(connectionString, loadPolicy,
        RetryBehavior.getDefaultRetryBehavior());
  }

  /**
   * Gets <see cref="ShardMapManager"/> from persisted state in a SQL Server database.
   *
   * @param connectionString Connection parameters used for performing operations against shard map
   * manager database(s).
   * @param loadPolicy Initialization policy.
   * @param retryBehavior Behavior for detecting transient exceptions in the store.
   * @return A shard map manager object used for performing management and read operations for shard
   * maps, shards and shard mappings.
   */
  public static ShardMapManager getSqlShardMapManager(String connectionString,
      ShardMapManagerLoadPolicy loadPolicy, RetryBehavior retryBehavior) {
    return getSqlShardMapManager(connectionString, loadPolicy, retryBehavior, null);
  }

  /**
   * Gets <see cref="ShardMapManager"/> from persisted state in a SQL Server database.
   *
   * @param connectionString Connection parameters used for performing operations against shard map
   * manager database(s).
   * @param loadPolicy Initialization policy.
   * @param retryBehavior Behavior for detecting transient exceptions in the store.
   * @param retryEventHandler Event handler for store operation retry events.
   * @return A shard map manager object used for performing management and read operations for shard
   * maps, shards and shard mappings.
   */
  public static ShardMapManager getSqlShardMapManager(String connectionString,
      ShardMapManagerLoadPolicy loadPolicy, RetryBehavior retryBehavior,
      EventHandler<RetryingEventArgs> retryEventHandler) {
    ExceptionUtils.disallowNullArgument(connectionString, "connectionString");
    ExceptionUtils.disallowNullArgument(retryBehavior, "retryBehavior");

    try (ActivityIdScope activityIdScope = new ActivityIdScope(UUID.randomUUID())) {
      log.info("ShardMapManagerFactory GetSqlShardMapManager Start; ");

      Stopwatch stopwatch = Stopwatch.createStarted();

      ShardMapManager shardMapManager = ShardMapManagerFactory.getSqlShardMapManager(
          connectionString, loadPolicy, retryBehavior, retryEventHandler, true);

      stopwatch.stop();

      assert shardMapManager != null;

      log.info("ShardMapManagerFactory GetSqlShardMapManager Complete; Duration: {}",
          stopwatch.elapsed(TimeUnit.MILLISECONDS));

      return shardMapManager;
    }
  }

  /**
   * Gets <see cref="ShardMapManager"/> from persisted state in a SQL Server database.
   *
   * @param connectionString Connection parameters used for performing operations against shard map
   * manager database(s).
   * @param loadPolicy Initialization policy.
   * @param retryBehavior Behavior for detecting transient exceptions in the store.
   * @param retryEventHandler Event handler for store operation retry events.
   * @param throwOnFailure Whether to raise exception on failure.
   * @return A shard map manager object used for performing management and read operations for shard
   * maps, shards and shard mappings or <c>null</c> if the object could not be created.
   */
  private static ShardMapManager getSqlShardMapManager(String connectionString,
      ShardMapManagerLoadPolicy loadPolicy, RetryBehavior retryBehavior,
      EventHandler<RetryingEventArgs> retryEventHandler, boolean throwOnFailure) {
    assert connectionString != null;
    assert retryBehavior != null;

    SqlShardMapManagerCredentials credentials = new SqlShardMapManagerCredentials(connectionString);

    StoreOperationFactory storeOperationFactory = new StoreOperationFactory();

    StoreResults result = null;

    RetryPolicy retryPolicy = new RetryPolicy(
        new ShardManagementTransientErrorDetectionStrategy(retryBehavior),
        RetryPolicy.getDefaultRetryPolicy().getExponentialRetryStrategy());

    EventHandler<RetryingEventArgs> handler = (sender, args) -> {
      if (retryEventHandler != null) {
        retryEventHandler.invoke(sender, new RetryingEventArgs(args));
      }
    };

    try {
      retryPolicy.retrying.addListener(handler);

      try (IStoreOperationGlobal op = storeOperationFactory.createGetShardMapManagerGlobalOperation(
          credentials, retryPolicy, throwOnFailure ? "GetSqlShardMapManager"
              : "TryGetSqlShardMapManager", throwOnFailure)) {
        result = op.doGlobal();
      } catch (Exception e) {
        e.printStackTrace();
        throw (ShardManagementException) e.getCause();
      }
    } finally {
      retryPolicy.retrying.removeListener(handler);
    }

    return result.getResult() == StoreResult.Success ? new ShardMapManager(credentials,
        new SqlStoreConnectionFactory(), storeOperationFactory, new CacheStore(), loadPolicy,
        RetryPolicy.getDefaultRetryPolicy(), retryBehavior, retryEventHandler) : null;
  }
}
