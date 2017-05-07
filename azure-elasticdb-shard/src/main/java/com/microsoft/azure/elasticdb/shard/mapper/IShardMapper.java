package com.microsoft.azure.elasticdb.shard.mapper;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.base.IShardProvider;
import java.sql.Connection;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Container for a collection of keys to shards mappings.
 */
public interface IShardMapper<MappingT extends IShardProvider, ValueT> {

  Connection openConnectionForKey(ValueT key, String connectionString);

  /**
   * Given a key value, obtains a SqlConnection to the shard in the mapping
   * that contains the key value.
   *
   * @param key Input key value.
   * @param connectionString Connection string with credential information, the DataSource and
   * Database are obtained from the results of the lookup operation for key.
   * @param options Options for validation operations to perform on opened connection.
   * @return An opened SqlConnection.
   */
  Connection openConnectionForKey(ValueT key, String connectionString,
      ConnectionOptions options);

  Callable<Connection> openConnectionForKeyAsync(ValueT key, String connectionString);

  /**
   * Given a key value, asynchronously obtains a SqlConnection to the shard in the mapping
   * that contains the key value.
   *
   * @param key Input key value.
   * @param connectionString Connection string with credential information, the DataSource and
   * Database are obtained from the results of the lookup operation for key.
   * @param options Options for validation operations to perform on opened connection.
   * @return An opened SqlConnection.
   */
  Callable<Connection> openConnectionForKeyAsync(ValueT key, String connectionString,
      ConnectionOptions options);

  /**
   * Adds a mapping.
   *
   * @param mapping Mapping being added.
   */
  MappingT add(MappingT mapping);

  /**
   * Removes a mapping.
   *
   * @param mapping Mapping being removed.
   * @param lockOwnerId Lock owner id of the mapping
   */
  void remove(MappingT mapping, UUID lockOwnerId);

  /**
   * Looks up the key value and returns the corresponding mapping.
   *
   * @param key Input key value.
   * @param useCache Whether to use cache for lookups.
   * @return Mapping that contains the key value.
   */
  MappingT lookup(ValueT key, boolean useCache);

  /**
   * Tries to looks up the key value and returns the corresponding mapping.
   *
   * @param key Input key value.
   * @param useCache Whether to use cache for lookups.
   * @param mapping Mapping that contains the key value.
   * @return <c>true</c> if mapping is found, <c>false</c> otherwise.
   */
  boolean tryLookup(ValueT key, boolean useCache, ReferenceObjectHelper<MappingT> mapping);
}