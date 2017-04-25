package com.microsoft.azure.elasticdb.shard.store;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

/**
 * Factory for store connections.
 */
public interface IStoreConnectionFactory {

  /**
   * Constructs a new instance of store connection.
   *
   * @param kind Type of store connection.
   * @param connectionString Connection string for store.
   * @return An unopened instance of the store connection.
   */
  IStoreConnection GetConnection(StoreConnectionKind kind, String connectionString);

  /**
   * Constructs a new instance of user connection.
   *
   * @param connectionString Connection string of user.
   * @return An unopened instance of the user connection.
   */
  IUserStoreConnection GetUserConnection(String connectionString);
}