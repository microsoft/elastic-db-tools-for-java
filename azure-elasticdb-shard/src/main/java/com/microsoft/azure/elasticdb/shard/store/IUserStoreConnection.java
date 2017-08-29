package com.microsoft.azure.elasticdb.shard.store;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import java.sql.Connection;

/**
 * Instance of a user connection to store.
 */
public interface IUserStoreConnection extends java.io.Closeable {

  /**
   * Underlying SQL server connection.
   */
  Connection getConnection();

}