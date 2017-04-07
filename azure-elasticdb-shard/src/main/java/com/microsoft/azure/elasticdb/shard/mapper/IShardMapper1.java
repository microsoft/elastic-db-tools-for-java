package com.microsoft.azure.elasticdb.shard.mapper;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.sqlserver.jdbc.SQLServerConnection;

import java.util.concurrent.Callable;

/**
 * Container for a collection of keys to shards mappings.
 * Can provide connection to a shard given a key.
 * <p>
 * <typeparam name="TKey">Key type.</typeparam>
 */
public interface IShardMapper1<TKey> extends IShardMapper {

    SQLServerConnection OpenConnectionForKey(TKey key, String connectionString);

    /**
     * Given a key value, obtains a SqlConnection to the shard in the mapping
     * that contains the key value.
     *
     * @param key              Input key value.
     * @param connectionString Connection string with credential information, the DataSource and Database are
     *                         obtained from the results of the lookup operation for key.
     * @param options          Options for validation operations to perform on opened connection.
     * @return An opened SqlConnection.
     */
    SQLServerConnection OpenConnectionForKey(TKey key, String connectionString, ConnectionOptions options);

    Callable<SQLServerConnection> OpenConnectionForKeyAsync(TKey key, String connectionString);

    /**
     * Given a key value, asynchronously obtains a SqlConnection to the shard in the mapping
     * that contains the key value.
     *
     * @param key              Input key value.
     * @param connectionString Connection string with credential information, the DataSource and Database are
     *                         obtained from the results of the lookup operation for key.
     * @param options          Options for validation operations to perform on opened connection.
     * @return An opened SqlConnection.
     */
    Callable<SQLServerConnection> OpenConnectionForKeyAsync(TKey key, String connectionString, ConnectionOptions options);
}