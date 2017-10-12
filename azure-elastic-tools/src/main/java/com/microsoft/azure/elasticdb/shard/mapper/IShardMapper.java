package com.microsoft.azure.elasticdb.shard.mapper;

import java.sql.Connection;
import java.util.UUID;
import java.util.concurrent.Callable;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.core.commons.helpers.ReferenceObjectHelper;
import com.microsoft.azure.elasticdb.shard.base.IShardProvider;
import com.microsoft.azure.elasticdb.shard.base.LookupOptions;

/**
 * Container for a collection of keys to shards mappings.
 */
public interface IShardMapper<MappingT extends IShardProvider, ValueT> {

    Connection openConnectionForKey(ValueT key,
            String connectionString);

    /**
     * Given a key value, obtains a SqlConnection to the shard in the mapping that contains the key value.
     *
     * @param key
     *            Input key value.
     * @param connectionString
     *            Connection string with credential information, the DataSource and Database are obtained from the results of the lookup operation for
     *            key.
     * @param options
     *            Options for validation operations to perform on opened connection.
     * @return An opened SqlConnection.
     */
    Connection openConnectionForKey(ValueT key,
            String connectionString,
            ConnectionOptions options);

    Callable<Connection> openConnectionForKeyAsync(ValueT key,
            String connectionString);

    /**
     * Given a key value, asynchronously obtains a SqlConnection to the shard in the mapping that contains the key value.
     *
     * @param key
     *            Input key value.
     * @param connectionString
     *            Connection string with credential information, the DataSource and Database are obtained from the results of the lookup operation for
     *            key.
     * @param options
     *            Options for validation operations to perform on opened connection.
     * @return An opened SqlConnection.
     */
    Callable<Connection> openConnectionForKeyAsync(ValueT key,
            String connectionString,
            ConnectionOptions options);

    /**
     * Adds a mapping.
     *
     * @param mapping
     *            Mapping being added.
     */
    MappingT add(MappingT mapping);

    /**
     * Removes a mapping.
     *
     * @param mapping
     *            Mapping being removed.
     * @param lockOwnerId
     *            Lock owner id of the mapping
     */
    void remove(MappingT mapping,
            UUID lockOwnerId);

    /**
     * Looks up the key value and returns the corresponding mapping.
     *
     * @param key
     *            Input key value.
     * @param lookupOptions
     *            Whether to use cache and/or storage for lookups.
     * @return Mapping that contains the key value.
     */
    MappingT lookup(ValueT key,
            LookupOptions lookupOptions);

    /**
     * Tries to looks up the key value and returns the corresponding mapping.
     *
     * @param key
     *            Input key value.
     * @param lookupOptions
     *            Whether to use cache and/or storage for lookups.
     * @param mapping
     *            Mapping that contains the key value.
     * @return <c>true</c> if mapping is found, <c>false</c> otherwise.
     */
    boolean tryLookup(ValueT key,
            LookupOptions lookupOptions,
            ReferenceObjectHelper<MappingT> mapping);
}