package com.microsoft.azure.elasticdb.shard.utils;

import java.util.Objects;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.schema.SchemaInfoException;
import com.microsoft.azure.elasticdb.shard.store.StoreException;

/**
 * Utility classes for exception and error handling.
 */
public final class ExceptionUtils {

    /**
     * Checks if given argument is null and if it is, throws an <see cref="ArgumentNullException"/>. <typeparam name="T">Type of value.</typeparam>
     *
     * @param value
     *            Value provided.
     * @param argName
     *            Name of argument whose value is provided in <paramref name="value"/>.
     */
    public static <T> void disallowNullArgument(T value,
            String argName) {
        if (value == null) {
            throw new IllegalArgumentException(argName);
        }
    }

    /**
     * Checks if given string argument is null or empty and if it is, throws an <see cref="ArgumentException"/>.
     *
     * @param s
     *            Input string.
     * @param argName
     *            Name of argument whose value is provided in <paramref name="s"/>.
     */
    public static void disallowNullOrEmptyStringArgument(String s,
            String argName) {
        if (StringUtilsLocal.isNullOrEmpty(s)) {
            throw new IllegalArgumentException(argName);
        }
    }

    /**
     * Ensures that the shard map and shard map manager information for given shard matches the one for current shard map.
     *
     * @param currentShardMapManager
     *            Current shard map manager.
     * @param currentShardMap
     *            Current shard map.
     * @param shard
     *            Input shard.
     * @param operation
     *            Operation being performed.
     * @param mappingType
     *            Type of mapping.
     */
    public static void ensureShardBelongsToShardMap(ShardMapManager currentShardMapManager,
            ShardMap currentShardMap,
            Shard shard,
            String operation,
            String mappingType) {
        // Ensure that shard is associated with current shard map.
        if (!shard.getShardMapId().equals(currentShardMap.getId())) {
            throw new IllegalStateException(StringUtilsLocal.formatInvariant(Errors._Shard_DifferentShardMap, shard.getValue(), mappingType,
                    currentShardMap.getName(), operation));
        }

        // Ensure that shard is associated with current shard map manager instance.
        if (!Objects.equals(shard.getShardMapManager(), currentShardMapManager)) {
            throw new IllegalStateException(StringUtilsLocal.formatInvariant(Errors._Shard_DifferentShardMapManager, shard.getValue(), mappingType,
                    currentShardMapManager.getCredentials().getShardMapManagerLocation(), operation));
        }
    }

    /**
     * Constructs a global store exception object based on the given input parameters.
     *
     * @param category
     *            Error category.
     * @param storeException
     *            Underlying store exception.
     * @param operationName
     *            Operation name.
     * @return ShardManagementException corresponding to the given store exception.
     */
    public static ShardManagementException getStoreExceptionGlobal(ShardManagementErrorCategory category,
            StoreException storeException,
            String operationName) {
        return new ShardManagementException(category, ShardManagementErrorCode.StorageOperationFailure, Errors._Store_SqlExceptionGlobal,
                storeException.getCause() != null ? storeException.getCause().getMessage() : storeException.getMessage(), storeException,
                operationName);
    }

    /**
     * Constructs a global store exception object based on the given input parameters.
     *
     * @param category
     *            Error category.
     * @param storeException
     *            Underlying store exception.
     * @param operationName
     *            Operation name.
     * @param location
     *            Location of server where error occurred.
     * @return ShardManagementException corresponding to the given store exception.
     */
    public static ShardManagementException getStoreExceptionLocal(ShardManagementErrorCategory category,
            StoreException storeException,
            String operationName,
            ShardLocation location) {
        return new ShardManagementException(category, ShardManagementErrorCode.StorageOperationFailure, Errors._Store_SqlExceptionLocal,
                storeException.getCause() != null ? storeException.getCause().getMessage() : storeException.getMessage(), storeException,
                operationName, location);
    }

    /**
     * Throw ShardManagementException or StoreException based on the current Exception thrown.
     *
     * @param e
     *            Current Exception thrown
     */
    public static void throwStronglyTypedException(Exception e) {
        // TODO: Handle this using subtype polymorphism instead of switch case with hardcoded strings
        Throwable cause = e.getCause() == null ? e : e.getCause();
        if (cause != null) {
            switch (cause.getClass().getSimpleName()) {
                case "ShardManagementException":
                    throw (ShardManagementException) cause;
                case "StoreException":
                    throw (StoreException) cause;
                case "SchemaInfoException":
                    throw (SchemaInfoException) cause;
                default:
                    e.printStackTrace();
            }
        }
    }
}