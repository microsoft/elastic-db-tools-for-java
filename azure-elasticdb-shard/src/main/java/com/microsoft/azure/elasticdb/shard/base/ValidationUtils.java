package com.microsoft.azure.elasticdb.shard.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.google.common.base.Stopwatch;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.*;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationErrorHandler;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public final class ValidationUtils {
    private final static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * Performs validation that the local representation is as up-to-date
     * as the representation on the backing data store.
     *
     * @param conn         Connection used for validation.
     * @param manager      ShardMapManager reference.
     * @param shardMap     Shard map for the mapping.
     * @param storeMapping Mapping to validate.
     */
    public static void ValidateMapping(Connection conn, ShardMapManager manager, StoreShardMap shardMap, StoreMapping storeMapping) {
        Stopwatch stopwatch = Stopwatch.createStarted();

        StoreResults lsmResult = new StoreResults();

        //TODO
        /*XElement xeValidate = StoreOperationRequestBuilder.ValidateShardMappingLocal(shardMap.getId(), storeMapping.getId());

        try (SqlCommand cmd = conn.CreateCommand()) {
            try (XmlReader input = xeValidate.CreateReader()) {
                cmd.CommandText = StoreOperationRequestBuilder.SpValidateShardMappingLocal;
                cmd.CommandType = CommandType.StoredProcedure;

                SqlUtils.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, new SqlXml(input));

                SqlParameter resultParam = SqlUtils.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                try (SqlDataReader reader = cmd.ExecuteReader()) {
                    lsmResult.newInstance(reader);
                }

                // Output parameter will be used to specify the outcome.
                lsmResult.getResult() = (StoreResult) resultParam.Value;
            }
        }*/

        stopwatch.stop();

        try {
            log.info("Shard ValidateMapping Complete; Shard: {}; Connection: {}; Result:{}; Duration: {}", storeMapping.getStoreShard().getLocation(), conn.getMetaData().getURL(), lsmResult.getResult(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (lsmResult.getResult() != StoreResult.Success) {
            if (lsmResult.getResult() == StoreResult.ShardMapDoesNotExist) {
                manager.getCache().DeleteShardMap(shardMap);
            } else {
                if (lsmResult.getResult() == StoreResult.MappingDoesNotExist) {
                    // Only evict from cache is mapping is no longer present,
                    // for Offline mappings, we don't even retry, so same request
                    // will continue to go to the LSM.
                    manager.getCache().DeleteMapping(storeMapping);
                }
            }

            // Possible errors are:
            // StoreResult.ShardMapDoesNotExist
            // StoreResult.MappingDoesNotExist
            // StoreResult.MappingIsOffline
            // StoreResult.ShardVersionMismatch
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnValidationErrorLocal(lsmResult, shardMap, storeMapping.getStoreShard().getLocation(), "ValidateMapping", StoreOperationRequestBuilder.SpValidateShardLocal);
        }

        assert lsmResult.getResult() == StoreResult.Success;
    }

    /**
     * Asynchronously performs validation that the local representation is as up-to-date
     * as the representation on the backing data store.
     *
     * @param conn         Connection used for validation.
     * @param manager      ShardMapManager reference.
     * @param shardMap     Shard map for the mapping.
     * @param storeMapping Mapping to validate.
     * @return A task to await validation completion
     */
    public static Callable ValidateMappingAsync(Connection conn, ShardMapManager manager, StoreShardMap shardMap, StoreMapping storeMapping) {
        Stopwatch stopwatch = Stopwatch.createStarted();

        StoreResults lsmResult = new StoreResults();

        /*XElement xeValidate = StoreOperationRequestBuilder.ValidateShardMappingLocal(shardMap.getId(), storeMapping.getId());

        try (SqlCommand cmd = conn.CreateCommand()) {
            try (XmlReader input = xeValidate.CreateReader()) {
                cmd.CommandText = StoreOperationRequestBuilder.SpValidateShardMappingLocal;
                cmd.CommandType = CommandType.StoredProcedure;

                SqlUtils.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, new SqlXml(input));

                SqlParameter resultParam = SqlUtils.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                try (SqlDataReader reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false)){
//TODO TASK: There is no equivalent to 'await' in Java:
                    await lsmResult.FetchAsync(reader).ConfigureAwait(false);
                }

                // Output parameter will be used to specify the outcome.
                lsmResult.getResult() = (StoreResult) resultParam.Value;
            }
        }*/

        stopwatch.stop();

        try {
            log.info("Shard ValidateMappingAsync", "Complete; Shard: {}; Connection: {}; Result: {}; Duration: {}", storeMapping.getStoreShard().getLocation(), conn.getMetaData().getURL(), lsmResult.getResult(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (lsmResult.getResult() != StoreResult.Success) {
            if (lsmResult.getResult() == StoreResult.ShardMapDoesNotExist) {
                manager.getCache().DeleteShardMap(shardMap);
            } else if (lsmResult.getResult() == StoreResult.MappingDoesNotExist || lsmResult.getResult() == StoreResult.MappingIsOffline) {
                manager.getCache().DeleteMapping(storeMapping);
            }

            // Possible errors are:
            // StoreResult.ShardMapDoesNotExist
            // StoreResult.MappingDoesNotExist
            // StoreResult.MappingIsOffline
            // StoreResult.ShardVersionMismatch
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnValidationErrorLocal(lsmResult, shardMap, storeMapping.getStoreShard().getLocation(), "ValidateMappingAsync", StoreOperationRequestBuilder.SpValidateShardLocal);
        }

        assert lsmResult.getResult() == StoreResult.Success;
        return null;
    }

    /**
     * Performs validation that the local representation is as
     * up-to-date as the representation on the backing data store.
     *
     * @param conn     Connection used for validation.
     * @param manager  ShardMapManager reference.
     * @param shardMap Shard map for the shard.
     * @param shard    Shard to validate.
     */
    public static void ValidateShard(Connection conn, ShardMapManager manager, StoreShardMap shardMap, StoreShard shard) {
        Stopwatch stopwatch = Stopwatch.createStarted();

        StoreResults lsmResult = new StoreResults();

        /*XElement xeValidate = StoreOperationRequestBuilder.ValidateShardLocal(shardMap.getId(), shard.getId(), shard.getVersion());

        try (SqlCommand cmd = conn.CreateCommand()) {
            try (XmlReader input = xeValidate.CreateReader()) {
                cmd.CommandText = StoreOperationRequestBuilder.SpValidateShardLocal;
                cmd.CommandType = CommandType.StoredProcedure;

                SqlUtils.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, new SqlXml(input));

                SqlParameter resultParam = SqlUtils.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                try (SqlDataReader reader = cmd.ExecuteReader()) {
                    lsmResult.newInstance(reader);
                }

                // Output parameter will be used to specify the outcome.
                lsmResult.getResult() = (StoreResult) resultParam.Value;
            }
        }*/

        stopwatch.stop();

        try {
            log.info("Shard ValidateShard", "Complete; Shard: {}; Connection: {}; Result: {}; Duration: {}", shard.getLocation(), conn.getMetaData().getURL(), lsmResult.getResult(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (lsmResult.getResult() != StoreResult.Success) {
            if (lsmResult.getResult() == StoreResult.ShardMapDoesNotExist) {
                manager.getCache().DeleteShardMap(shardMap);
            }

            // Possible errors are:
            // StoreResult.ShardMapDoesNotExist
            // StoreResult.ShardDoesNotExist
            // StoreResult.ShardVersionMismatch
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnValidationErrorLocal(lsmResult, shardMap, shard.getLocation(), "ValidateShard", StoreOperationRequestBuilder.SpValidateShardLocal);
        }
    }

    /**
     * Asynchronously performs validation that the local representation is as
     * up-to-date as the representation on the backing data store.
     *
     * @param conn     Connection used for validation.
     * @param manager  ShardMapManager reference.
     * @param shardMap Shard map for the shard.
     * @param shard    Shard to validate.
     * @return A task to await validation completion
     */
    public static Callable ValidateShardAsync(Connection conn, ShardMapManager manager, StoreShardMap shardMap, StoreShard shard) {
        Stopwatch stopwatch = Stopwatch.createStarted();

        StoreResults lsmResult = new StoreResults();

        /*XElement xeValidate = StoreOperationRequestBuilder.ValidateShardLocal(shardMap.getId(), shard.getId(), shard.getVersion());

        try (SqlCommand cmd = conn.CreateCommand()) {
            try (XmlReader input = xeValidate.CreateReader()) {
                cmd.CommandText = StoreOperationRequestBuilder.SpValidateShardLocal;
                cmd.CommandType = CommandType.StoredProcedure;

                SqlUtils.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, new SqlXml(input));

                SqlParameter resultParam = SqlUtils.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                try (SqlDataReader reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false)){
//TODO TASK: There is no equivalent to 'await' in Java:
                    await lsmResult.FetchAsync(reader).ConfigureAwait(false);
                }

                // Output parameter will be used to specify the outcome.
                lsmResult.getResult() = (StoreResult) resultParam.Value;
            }
        }*/

        stopwatch.stop();

        try {
            log.info("Shard ValidateShardAsync", "Complete; Shard: {}; Connection: {}; Result: {}; Duration: {}", shard.getLocation(), conn.getMetaData().getURL(), lsmResult.getResult(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (lsmResult.getResult() != StoreResult.Success) {
            if (lsmResult.getResult() == StoreResult.ShardMapDoesNotExist) {
                manager.getCache().DeleteShardMap(shardMap);
            }

            // Possible errors are:
            // StoreResult.ShardMapDoesNotExist
            // StoreResult.ShardDoesNotExist
            // StoreResult.ShardVersionMismatch
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnValidationErrorLocal(lsmResult, shardMap, shard.getLocation(), "ValidateShardAsync", StoreOperationRequestBuilder.SpValidateShardLocal);
        }
        return null;
    }
}
