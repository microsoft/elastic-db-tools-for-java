package com.microsoft.azure.elasticdb.shard.base;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.store.IStoreMapping;
import com.microsoft.azure.elasticdb.shard.store.IStoreShard;
import com.microsoft.azure.elasticdb.shard.store.IStoreShardMap;
import javafx.concurrent.Task;

import java.sql.Connection;

public final class ValidationUtils {
    /**
     * The Tracer
     */
    private static ILogger getTracer() {
        return TraceHelper.Tracer;
    }

    /**
     * Performs validation that the local representation is as up-to-date
     * as the representation on the backing data store.
     *
     * @param conn         Connection used for validation.
     * @param manager      ShardMapManager reference.
     * @param shardMap     Shard map for the mapping.
     * @param storeMapping Mapping to validate.
     */
    public static void ValidateMapping(Connection conn, ShardMapManager manager, IStoreShardMap shardMap, IStoreMapping storeMapping) {
        Stopwatch stopwatch = Stopwatch.StartNew();

        SqlResults lsmResult = new SqlResults();

        XElement xeValidate = StoreOperationRequestBuilder.ValidateShardMappingLocal(shardMap.Id, storeMapping.Id);

        try (SqlCommand cmd = conn.CreateCommand()) {
            try (XmlReader input = xeValidate.CreateReader()) {
                cmd.CommandText = StoreOperationRequestBuilder.SpValidateShardMappingLocal;
                cmd.CommandType = CommandType.StoredProcedure;

                SqlUtils.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, new SqlXml(input));

                SqlParameter resultParam = SqlUtils.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                try (SqlDataReader reader = cmd.ExecuteReader()) {
                    lsmResult.Fetch(reader);
                }

                // Output parameter will be used to specify the outcome.
                lsmResult.Result = (StoreResult) resultParam.Value;
            }
        }

        stopwatch.Stop();

        getTracer().TraceInfo(TraceSourceConstants.ComponentNames.Shard, "ValidateMapping", "Complete; Shard: {0}; Connection: {1}; Result: {2}; Duration: {3}", storeMapping.StoreShard.Location, conn.ConnectionString, lsmResult.Result, stopwatch.Elapsed);

        if (lsmResult.Result != StoreResult.Success) {
            if (lsmResult.Result == StoreResult.ShardMapDoesNotExist) {
                manager.Cache.DeleteShardMap(shardMap);
            } else {
                if (lsmResult.Result == StoreResult.MappingDoesNotExist) {
                    // Only evict from cache is mapping is no longer present,
                    // for Offline mappings, we don't even retry, so same request
                    // will continue to go to the LSM.
                    manager.Cache.DeleteMapping(storeMapping);
                }
            }

            // Possible errors are:
            // StoreResult.ShardMapDoesNotExist
            // StoreResult.MappingDoesNotExist
            // StoreResult.MappingIsOffline
            // StoreResult.ShardVersionMismatch
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnValidationErrorLocal(lsmResult, shardMap, storeMapping.StoreShard.Location, "ValidateMapping", StoreOperationRequestBuilder.SpValidateShardLocal);
        }

        assert lsmResult.Result == StoreResult.Success;
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
    public static Task ValidateMappingAsync(Connection conn, ShardMapManager manager, IStoreShardMap shardMap, IStoreMapping storeMapping) {
        Stopwatch stopwatch = Stopwatch.StartNew();

        SqlResults lsmResult = new SqlResults();

        XElement xeValidate = StoreOperationRequestBuilder.ValidateShardMappingLocal(shardMap.Id, storeMapping.Id);

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
                lsmResult.Result = (StoreResult) resultParam.Value;
            }
        }

        stopwatch.Stop();

        getTracer().TraceInfo(TraceSourceConstants.ComponentNames.Shard, "ValidateMappingAsync", "Complete; Shard: {0}; Connection: {1}; Result: {2}; Duration: {3}", storeMapping.StoreShard.Location, conn.ConnectionString, lsmResult.Result, stopwatch.Elapsed);

        if (lsmResult.Result != StoreResult.Success) {
            if (lsmResult.Result == StoreResult.ShardMapDoesNotExist) {
                manager.Cache.DeleteShardMap(shardMap);
            } else if (lsmResult.Result == StoreResult.MappingDoesNotExist || lsmResult.Result == StoreResult.MappingIsOffline) {
                manager.Cache.DeleteMapping(storeMapping);
            }

            // Possible errors are:
            // StoreResult.ShardMapDoesNotExist
            // StoreResult.MappingDoesNotExist
            // StoreResult.MappingIsOffline
            // StoreResult.ShardVersionMismatch
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnValidationErrorLocal(lsmResult, shardMap, storeMapping.StoreShard.Location, "ValidateMappingAsync", StoreOperationRequestBuilder.SpValidateShardLocal);
        }

        assert lsmResult.Result == StoreResult.Success;
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
    public static void ValidateShard(Connection conn, ShardMapManager manager, IStoreShardMap shardMap, IStoreShard shard) {
        Stopwatch stopwatch = Stopwatch.StartNew();

        SqlResults lsmResult = new SqlResults();

        XElement xeValidate = StoreOperationRequestBuilder.ValidateShardLocal(shardMap.Id, shard.Id, shard.Version);

        try (SqlCommand cmd = conn.CreateCommand()) {
            try (XmlReader input = xeValidate.CreateReader()) {
                cmd.CommandText = StoreOperationRequestBuilder.SpValidateShardLocal;
                cmd.CommandType = CommandType.StoredProcedure;

                SqlUtils.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, new SqlXml(input));

                SqlParameter resultParam = SqlUtils.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                try (SqlDataReader reader = cmd.ExecuteReader()) {
                    lsmResult.Fetch(reader);
                }

                // Output parameter will be used to specify the outcome.
                lsmResult.Result = (StoreResult) resultParam.Value;
            }
        }

        stopwatch.Stop();

        getTracer().TraceInfo(TraceSourceConstants.ComponentNames.Shard, "ValidateShard", "Complete; Shard: {0}; Connection: {1}; Result: {2}; Duration: {3}", shard.Location, conn.ConnectionString, lsmResult.Result, stopwatch.Elapsed);

        if (lsmResult.Result != StoreResult.Success) {
            if (lsmResult.Result == StoreResult.ShardMapDoesNotExist) {
                manager.Cache.DeleteShardMap(shardMap);
            }

            // Possible errors are:
            // StoreResult.ShardMapDoesNotExist
            // StoreResult.ShardDoesNotExist
            // StoreResult.ShardVersionMismatch
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnValidationErrorLocal(lsmResult, shardMap, shard.Location, "ValidateShard", StoreOperationRequestBuilder.SpValidateShardLocal);
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
//TODO TASK: There is no equivalent in Java to the 'async' keyword:
//ORIGINAL LINE: internal static async Task ValidateShardAsync(SqlConnection conn, ShardMapManager manager, IStoreShardMap shardMap, IStoreShard shard)
    public static Task ValidateShardAsync(Connection conn, ShardMapManager manager, IStoreShardMap shardMap, IStoreShard shard) {
        Stopwatch stopwatch = Stopwatch.StartNew();

        SqlResults lsmResult = new SqlResults();

        XElement xeValidate = StoreOperationRequestBuilder.ValidateShardLocal(shardMap.Id, shard.Id, shard.Version);

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
                lsmResult.Result = (StoreResult) resultParam.Value;
            }
        }

        stopwatch.Stop();

        getTracer().TraceInfo(TraceSourceConstants.ComponentNames.Shard, "ValidateShardAsync", "Complete; Shard: {0}; Connection: {1}; Result: {2}; Duration: {3}", shard.Location, conn.ConnectionString, lsmResult.Result, stopwatch.Elapsed);

        if (lsmResult.Result != StoreResult.Success) {
            if (lsmResult.Result == StoreResult.ShardMapDoesNotExist) {
                manager.Cache.DeleteShardMap(shardMap);
            }

            // Possible errors are:
            // StoreResult.ShardMapDoesNotExist
            // StoreResult.ShardDoesNotExist
            // StoreResult.ShardVersionMismatch
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnValidationErrorLocal(lsmResult, shardMap, shard.Location, "ValidateShardAsync", StoreOperationRequestBuilder.SpValidateShardLocal);
        }
    }
}
