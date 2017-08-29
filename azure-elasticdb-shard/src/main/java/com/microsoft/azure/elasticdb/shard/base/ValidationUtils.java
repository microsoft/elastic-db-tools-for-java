package com.microsoft.azure.elasticdb.shard.base;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.google.common.base.Stopwatch;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlResults;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreResult;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreShard;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationErrorHandler;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationInput;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder;
import java.lang.invoke.MethodHandles;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Types;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.transform.sax.SAXResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ValidationUtils {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Performs validation that the local representation is as up-to-date
   * as the representation on the backing data store.
   *
   * @param conn Connection used for validation.
   * @param shardMapManager ShardMapManager reference.
   * @param shardMap Shard map for the mapping.
   * @param storeMapping Mapping to validate.
   */
  public static void validateMapping(Connection conn, ShardMapManager shardMapManager,
      StoreShardMap shardMap, StoreMapping storeMapping) {
    Stopwatch stopwatch = Stopwatch.createStarted();

    StoreResults lsmResult = new StoreResults();

    JAXBElement jaxbElement = StoreOperationRequestBuilder.validateShardMappingLocal(
        shardMap.getId(), storeMapping.getId());

    try (CallableStatement cstmt = conn.prepareCall(String.format("{call %s(?,?)}",
        StoreOperationRequestBuilder.SP_VALIDATE_SHARD_MAPPING_LOCAL))) {
      SQLXML sqlxml = conn.createSQLXML();

      JAXBContext context = JAXBContext.newInstance(StoreOperationInput.class, StoreShard.class,
          StoreShardMap.class);
      // Set the result value from SAX events.
      SAXResult sxResult = sqlxml.setResult(SAXResult.class);
      context.createMarshaller().marshal(jaxbElement, sxResult);
      /*log.info("Xml:{}\n{}", "ValidateShardMappingLocal",
          SqlStoreTransactionScope.asString(context, jaxbElement));//*/

      cstmt.setSQLXML("input", sqlxml);
      cstmt.registerOutParameter("result", Types.INTEGER);
      Boolean hasResults = cstmt.execute();
      StoreResults storeResults = SqlResults.newInstance(cstmt);
      // After iterating resultSet's, get result integer.
      int result = cstmt.getInt("result");
      lsmResult.setResult(StoreResult.forValue(result));
    } catch (SQLException | JAXBException e) {
      e.printStackTrace();
    }

    stopwatch.stop();

    try {
      log.info("Shard ValidateMapping Complete; Shard: {}; Connection: {}; Result:{}; Duration: {}",
          storeMapping.getStoreShard().getLocation(), conn.getMetaData().getURL(),
          lsmResult.getResult(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
    } catch (SQLException e) {
      e.printStackTrace();
    }

    if (lsmResult.getResult() != StoreResult.Success) {
      if (lsmResult.getResult() == StoreResult.ShardMapDoesNotExist) {
        shardMapManager.getCache().deleteShardMap(shardMap);
      } else {
        if (lsmResult.getResult() == StoreResult.MappingDoesNotExist) {
          // Only evict from cache is mapping is no longer present,
          // for Offline mappings, we don't even retry, so same request
          // will continue to go to the LSM.
          shardMapManager.getCache().deleteMapping(storeMapping);
        }
      }

      // Possible errors are:
      // StoreResult.ShardMapDoesNotExist
      // StoreResult.MappingDoesNotExist
      // StoreResult.MappingIsOffline
      // StoreResult.ShardVersionMismatch
      // StoreResult.StoreVersionMismatch
      // StoreResult.MissingParametersForStoredProcedure
      throw StoreOperationErrorHandler.onValidationErrorLocal(lsmResult, shardMap,
          storeMapping.getStoreShard().getLocation(), "ValidateMapping",
          StoreOperationRequestBuilder.SP_VALIDATE_SHARD_LOCAL);
    }

    assert lsmResult.getResult() == StoreResult.Success;
  }

  /**
   * Asynchronously performs validation that the local representation is as up-to-date
   * as the representation on the backing data store.
   *
   * @param conn Connection used for validation.
   * @param shardMapManager ShardMapManager reference.
   * @param shardMap Shard map for the mapping.
   * @param storeMapping Mapping to validate.
   * @return A task to await validation completion
   */
  public static Callable validateMappingAsync(Connection conn,
      ShardMapManager shardMapManager, StoreShardMap shardMap, StoreMapping storeMapping) {
    return () -> {
      validateMapping(conn, shardMapManager, shardMap, storeMapping);
      return null;
    };
  }

  /**
   * Performs validation that the local representation is as
   * up-to-date as the representation on the backing data store.
   *
   * @param conn Connection used for validation.
   * @param shardMapManager ShardMapManager reference.
   * @param shardMap Shard map for the shard.
   * @param shard Shard to validate.
   */
  public static void validateShard(Connection conn, ShardMapManager shardMapManager,
      StoreShardMap shardMap, StoreShard shard) {
    Stopwatch stopwatch = Stopwatch.createStarted();

    StoreResults lsmResult = new StoreResults();

    JAXBElement jaxbElement = StoreOperationRequestBuilder.validateShardLocal(shardMap.getId(),
        shard.getId(), shard.getVersion());

    try (CallableStatement cstmt = conn.prepareCall(String.format("{call %s(?,?)}",
        StoreOperationRequestBuilder.SP_VALIDATE_SHARD_LOCAL))) {
      SQLXML sqlxml = conn.createSQLXML();

      JAXBContext context = JAXBContext.newInstance(StoreOperationInput.class, StoreShard.class,
          StoreShardMap.class);
      // Set the result value from SAX events.
      SAXResult sxResult = sqlxml.setResult(SAXResult.class);
      context.createMarshaller().marshal(jaxbElement, sxResult);
      /*log.info("Xml:{}\n{}", "ValidateShardLocal",
          SqlStoreTransactionScope.asString(context, jaxbElement));//*/

      cstmt.setSQLXML("input", sqlxml);
      cstmt.registerOutParameter("result", Types.INTEGER);
      Boolean hasResults = cstmt.execute();
      StoreResults storeResults = SqlResults.newInstance(cstmt);
      // After iterating resultSet's, get result integer.
      int result = cstmt.getInt("result");
      lsmResult.setResult(StoreResult.forValue(result));

      stopwatch.stop();

      log.info("Shard ValidateShard",
          "Complete; Shard: {}; Connection: {}; Result: {}; Duration: {}", shard.getLocation(),
          conn.getMetaData().getURL(), lsmResult.getResult(),
          stopwatch.elapsed(TimeUnit.MILLISECONDS));
    } catch (SQLException | JAXBException e) {
      e.printStackTrace();
    }

    if (lsmResult.getResult() != StoreResult.Success) {
      if (lsmResult.getResult() == StoreResult.ShardMapDoesNotExist) {
        shardMapManager.getCache().deleteShardMap(shardMap);
      }

      // Possible errors are:
      // StoreResult.ShardMapDoesNotExist
      // StoreResult.ShardDoesNotExist
      // StoreResult.ShardVersionMismatch
      // StoreResult.StoreVersionMismatch
      // StoreResult.MissingParametersForStoredProcedure
      throw StoreOperationErrorHandler.onValidationErrorLocal(lsmResult, shardMap,
          shard.getLocation(), "ValidateShard",
          StoreOperationRequestBuilder.SP_VALIDATE_SHARD_LOCAL);
    }
  }

  /**
   * Asynchronously performs validation that the local representation is as
   * up-to-date as the representation on the backing data store.
   *
   * @param conn Connection used for validation.
   * @param shardMapManager ShardMapManager reference.
   * @param shardMap Shard map for the shard.
   * @param shard Shard to validate.
   * @return A task to await validation completion
   */
  public static Callable validateShardAsync(Connection conn,
      ShardMapManager shardMapManager, StoreShardMap shardMap, StoreShard shard) {
    return () -> {
      validateShard(conn, shardMapManager, shardMap, shard);
      return null;
    };
  }
}
