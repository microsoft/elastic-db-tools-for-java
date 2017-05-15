package com.microsoft.azure.elasticdb.shard.sqlstore;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreException;
import com.microsoft.azure.elasticdb.shard.store.StoreResult;
import com.microsoft.azure.elasticdb.shard.store.StoreResults;
import com.microsoft.azure.elasticdb.shard.store.StoreShard;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.store.StoreTransactionScopeKind;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationInput;
import com.microsoft.azure.elasticdb.shard.utils.Errors;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Types;
import java.util.List;
import java.util.concurrent.Callable;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.transform.sax.SAXResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scope of a transactional operation. Operations within scope happen atomically.
 */
public class SqlStoreTransactionScope implements IStoreTransactionScope {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final JAXBContext context;

  /**
   * Connection used for operation.
   */
  private Connection conn;
  /**
   * Transaction used for operation.
   */
  private int tran;
  /**
   * Type of transaction scope.
   */
  private StoreTransactionScopeKind kind;
  /**
   * Property used to mark successful completion of operation. The transaction
   * will be committed if this is <c>true</c> and rolled back if this is <c>false</c>.
   */
  private boolean success;

  /**
   * Constructs an instance of an atom transaction scope.
   *
   * @param kind Type of transaction scope.
   * @param conn Connection to use for the transaction scope.
   */
  protected SqlStoreTransactionScope(StoreTransactionScopeKind kind, Connection conn) {
    this.kind = kind;
    this.conn = conn;
    try {
      context = JAXBContext
          .newInstance(StoreOperationInput.class, StoreShard.class, StoreShardMap.class);
    } catch (JAXBException e) {
      throw new RuntimeException(e);
    }
    try {
      switch (this.getKind()) {
        case ReadOnly:
          conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
          break;
        case ReadWrite:
          conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
          break;
        default:
          // Do not start any transaction.
          //conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
          assert this.getKind() == StoreTransactionScopeKind.NonTransactional;
          break;
      }
      tran = conn.getTransactionIsolation();
    } catch (SQLException e) {
      e.printStackTrace();
      throw new StoreException(Errors._Store_StoreException, e);
    }
  }

  /**
   * Convert StoreOperationInput XML to string.
   *
   * @param jaxbContext JAXBContext
   * @param o StoreOperationInput
   * @return StoreOperationInput as String
   * @throws JAXBException Exception if unable to convert
   */
  public static String asString(JAXBContext jaxbContext, Object o) throws JAXBException {

    java.io.StringWriter sw = new StringWriter();

    Marshaller marshaller = jaxbContext.createMarshaller();
    marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
    marshaller.marshal(o, sw);

    return sw.toString();
  }

  public final StoreTransactionScopeKind getKind() {
    return kind;
  }

  public boolean getSuccess() {
    return success;
  }

  public void setSuccess(boolean value) {
    success = value;
  }

  /**
   * Executes the given stored procedure using the <paramref name="operationData"/> values
   * as the parameter and a single output parameter.
   *
   * @param operationName Operation to execute.
   * @param jaxbElement Input data for operation.
   * @return Storage results object.
   */
  public StoreResults executeOperation(String operationName, JAXBElement jaxbElement) {
    try {
      if (this.tran != 0) {
        conn.setAutoCommit(false);
      }
      try (CallableStatement cstmt = conn
          .prepareCall(String.format("{call %s(?,?)}", operationName))) {
        SQLXML sqlxml = conn.createSQLXML();

        // Set the result value from SAX events.
        SAXResult sxResult = sqlxml.setResult(SAXResult.class);
        context.createMarshaller().marshal(jaxbElement, sxResult);
        //log.info("Xml:{}\n{}", operationName, asString(context, jaxbElement));

        cstmt.setSQLXML("input", sqlxml);
        cstmt.registerOutParameter("result", Types.INTEGER);
        Boolean hasResults = cstmt.execute();
        StoreResults storeResults = SqlResults.newInstance(cstmt);
        // After iterating resultSet's, get result integer.
        int result = cstmt.getInt("result");
        storeResults.setResult(StoreResult.forValue(result));
        if (tran != 0) {
          if (storeResults.getResult() == StoreResult.Success
              || storeResults.getResult() == StoreResult.ShardPendingOperation) {
            conn.commit();
          } else {
            conn.rollback();
          }
        }
        /*log.info("hasResults:{} StoreResults:{}", hasResults,
            org.apache.commons.lang.builder.ReflectionToStringBuilder.toString(storeResults));//*/

        return storeResults;
      } catch (Exception e) {
        if (tran != 0) {
          conn.rollback();
        }
        log.error("Exception in sql transaction.", e);
      }
    } catch (SQLException e) {
      log.error("SQLException in sql transaction.", e);
    }
    return null;
  }

  /**
   * Asynchronously executes the given operation using the <paramref name="operationData"/> values
   * as input to the operation.
   *
   * @param operationName Operation to execute.
   * @param operationData Input data for operation.
   * @return Task encapsulating storage results object.
   */
  public Callable<StoreResults> executeOperationAsync(String operationName,
      JAXBElement operationData) {
    return () -> executeOperation(operationName, operationData);
  }

  /**
   * Executes the given command.
   *
   * @param command Command to execute.
   * @return Storage results object.
   */
  public StoreResults executeCommandSingle(StringBuilder command) {
    try {
      if (this.tran != 0) {
        conn.setAutoCommit(false);
      }
      StoreResults storeResults = null;
      try (CallableStatement stmt = conn.prepareCall(command.toString())) {
        Boolean hasResult = stmt.execute();
        if (hasResult) {
          storeResults = SqlResults.newInstance(stmt);
        } else {
          log.error(
              "Command Returned NULL!\r\nCommand: " + command.toString().replace("\r\n", "\\r\\n"));
        }
        if (tran != 0) {
          if (storeResults != null && storeResults.getResult() == StoreResult.Success) {
            conn.commit();
            return storeResults;
          } else {
            conn.rollback();
          }
        }
      } catch (SQLException ex) {
        if (tran != 0) {
          conn.rollback();
        }
      }
    } catch (SQLException ex) {
      log.error("Error in executing command.", ex);
    }
    return new StoreResults();
  }

  /**
   * Executes the given set of commands.
   *
   * @param commands Collection of commands to execute.
   */
  public void executeCommandBatch(List<StringBuilder> commands) {
    try {
      if (this.tran != 0) {
        conn.setAutoCommit(false);
      }
      for (StringBuilder batch : commands) {
        try (CallableStatement stmt = conn.prepareCall(batch.toString())) {
          stmt.execute();
        } catch (SQLException ex) {
          log.error("Error in executing command: " + batch.toString(), ex);
          if (this.tran != 0) {
            conn.rollback();
            return;
          }
        }
      }
      if (tran != 0) {
        conn.commit();
      }
    } catch (SQLException ex) {
      log.error("Error in executing command.", ex);
    }
  }

  @Override
  public void close() throws Exception {
    //TODO
    /*if (tran != null) {
        SqlUtils.WithSqlExceptionHandling(() -> {
            try {
                if (this.getSuccess()) {
                    tran.Commit();
                } else {
                    tran.Rollback();
                }
            } catch (IllegalStateException e) {
                // We ignore zombied transactions.
            } finally {
                tran.Dispose();
                tran = null;
            }
        });
    }*/
  }
}
