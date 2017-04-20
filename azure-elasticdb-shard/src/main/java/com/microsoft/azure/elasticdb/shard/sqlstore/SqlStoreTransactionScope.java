package com.microsoft.azure.elasticdb.shard.sqlstore;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.store.*;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.transform.sax.SAXResult;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.sql.*;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Scope of a transactional operation. Operations within scope happen atomically.
 */
public class SqlStoreTransactionScope implements IStoreTransactionScope {
    private final static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final JAXBContext context;

    /**
     * Connection used for operation.
     */
    private Connection _conn;
    /**
     * Transaction used for operation.
     */
    private int _tran;
    /**
     * Type of transaction scope.
     */
    private StoreTransactionScopeKind Kind;
    /**
     * Property used to mark successful completion of operation. The transaction
     * will be committed if this is <c>true</c> and rolled back if this is <c>false</c>.
     */
    private boolean Success;

    /**
     * Constructs an instance of an atom transaction scope.
     *
     * @param kind Type of transaction scope.
     * @param conn Connection to use for the transaction scope.
     */
    protected SqlStoreTransactionScope(StoreTransactionScopeKind kind, Connection conn) {
        Kind = kind;
        this._conn = conn;
        try {
            context = JAXBContext.newInstance(StoreOperationInput.class, StoreShard.class, StoreShardMap.class);
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
            _tran = conn.getTransactionIsolation();
        } catch (SQLException e) {
            e.printStackTrace();
            //TODO: Handle Exception
        }
    }

    public final StoreTransactionScopeKind getKind() {
        return Kind;
    }

    public boolean getSuccess() {
        return Success;
    }

    public void setSuccess(boolean value) {
        Success = value;
    }

    /**
     * Executes the given stored procedure using the <paramref name="operationData"/> values
     * as the parameter and a single output parameter.
     *
     * @param operationName Operation to execute.
     * @param jaxbElement   Input data for operation.
     * @return Storage results object.
     */
    public StoreResults ExecuteOperation(String operationName, JAXBElement jaxbElement) {
        try {
            if (this._tran != 0) {
                _conn.setAutoCommit(false);
            }
            try (CallableStatement cstmt = _conn.prepareCall(String.format("{call %s(?,?)}", operationName))) {
                SQLXML sqlxml = _conn.createSQLXML();

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
                if (_tran != 0) {
                    if (storeResults.getResult() == StoreResult.Success
                            || storeResults.getResult() == StoreResult.ShardPendingOperation)
                        _conn.commit();
                    else
                        _conn.rollback();
                }
                /*log.info("hasResults:{} StoreResults:{}", hasResults,
                        org.apache.commons.lang.builder.ReflectionToStringBuilder.toString(storeResults));//*/

                return storeResults;
            } catch (Exception e) {
                if (_tran != 0) {
                    _conn.rollback();
                }
                log.error("Exception in sql transaction.", e);
            }
        } catch (SQLException e) {
            log.error("SQLException in sql transaction.", e);
        }
        return null;
    }

    public String asString(JAXBContext pContext,
                           Object pObject)
            throws
            JAXBException {

        java.io.StringWriter sw = new StringWriter();

        Marshaller marshaller = pContext.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
        marshaller.marshal(pObject, sw);

        return sw.toString();
    }

    /**
     * Asynchronously executes the given operation using the <paramref name="operationData"/> values
     * as input to the operation.
     *
     * @param operationName Operation to execute.
     * @param operationData Input data for operation.
     * @return Task encapsulating storage results object.
     */
    public Callable<StoreResults> ExecuteOperationAsync(String operationName, JAXBElement operationData) {
        return () -> ExecuteOperation(operationName, operationData);
    }

    /**
     * Executes the given command.
     *
     * @param command Command to execute.
     * @return Storage results object.
     */
    public StoreResults ExecuteCommandSingle(StringBuilder command) {
        try {
            if (this._tran != 0) {
                _conn.setAutoCommit(false);
            }
            StoreResults storeResults = null;
            try (CallableStatement stmt = _conn.prepareCall(command.toString())) {
                Boolean hasResult = stmt.execute();
                if (hasResult) {
                    storeResults = SqlResults.newInstance(stmt);
                } else {
                    log.error("Command Returned NULL!\r\nCommand: " + command.toString().replace("\r\n", "\\r\\n"));
                }
                if (_tran != 0) {
                    if (storeResults != null && storeResults.getResult() == StoreResult.Success) {
                        _conn.commit();
                        return storeResults;
                    } else {
                        _conn.rollback();
                    }
                }
            } catch (SQLException ex) {
                if (_tran != 0) {
                    _conn.rollback();
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
    public void ExecuteCommandBatch(List<StringBuilder> commands) {
        try {
            if (this._tran != 0) {
                _conn.setAutoCommit(false);
            }
            for (StringBuilder batch : commands) {
                try (CallableStatement stmt = _conn.prepareCall(batch.toString())) {
                    stmt.execute();
                } catch (SQLException ex) {
                    log.error("Error in executing command: " + batch.toString(), ex);
                    if (this._tran != 0) {
                        _conn.rollback();
                        return;
                    }
                }
            }
            if (_tran != 0) {
                _conn.commit();
            }
        } catch (SQLException ex) {
            log.error("Error in executing command.", ex);
        }
    }

    ///#region IDisposable

    /**
     * Disposes the object. Commits or rolls back the transaction.
     */
    public final void Dispose() {
        this.Dispose(true);
        //TODO GC.SuppressFinalize(this);
    }

    /**
     * Performs actual Dispose of resources.
     *
     * @param disposing Whether the invocation was from IDisposable.Dipose method.
     */
    protected void Dispose(boolean disposing) {
        if (disposing) {
            //TODO
            /*if (_tran != null) {
                SqlUtils.WithSqlExceptionHandling(() -> {
                    try {
                        if (this.getSuccess()) {
                            _tran.Commit();
                        } else {
                            _tran.Rollback();
                        }
                    } catch (IllegalStateException e) {
                        // We ignore zombied transactions.
                    } finally {
                        _tran.Dispose();
                        _tran = null;
                    }
                });
            }*/
        }
    }

    @Override
    public void close() throws Exception {

    }
}
