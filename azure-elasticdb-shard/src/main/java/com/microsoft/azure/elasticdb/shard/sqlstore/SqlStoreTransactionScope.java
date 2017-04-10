package com.microsoft.azure.elasticdb.shard.sqlstore;

// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import com.microsoft.azure.elasticdb.shard.store.IStoreResults;
import com.microsoft.azure.elasticdb.shard.store.IStoreTransactionScope;
import com.microsoft.azure.elasticdb.shard.store.StoreResult;
import com.microsoft.azure.elasticdb.shard.store.StoreTransactionScopeKind;
import com.microsoft.azure.elasticdb.shard.utils.XElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.sql.*;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Scope of a transactional operation. Operations within scope happen atomically.
 */
public class SqlStoreTransactionScope implements IStoreTransactionScope {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * Connection used for operation.
     */
    private Connection _conn;

    /**
     * Transaction used for operation.
     */
    //TODO private SqlTransaction _tran;
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
        this.setKind(kind);
        _conn = conn;

        switch (this.getKind()) {
            case ReadOnly:
                //TODO:
                /*SqlUtils.WithSqlExceptionHandling(() -> {
                    _tran = conn.BeginTransaction(IsolationLevel.ReadCommitted);
                });*/
                break;
            case ReadWrite:
                //TODO:
                /*SqlUtils.WithSqlExceptionHandling(() -> {
                    _tran = conn.BeginTransaction(IsolationLevel.RepeatableRead);
                });*/
                break;
            default:
                // Do not start any transaction.
                assert this.getKind() == StoreTransactionScopeKind.NonTransactional;
                break;
        }
    }

    public final StoreTransactionScopeKind getKind() {
        return Kind;
    }

    private void setKind(StoreTransactionScopeKind value) {
        Kind = value;
    }

    public boolean getSuccess() {
        return Success;
    }

    public void setSuccess(boolean value) {
        Success = value;
    }

    @Override
    public IStoreResults ExecuteOperation(String operationName, Object operationData) {
        return null;
    }

    @Override
    public Callable<IStoreResults> ExecuteOperationAsync(String operationName, Object operationData) {
        return null;
    }

    /**
     * Executes the given stored procedure using the <paramref name="operationData"/> values
     * as the parameter and a single output parameter.
     *
     * @param operationName Operation to execute.
     * @param operationData Input data for operation.
     * @return Storage results object.
     */
    public IStoreResults ExecuteOperation(String operationName, XElement operationData) {
        SqlResults sqlResults = new SqlResults();

        try (CallableStatement cstmt = _conn.prepareCall(String.format("{call %s(?)}", operationName))) {
            cstmt.setSQLXML("@input", null);
            cstmt.registerOutParameter("@result", Types.INTEGER);
            cstmt.execute();
            int result = cstmt.getInt("@result");
            sqlResults.setResult(StoreResult.forValue(result));
            sqlResults.FetchAsync(cstmt);
        } catch (Exception e) {
            log.error("Error in sql transaction.", e);
        }
        return sqlResults;

        /*return SqlUtils.<IStoreResults>WithSqlExceptionHandling(() -> {
            SqlResults results = new SqlResults();

            try (SqlCommand cmd = _conn.CreateCommand()) {
                try (XmlReader input = operationData.CreateReader()) {
                    cmd.Transaction = _tran;
                    cmd.CommandText = operationName;
                    cmd.CommandType = CommandType.StoredProcedure;

                    SqlUtils.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, -1, new SqlXml(input));

                    SqlParameter result = SqlUtils.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    try (SqlDataReader reader = cmd.ExecuteReader()) {
                        results.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    results.getResult() = (StoreResult) result.Value;
                }
            }

            return results;
        });*/
    }

    /**
     * Asynchronously executes the given operation using the <paramref name="operationData"/> values
     * as input to the operation.
     *
     * @param operationName Operation to execute.
     * @param operationData Input data for operation.
     * @return Task encapsulating storage results object.
     */
    public Callable<IStoreResults> ExecuteOperationAsync(String operationName, XElement operationData) {
        // TODO
        return null;
        /*return SqlUtils.<IStoreResults>WithSqlExceptionHandlingAsync(async() ->{
            SqlResults results = new SqlResults();

            try (SqlCommand cmd = _conn.CreateCommand()) {
                try (XmlReader input = operationData.CreateReader()) {
                    cmd.Transaction = _tran;
                    cmd.CommandText = operationName;
                    cmd.CommandType = CommandType.StoredProcedure;

                    SqlUtils.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, -1, new SqlXml(input));

                    SqlParameter result = SqlUtils.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    try (SqlDataReader reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false)){
                        await results.FetchAsync(reader).ConfigureAwait(false);
                    }

                    // Output parameter will be used to specify the outcome.
                    results.Result = (StoreResult) result.Value;
                }
            }

            return results;
        });*/
    }

    /**
     * Executes the given command.
     *
     * @param command Command to execute.
     * @return Storage results object.
     */
    public IStoreResults ExecuteCommandSingle(StringBuilder command) {
        SqlResults sqlResults = new SqlResults();
        try(CallableStatement stmt = _conn.prepareCall(command.toString())) {
            Boolean hasResult = stmt.execute();
            ResultSet rs = stmt.getResultSet();
            if (hasResult && rs != null) {
                sqlResults.Fetch(rs);
            } else {
                log.error("Command Returned NULL!\r\nCommand: " + command.toString());
            }
        } catch (SQLException ex) {
            log.error("Error in executing command.", ex);
        }
        return sqlResults;
    }

    /**
     * Executes the given set of commands.
     *
     * @param commands Collection of commands to execute.
     */
    public void ExecuteCommandBatch(List<StringBuilder> commands) {
        for (StringBuilder batch : commands) {
            /*SqlUtils.WithSqlExceptionHandling(() -> {
                try (SqlCommand cmd = _conn.CreateCommand()) {
                    cmd.Transaction = _tran;
                    cmd.CommandText = batch.toString();
                    cmd.CommandType = CommandType.Text;

                    cmd.ExecuteNonQuery();
                }
            });*/
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
