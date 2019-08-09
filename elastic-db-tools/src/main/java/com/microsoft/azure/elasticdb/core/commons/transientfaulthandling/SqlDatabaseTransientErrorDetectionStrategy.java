package com.microsoft.azure.elasticdb.core.commons.transientfaulthandling;

/*
 * Elastic database tools for Azure SQL Database.
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 * 
 * Notes: This class was forked from the Windows Azure Transient Fault Handling Library ("Topaz") available here: http://topaz.codeplex.com/ and will
 * now be maintained by Microsoft. In the future, we should consider moving this to a config file to make updates easier in-case WA Sql Database
 * decides to change their error codes.
 */

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.microsoft.azure.elasticdb.core.commons.helpers.EnumHelpers;
import com.microsoft.azure.elasticdb.core.commons.helpers.MappableEnum;

/**
 * Provides the transient error detection logic for transient faults that are specific to SQL Database.
 */
public final class SqlDatabaseTransientErrorDetectionStrategy implements ITransientErrorDetectionStrategy {

    /**
     * Determines whether the specified exception represents a transient failure that can be compensated by a retry.
     *
     * @param ex
     *            The exception object to be verified.
     * @return true if the specified exception is considered as transient; otherwise, false.
     */
    @Override
    public boolean isTransient(Exception ex) {
        // TODO: Complete this method.
        if (ex != null) {
            SQLException sqlException;
            Exception e = ex.getCause() != null ? (Exception) ex.getCause() : ex;
            if ((sqlException = (SQLException) ((e instanceof SQLException) ? e : null)) != null) {
                // Enumerate through all errors found in the exception.
                // for (Iterator err : sqlException.iterator()) {
                switch (sqlException.getErrorCode()) {
                    // SQL Error Code: 40501
                    // The service is currently busy. Retry the request after 10 seconds. Code:
                    // (reason code to be decoded).
                    /*
                     * case ThrottlingCondition.ThrottlingErrorNumber: // Decode the reason code from the error message to determine the grounds for
                     * throttling. var condition = ThrottlingCondition.FromError(err);
                     * 
                     * // Attach the decoded values as additional attributes to the original SQL exception.
                     * sqlException.Data[condition.ThrottlingMode.getClass().getSimpleName()] = condition.ThrottlingMode.toString();
                     * sqlException.Data[condition.getClass().getSimpleName()] = condition;
                     * 
                     * return true;
                     */

                    // SQL Error Code: 10928
                    // Resource ID: %d. The %s limit for the database is %d and has been reached.
                    case 10928:
                        // SQL Error Code: 10929
                        // Resource ID: %d. The %s minimum guarantee is %d, maximum limit is %d and the current
                        // usage for the database is %d. However, the server is currently too busy to support
                        // requests greater than %d for this database.
                    case 10929:
                        // SQL Error Code: 10053
                        // A transport-level error has occurred when receiving results from the server.
                        // An established connection was aborted by the software in your host machine.
                    case 10053:
                        // SQL Error Code: 10054
                        // A transport-level error has occurred when sending the request to the server.
                        // (provider: TCP Provider, error: 0 - An existing connection was forcibly closed
                        // by the remote host.)
                    case 10054:
                        // SQL Error Code: 10060
                        // A network-related or instance-specific error occurred while establishing a connection
                        // to SQL Server The server was not found or was not accessible. Verify that the
                        // instance name is correct and that SQL Server is configured to allow remote
                        // connections. (provider: TCP Provider, error: 0 - A connection attempt failed because
                        // the connected party did not properly respond after a period of time, or established
                        // connection failed because connected host has failed to respond.)"}
                    case 10060:
                        // SQL Error Code: 18401
                        // Login failed for user '%s'. Reason: Server is in script upgrade mode. Only
                        // administrator can connect at this time.
                        // Devnote: this can happen when SQL is going through recovery (e.g. after failover)
                    case 18401:
                        // SQL Error Code: 40197
                        // The service has encountered an error processing your request. Please try again.
                    case 40197:
                        // SQL Error Code: 40540
                        // The service has encountered an error processing your request. Please try again.
                    case 40540:
                        // SQL Error Code: 40613
                        // Database XXXX on server YYYY is not currently available.
                        // Please retry the connection later. If the problem persists, contact customer support,
                        // and provide them the session tracing ID of ZZZZZ.
                    case 40613:
                        // SQL Error Code: 40143
                        // The service has encountered an error processing your request. Please try again.
                    case 40143:
                        // SQL Error Code: 233
                        // The client was unable to establish a connection because of an error during connection
                        // initialization process before login. Possible causes include the following:
                        // the client tried to connect to an unsupported version of SQL Server;
                        // the server was too busy to accept new connections; or
                        // there was a resource limitation (insufficient memory or maximum allowed connections)
                        // on the server.
                        // (provider: TCP Provider, error: 0 - An existing connection was forcibly closed by
                        // the remote host.)
                    case 233:
                        // SQL Error Code: 64
                        // A connection was successfully established with the server, but then an error occurred
                        // during the login process.
                        // (provider: TCP Provider, error: 0 - The specified network name is no longer
                        // available.)
                    case 64:
                        // DBNETLIB Error Code: 20
                        // The instance of SQL Server you attempted to connect to does not support encryption.
                        // case ProcessNetLibErrorCode.EncryptionNotSupported.getValue():
                    default:
                        return true;
                }
                // }

                // Prelogin failure can happen due to waits expiring on windows handles. Or
                // due to a bug in the gateway code, a dropped database with a pooled connection
                // when reset results in a timeout error instead of immediate failure.
                /*
                 * Win32Exception wex = (Win32Exception) ((sqlException.getCause() instanceof Win32Exception) ? sqlException.getCause() : null); if
                 * (wex != null) { switch (wex.NativeErrorCode) { // Timeout expired case 0x102: return true;
                 * 
                 * // Semaphore timeout expired case 0x121: return true; } }
                 */
            }
            else if (ex instanceof TimeoutException) {
                return true;
            }
        }

        return false;
    }

    /**
     * Error codes reported by the DBNETLIB module.
     */
    private enum ProcessNetLibErrorCode implements MappableEnum {
        ZeroBytes(-3),

        Timeout(-2),
        /*
         * Timeout expired. The timeout period elapsed prior to completion of the operation or the server is not responding.
         */

        Unknown(-1),

        InsufficientMemory(1),

        AccessDenied(2),

        ConnectionBusy(3),

        ConnectionBroken(4),

        ConnectionLimit(5),

        ServerNotFound(6),

        NetworkNotFound(7),

        InsufficientResources(8),

        NetworkBusy(9),

        NetworkAccessDenied(10),

        GeneralError(11),

        IncorrectMode(12),

        NameNotFound(13),

        InvalidConnection(14),

        ReadWriteError(15),

        TooManyHandles(16),

        ServerError(17),

        SSLError(18),

        EncryptionError(19),

        EncryptionNotSupported(20);

        private static final Map<Integer, ProcessNetLibErrorCode> mappings = EnumHelpers.createMap(ProcessNetLibErrorCode.class);
        private int intValue;

        ProcessNetLibErrorCode(int value) {
            intValue = value;
        }

        public static ProcessNetLibErrorCode forValue(int value) {
            return mappings.get(value);
        }

        public int getValue() {
            return intValue;
        }
    }
}