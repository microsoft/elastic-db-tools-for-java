package com.microsoft.azure.elasticdb.query.logging;

/**
 * Summary: Provides a description of the results of the query and its effect on the database.
 */
public enum CommandBehavior {

    /**
     * The query may return multiple result sets. Execution of the query may affect the database state. Default sets no CommandBehavior flags, so
     * calling executeQuery(CommandBehavior.Default) is functionally equivalent to calling executeQuery().
     */
    Default(0),

    /**
     * The query returns a single result set.
     */
    SingleResult(1),

    /**
     * The query returns column information only. When using CommandBehavior.SchemaOnly, the .NET Framework Data Provider for SQL Server precedes the
     * statement being executed with SET FMTONLY ON.
     */
    SchemaOnly(2),

    /**
     * The query returns column and primary key information.
     */
    KeyInfo(4),

    /**
     * The query is expected to return a single row of the first result set. Execution of the query may affect the database state. Some .NET Framework
     * data providers may, but are not required to, use this information to optimize the performance of the command. It is possible to specify
     * SingleRow when executing queries that are expected to return multiple result sets. In that case, where both a multi-result set SQL query and
     * single row are specified, the result returned will contain only the first row of the first result set. The other result sets of the query will
     * not be returned.
     */
    SingleRow(8),

    /**
     * Provides a way for the ResultSet to handle rows that contain columns with large binary values. Rather than loading the entire row,
     * SequentialAccess enables the ResultSet to load data as a stream. You can then use the GetBytes or GetChars method to specify a byte location to
     * start the read operation, and a limited buffer size for the data being returned.
     */
    SequentialAccess(16),

    /**
     * When the command is executed, the associated Connection object is closed when the associated ResultSet object is closed.
     */
    CloseConnection(32);

    private int intValue;

    CommandBehavior(int value) {
        intValue = value;
    }

    public int getValue() {
        return intValue;
    }
}
