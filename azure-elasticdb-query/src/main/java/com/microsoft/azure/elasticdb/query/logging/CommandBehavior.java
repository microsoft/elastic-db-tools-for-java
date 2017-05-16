package com.microsoft.azure.elasticdb.query.logging;

/**
 * Summary:
 * Provides a description of the results of the query and its effect on the database.
 */
public enum CommandBehavior {

  /**
   * Summary:
   * The query may return multiple result sets. Execution of the query may affect
   * the database state. Default sets no System.Data.CommandBehavior flags, so calling
   * ExecuteReader(CommandBehavior.Default) is functionally equivalent to calling
   * ExecuteReader().
   */
  Default(0),

  /**
   * Summary:
   * The query returns a single result set.
   */
  SingleResult(1),

  /**
   * Summary:
   * The query returns column information only. When using System.Data.CommandBehavior.SchemaOnly,
   * the .NET Framework Data Provider for SQL Server precedes the statement being
   * executed with SET FMTONLY ON.
   */
  SchemaOnly(2),

  /**
   * Summary:
   * The query returns column and primary key information.
   */
  KeyInfo(4),

  /**
   * Summary: The query is expected to return a single row of the first result set. Execution of the
   * query may affect the database state. Some .NET Framework data providers may, but are not
   * required to, use this information to optimize the performance of the command. When you specify
   * System.Data.CommandBehavior.SingleRow with the System.Data.OleDb.OleDbCommand.ExecuteReader
   * method of the System.Data.OleDb.OleDbCommand object, the .NET Framework Data Provider for OLE
   * DB performs binding using the OLE DB IRow interface if it is available. Otherwise, it uses the
   * IRowset interface. If your SQL statement is expected to return only a single row, specifying
   * System.Data.CommandBehavior.SingleRow can also improve application performance. It is possible
   * to specify SingleRow when executing queries that are expected to return multiple result sets.
   * In that case, where both a multi-result set SQL query and single row are specified, the result
   * returned will contain only the first row of the first result set. The other result sets of the
   * query will not be returned.
   */
  SingleRow(8),

  /**
   * Summary:
   * Provides a way for the DataReader to handle rows that contain columns with large
   * binary values. Rather than loading the entire row, SequentialAccess enables the
   * DataReader to load data as a stream. You can then use the GetBytes or GetChars
   * method to specify a byte location to start the read operation, and a limited
   * buffer size for the data being returned.
   */
  SequentialAccess(16),

  /**
   * Summary:
   * When the command is executed, the associated Connection object is closed when
   * the associated DataReader object is closed.
   */
  CloseConnection(32);

  public static final int SIZE = java.lang.Integer.SIZE;
  private static java.util.HashMap<Integer, CommandBehavior> mappings;
  private int intValue;

  CommandBehavior(int value) {
    intValue = value;
    getMappings().put(value, this);
  }

  private static java.util.HashMap<Integer, CommandBehavior> getMappings() {
    if (mappings == null) {
      synchronized (CommandBehavior.class) {
        if (mappings == null) {
          mappings = new java.util.HashMap<>();
        }
      }
    }
    return mappings;
  }

  public static CommandBehavior forValue(int value) {
    return getMappings().get(value);
  }

  public int getValue() {
    return intValue;
  }
}
