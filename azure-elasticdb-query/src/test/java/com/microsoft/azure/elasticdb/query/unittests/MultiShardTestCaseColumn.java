package com.microsoft.azure.elasticdb.query.unittests;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Purpose:
 * Utility class that maintains column information about the test columns in our
 * sharded database table.
 * Notes:
 * Aim is to centralize the column information to make it easier
 * to perform exhaustive testing on our value getters and conversions, etc.
 */
public class MultiShardTestCaseColumn {

  /**
   * Private static member that holds our defined test columns.
   */
  private static List<MultiShardTestCaseColumn> definedColumns;

  /**
   * Private static memeber that holds our pseudo column.
   */
  private static MultiShardTestCaseColumn shardNamePseudoColumn;

  /**
   * Private immutable object member that holds the SqlServer engine type.
   */
  private String sqlServerDatabaseEngineType;

  /**
   * The text column type declaration to use.
   */
  private String columnTypeDeclaration;

  /**
   * The Sql Type for this column.
   */
  private int dbType;

  /**
   * The field length (if applicable).
   */
  private int fieldLength;

  /**
   * The name to use when creating the column in the test database.
   */
  private String testColumnName;

  /**
   * Private, and only, c-tor for MultiShardTestCaseColumn objects.  It is private only
   * so that we can tightly control the columns that appear in our test code.
   *
   * @param engineType String representing the SQL Server Engine data type name.
   * @param columnTypeDeclaration The text to use when declaring the column data type when setting
   * up our test tables.
   * @param fieldLength The max length to pull for variable length accessors (e.g., GetChars or
   * GetBytes).
   */
  private MultiShardTestCaseColumn(String engineType, String columnTypeDeclaration, int dbType,
      int fieldLength, String testColumnName) {
    sqlServerDatabaseEngineType = engineType;
    this.columnTypeDeclaration = columnTypeDeclaration;
    this.fieldLength = fieldLength;
    this.dbType = dbType;
    this.testColumnName = testColumnName;
  }

  /**
   * Static getter that exposes our defined test case columns.
   */
  public static List<MultiShardTestCaseColumn> getDefinedColumns() {
    if (null == definedColumns) {
      definedColumns = generateColumns();
    }
    return definedColumns;
  }

  /**
   * Static getter that exposes our shard name pseudo column.
   */
  public static MultiShardTestCaseColumn getShardNamePseudoColumn() {
    if (null == shardNamePseudoColumn) {
      shardNamePseudoColumn = generateShardNamePseudoColumn();
    }
    return shardNamePseudoColumn;
  }

  /**
   * Static helper to produce the $ShardName pseudo column for use in comparisons when testing.
   */
  private static MultiShardTestCaseColumn generateShardNamePseudoColumn() {
    return new MultiShardTestCaseColumn("nvarchar", "nvarchar(4000)", Types.NVARCHAR, 4000,
        "$ShardName");
  }

  /**
   * Static helper to generate the columns we will test.
   */
  private static List<MultiShardTestCaseColumn> generateColumns() {
    List<MultiShardTestCaseColumn> theColumns = new ArrayList<>();

    String sqlName;
    int length;
    String fieldDecl;
    int dbType;

    // bigint
    sqlName = "bigint";
    length = -1;
    fieldDecl = sqlName;
    dbType = Types.BIGINT;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // binary
    sqlName = "binary";
    length = 100;
    fieldDecl = String.format("%1$s(%2$s)", sqlName, length);
    dbType = Types.BINARY;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // bit
    sqlName = "bit";
    length = -1;
    fieldDecl = sqlName;
    dbType = Types.BIT;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // char
    sqlName = "char";
    length = 50;
    fieldDecl = String.format("%1$s(%2$s)", sqlName, length);
    dbType = Types.CHAR;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // date
    sqlName = "date";
    length = -1;
    fieldDecl = sqlName;
    dbType = Types.DATE;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // datetime
    sqlName = "datetime";
    length = -1;
    fieldDecl = sqlName;
    dbType = Types.TIMESTAMP;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // datetime2
    sqlName = "datetime2";
    length = -1;
    fieldDecl = sqlName;
    dbType = Types.TIMESTAMP;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // datetimeoffset
    sqlName = "datetimeoffset";
    length = -1;
    fieldDecl = sqlName;
    dbType = microsoft.sql.Types.DATETIMEOFFSET;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // decimal
    sqlName = "decimal";
    length = 38;
    fieldDecl = String.format("%1$s(%2$s)", sqlName, length);
    dbType = Types.DECIMAL;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // FILESTREAM/varbinary(max)

    // float
    sqlName = "float";
    length = -1;
    fieldDecl = sqlName;
    dbType = Types.DOUBLE;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // image
    sqlName = "image";
    length = 75;
    fieldDecl = sqlName;
    dbType = Types.LONGVARBINARY;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // int
    sqlName = "int";
    length = -1;
    fieldDecl = sqlName;
    dbType = Types.INTEGER;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // money
    sqlName = "money";
    length = -1;
    fieldDecl = sqlName;
    dbType = Types.DECIMAL;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // nchar
    sqlName = "nchar";
    length = 255;
    fieldDecl = String.format("%1$s(%2$s)", sqlName, length);
    dbType = Types.NCHAR;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // ntext
    sqlName = "ntext";
    length = 20;
    fieldDecl = sqlName;
    dbType = Types.LONGNVARCHAR;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // numeric
    sqlName = "numeric";
    length = 38;
    fieldDecl = String.format("%1$s(%2$s)", sqlName, length);
    dbType = Types.NUMERIC;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // nvarchar
    sqlName = "nvarchar";
    length = 10;
    fieldDecl = String.format("%1$s(%2$s)", sqlName, length);
    dbType = Types.NVARCHAR;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // real
    sqlName = "real";
    length = -1;
    fieldDecl = sqlName;
    dbType = Types.REAL;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // rowversion

    // smalldatetime
    sqlName = "smalldatetime";
    length = -1;
    fieldDecl = sqlName;
    dbType = Types.TIMESTAMP;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // smallint
    sqlName = "smallint";
    length = -1;
    fieldDecl = sqlName;
    dbType = Types.SMALLINT;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // smallmoney
    sqlName = "smallmoney";
    length = -1;
    fieldDecl = sqlName;
    dbType = Types.DECIMAL;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // sql_variant

    // text
    sqlName = "text";
    length = 17;
    fieldDecl = sqlName;
    dbType = Types.LONGVARCHAR;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // time
    sqlName = "time";
    length = -1;
    fieldDecl = sqlName;
    dbType = Types.TIME;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // timestamp
    sqlName = "timestamp";
    length = 8;
    fieldDecl = sqlName;
    dbType = Types.BINARY;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // tinyint
    sqlName = "tinyint";
    length = -1;
    fieldDecl = sqlName;
    dbType = Types.TINYINT;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // uniqueidentifier
    sqlName = "uniqueidentifier";
    length = -1;
    fieldDecl = sqlName;
    dbType = Types.CHAR;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // varbinary
    sqlName = "varbinary";
    length = 4;
    fieldDecl = String.format("%1$s(%2$s)", sqlName, length);
    dbType = Types.VARBINARY;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // varchar
    sqlName = "varchar";
    length = 13;
    fieldDecl = String.format("%1$s(%2$s)", sqlName, length);
    dbType = Types.VARCHAR;
    addColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

    // xml

    return new ArrayList<>(theColumns);
  }

  /**
   * Static helper to package up the relevant info into a MultiShardTestCaseColumn object.
   *
   * @param sqlTypeName The SQL Server database engine data type sqlTypeName.
   * @param sqlFieldDeclarationText The text to use to create this column in SQL Server.
   * @param length The length of the column (usefulf for char/binary/etc.
   * @param listToAddTo The list to add the newly created column into.
   */
  private static void addColumnToList(String sqlTypeName, String sqlFieldDeclarationText,
      int dbType, int length, List<MultiShardTestCaseColumn> listToAddTo) {
    String colName = generateTestColumnName(sqlTypeName);
    MultiShardTestCaseColumn toAdd = new MultiShardTestCaseColumn(sqlTypeName,
        sqlFieldDeclarationText, dbType, length, colName);
    listToAddTo.add(toAdd);
  }

  /**
   * Static helper to auto-generate test column names.
   *
   * @param sqlTypeName The sql server type sqlTypeName of the column (e.g., numeric).
   * @return A string formatted as Test_{0}_Field where {0} is the type sqlTypeName parameter.
   */
  private static String generateTestColumnName(String sqlTypeName) {
    return String.format("Test_%1$s_Field", sqlTypeName);
  }

  /**
   * Public getter that exposes the SqlServer Engine data type name for this column.
   */
  public final String getSqlServerDatabaseEngineType() {
    return sqlServerDatabaseEngineType;
  }

  /**
   * Getter that exposes the string to use to declare the column data type
   * when creating the test table.  Useful shortcut for allowing us to specify
   * type length parameters directly without pawing through data structures to generate
   * them on the fly.
   */
  public final String getColumnTypeDeclaration() {
    return columnTypeDeclaration;
  }

  /**
   * Getter that exposes the Sql Type for the column type when accessing it. Useful shortcut for
   * allowing us to pull type information without pawing through column type data structures.
   */
  public final int getDbType() {
    return dbType;
  }

  /**
   * Getter that exposes the field length (if applicable) for the data type when accessing it.
   * Useful shortcut for allowing us to pull variable length data without pawing
   * through column type data structures.
   */
  public final int getFieldLength() {
    return fieldLength;
  }

  /**
   * Getter that gives us the column name for this column in the test database.
   */
  public final String getTestColumnName() {
    return testColumnName;
  }
}