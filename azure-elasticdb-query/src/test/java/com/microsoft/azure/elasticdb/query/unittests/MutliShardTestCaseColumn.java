package com.microsoft.azure.elasticdb.query.unittests;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

/**
 * Purpose:
 * Utility class that maintains column information about the test columns in our
 * sharded database table.
 * Notes:
 * Aim is to centralize the column information to make it easier
 * to perform exhaustive testing on our value getters and conversions, etc.
 */
public class MutliShardTestCaseColumn {
//
//  /**
//   * Private static member that holds our defined test columns.
//   */
//  private static List<MutliShardTestCaseColumn> s_definedColumns;
//
//  /**
//   * Private static memeber that holds our pseudo column.
//   */
//  private static MutliShardTestCaseColumn s_shardNamePseudoColumn;
//
//  /**
//   * Private immutable object member that holds the SqlServer engine type.
//   */
//  private String _sqlServerDatabaseEngineType;
//
//  /**
//   * The text column type declaration to use.
//   */
//  private String _columnTypeDeclaration;
//
//  /**
//   * The SqlDbType for this column.
//   */
//  private SqlDbType _dbType;
//
//  /**
//   * The field length (if applicable).
//   */
//  private int _fieldLength;
//
//  /**
//   * The name to use when creating the column in the test database.
//   */
//  private String _testColumnName;
//
//  /**
//   * Private, and only, c-tor for MutliShardTestCaseColumn objects.  It is private only
//   * so that we can tightly control the columns that appear in our test code.
//   *
//   * @param engineType String representing the SQL Server Engine data type name.
//   * @param columnTypeDeclaration The text to use when declaring the column data type when setting
//   * up our test tables.
//   * @param dbType SqlDbType enum value for this column.
//   * @param fieldLength The max length to pull for variable length accessors (e.g., GetChars or
//   * GetBytes).
//   */
//  private MutliShardTestCaseColumn(String engineType, String columnTypeDeclaration,
//      SqlDbType dbType, int fieldLength, String testColumnName) {
//    _sqlServerDatabaseEngineType = engineType;
//    _columnTypeDeclaration = columnTypeDeclaration;
//    _dbType = dbType;
//    _fieldLength = fieldLength;
//    _testColumnName = testColumnName;
//  }
//
//  /**
//   * Static getter that exposes our defined test case columns.
//   */
//  public static List<MutliShardTestCaseColumn> getDefinedColumns() {
//    if (null == s_definedColumns) {
//      s_definedColumns = GenerateColumns();
//    }
//    return s_definedColumns;
//  }
//
//  /**
//   * Static getter that exposes our shard name pseudo column.
//   */
//  public static MutliShardTestCaseColumn getShardNamePseudoColumn() {
//    if (null == s_shardNamePseudoColumn) {
//      s_shardNamePseudoColumn = GenerateShardNamePseudoColumn();
//    }
//    return s_shardNamePseudoColumn;
//  }
//
//  /**
//   * Public getter that exposes the SqlServer Engine data type name for this column.
//   */
//  public final String getSqlServerDatabaseEngineType() {
//    return _sqlServerDatabaseEngineType;
//  }
//
//  /**
//   * Getter that exposes the string to use to declare the column data type
//   * when creating the test table.  Useful shortcut for allowing us to specify
//   * type length parameters directly without pawing through data structures to generate
//   * them on the fly.
//   */
//  public final String getColumnTypeDeclaration() {
//    return _columnTypeDeclaration;
//  }
//
//  /**
//   * Getter that exposes the field length (if applicable) for the data type when accessing it.
//   * Useful shortcut for allowing us to pull variable length data without pawing
//   * through column type data structures.
//   */
//  public final int getFieldLength() {
//    return _fieldLength;
//  }
//
//  /**
//   * Getter that exposes the SqlDbType for the column type when accessing it.
//   * Useful shortcut for allowing us to pull type information without pawing
//   * through column type data structures.
//   */
//  public final SqlDbType getDbType() {
//    return _dbType;
//  }
//
//  /**
//   * Getter that gives us the column name for this column in the test database.
//   */
//  public final String getTestColumnName() {
//    return _testColumnName;
//  }
//
//  /**
//   * Static helper to produce the $ShardName pseudo column for use in comparisons when testing.
//   */
//  private static MutliShardTestCaseColumn GenerateShardNamePseudoColumn() {
//    return new MutliShardTestCaseColumn("nvarchar", "nvarchar(4000)", SqlDbType.NVarChar, 4000,
//        "$ShardName");
//  }
//
//  /**
//   * Static helper to generate the columns we will test.
//   */
//  private static List<MutliShardTestCaseColumn> GenerateColumns() {
//    ArrayList<MutliShardTestCaseColumn> theColumns = new ArrayList<MutliShardTestCaseColumn>();
//
//    String sqlName;
//    int length;
//    String fieldDecl;
//    SqlDbType dbType;
//
//    // bigint
//    sqlName = "bigint";
//    length = -1;
//    fieldDecl = sqlName;
//    dbType = SqlDbType.BigInt;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // binary
//    sqlName = "binary";
//    length = 100;
//    fieldDecl = String.format("%1$s(%2$s)", sqlName, length);
//    dbType = SqlDbType.Binary;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // bit
//    sqlName = "bit";
//    length = -1;
//    fieldDecl = sqlName;
//    dbType = SqlDbType.Bit;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // char
//    sqlName = "char";
//    length = 50;
//    fieldDecl = String.format("%1$s(%2$s)", sqlName, length);
//    dbType = SqlDbType.Char;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // date
//    sqlName = "date";
//    length = -1;
//    fieldDecl = sqlName;
//    dbType = SqlDbType.Date;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // datetime
//    sqlName = "datetime";
//    length = -1;
//    fieldDecl = sqlName;
//    dbType = SqlDbType.DateTime;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // datetime2
//    sqlName = "datetime2";
//    length = -1;
//    fieldDecl = sqlName;
//    dbType = SqlDbType.DateTime2;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // datetimeoffset
//    sqlName = "datetimeoffset";
//    length = -1;
//    fieldDecl = sqlName;
//    dbType = SqlDbType.DateTimeOffset;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // decimal
//    sqlName = "decimal";
//    length = 38;
//    fieldDecl = String.format("%1$s(%2$s)", sqlName, length);
//    dbType = SqlDbType.Decimal;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // FILESTREAM/varbinary(max)
//
//    // float
//    sqlName = "float";
//    length = -1;
//    fieldDecl = sqlName;
//    dbType = SqlDbType.Float;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // image
//    sqlName = "image";
//    length = 75;
//    fieldDecl = sqlName;
//    dbType = SqlDbType.Image;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // int
//    sqlName = "int";
//    length = -1;
//    fieldDecl = sqlName;
//    dbType = SqlDbType.Int;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // money
//    sqlName = "money";
//    length = -1;
//    fieldDecl = sqlName;
//    dbType = SqlDbType.Money;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // nchar
//    sqlName = "nchar";
//    length = 255;
//    fieldDecl = String.format("%1$s(%2$s)", sqlName, length);
//    dbType = SqlDbType.NChar;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // ntext
//    sqlName = "ntext";
//    length = 20;
//    fieldDecl = sqlName;
//    dbType = SqlDbType.NText;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // numeric
//    sqlName = "numeric";
//    length = 38;
//    fieldDecl = String.format("%1$s(%2$s)", sqlName, length);
//    dbType = SqlDbType.Decimal;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // nvarchar
//    sqlName = "nvarchar";
//    length = 10;
//    fieldDecl = String.format("%1$s(%2$s)", sqlName, length);
//    dbType = SqlDbType.NVarChar;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // real
//    sqlName = "real";
//    length = -1;
//    fieldDecl = sqlName;
//    dbType = SqlDbType.Real;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // rowversion
//
//    // smalldatetime
//    sqlName = "smalldatetime";
//    length = -1;
//    fieldDecl = sqlName;
//    dbType = SqlDbType.SmallDateTime;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // smallint
//    sqlName = "smallint";
//    length = -1;
//    fieldDecl = sqlName;
//    dbType = SqlDbType.SmallInt;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // smallmoney
//    sqlName = "smallmoney";
//    length = -1;
//    fieldDecl = sqlName;
//    dbType = SqlDbType.SmallMoney;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // sql_variant
//
//    // text
//    sqlName = "text";
//    length = 17;
//    fieldDecl = sqlName;
//    dbType = SqlDbType.Text;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // time
//    sqlName = "time";
//    length = -1;
//    fieldDecl = sqlName;
//    dbType = SqlDbType.Time;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // timestamp
//    sqlName = "timestamp";
//    length = 8;
//    fieldDecl = sqlName;
//    dbType = SqlDbType.Timestamp;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // tinyint
//    sqlName = "tinyint";
//    length = -1;
//    fieldDecl = sqlName;
//    dbType = SqlDbType.TinyInt;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // uniqueidentifier
//    sqlName = "uniqueidentifier";
//    length = -1;
//    fieldDecl = sqlName;
//    dbType = SqlDbType.UniqueIdentifier;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // varbinary
//    sqlName = "varbinary";
//    length = 4;
//    fieldDecl = String.format("%1$s(%2$s)", sqlName, length);
//    dbType = SqlDbType.VarBinary;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // varchar
//    sqlName = "varchar";
//    length = 13;
//    fieldDecl = String.format("%1$s(%2$s)", sqlName, length);
//    dbType = SqlDbType.VarChar;
//    AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);
//
//    // xml
//
//    return new ReadOnlyCollection<MutliShardTestCaseColumn>(theColumns);
//  }
//
//  /**
//   * Static helper to package up the relevant info into a MutliShardTestCaseColumn object.
//   *
//   * @param sqlTypeName The SQL Server database engine data type sqlTypeName.
//   * @param sqlFieldDeclarationText The text to use to create this column in SQL Server.
//   * @param dbType The SqlDbType for this column.
//   * @param length The length of the column (usefulf for char/binary/etc.
//   * @param listToAddTo The list to add the newly created column into.
//   */
//  private static void AddColumnToList(String sqlTypeName, String sqlFieldDeclarationText,
//      SqlDbType dbType, int length, ArrayList<MutliShardTestCaseColumn> listToAddTo) {
//    String colName = GenerateTestColumnName(sqlTypeName);
//    MutliShardTestCaseColumn toAdd = new MutliShardTestCaseColumn(sqlTypeName,
//        sqlFieldDeclarationText, dbType, length, colName);
//    listToAddTo.add(toAdd);
//  }
//
//  /**
//   * Static helper to auto-generate test column names.
//   *
//   * @param sqlTypeName The sql server type sqlTypeName of the column (e.g., numeric).
//   * @return A string formatted as Test_{0}_Field where {0} is the type sqlTypeName parameter.
//   */
//  private static String GenerateTestColumnName(String sqlTypeName) {
//    return String.format("Test_%1$s_Field", sqlTypeName);
//  }
}