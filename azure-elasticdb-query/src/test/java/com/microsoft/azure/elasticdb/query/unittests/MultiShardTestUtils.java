package com.microsoft.azure.elasticdb.query.unittests;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.google.common.base.Preconditions;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerCreateMode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerFactory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerLoadPolicy;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlConnectionStringBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * Common utilities used by tests.
 */
public final class MultiShardTestUtils {

  /**
   * SharedMapManager database name.
   */
  private static final String SHARD_MAP_MANAGER_DATABASE_NAME = "ShardMapManager";
  /**
   * Table name for the sharded table we will issue fanout queries against.
   */
  private static final String TABLE_NAME = "ConsistentShardedTable";
  /**
   * Field on the sharded table where we will store the database name.
   */
  private static final String DB_NAME_FIELD = "dbNameField";
  /**
   * Name of the test shard map to use.
   */
  private static final String SHARD_MAP_NAME = "TestShardMap";
  private static Properties properties = loadProperties();
  private static final String TEST_CONN_USER = properties.getProperty("TEST_CONN_USER");
  private static final String TEST_CONN_PASSWORD = properties.getProperty("TEST_CONN_PASSWORD");
  private static final String MULTI_SHARD_TEST_CONN_USER
      = properties.getProperty("MULTI_SHARD_TEST_CONN_USER");
  private static final String MULTI_SHARD_TEST_CONN_PASSWORD
      = properties.getProperty("MULTI_SHARD_TEST_CONN_PASSWORD");
  static final String MULTI_SHARD_CONN_STRING = multiShardConnectionString();
  /**
   * Name of the test server.
   */
  private static final String TEST_CONN_SERVER_NAME = properties
      .getProperty("TEST_CONN_SERVER_NAME");
  /**
   * SMM connection String.
   */
  private static final String DB_CONN_STRING = dbConnectionString();
  /**
   * Connection string for connecting to test server.
   */
  static final String MULTI_SHARD_TEST_CONN_STRING = multiShardTestConnectionString();
  /**
   * List containing the names of the test databases.
   */
  private static List<String> testDatabaseNames = generateTestDatabaseNames();
  /**
   * Class level Random object.
   */
  private static Random random = new Random();

  private static Properties loadProperties() {
    InputStream inStream = MultiShardTestUtils.class.getClassLoader()
        .getResourceAsStream("resources.properties");
    Properties prop = new Properties();
    if (inStream != null) {
      try {
        prop.load(inStream);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return prop;
  }

  /**
   * Connection string for global shard map manager.
   */
  private static String multiShardTestConnectionString() {
    SqlConnectionStringBuilder connStr = new SqlConnectionStringBuilder();
    connStr.setIntegratedSecurity(false);
    connStr.setDataSource(TEST_CONN_SERVER_NAME);
    connStr.setDatabaseName(SHARD_MAP_MANAGER_DATABASE_NAME);
    connStr.setUser(MULTI_SHARD_TEST_CONN_USER);
    connStr.setPassword(MULTI_SHARD_TEST_CONN_PASSWORD);
    return connStr.toString();
  }

  /**
   * Connection string for global shard map manager.
   */
  private static String multiShardConnectionString() {
    SqlConnectionStringBuilder connStr = new SqlConnectionStringBuilder();
    connStr.setIntegratedSecurity(false);
    connStr.setUser(MULTI_SHARD_TEST_CONN_USER);
    connStr.setPassword(MULTI_SHARD_TEST_CONN_PASSWORD);
    return connStr.toString();
  }

  /**
   * Connection string for global shard map manager.
   */
  private static String dbConnectionString() {
    SqlConnectionStringBuilder connStr = new SqlConnectionStringBuilder();
    connStr.setIntegratedSecurity(false);
    connStr.setDataSource(TEST_CONN_SERVER_NAME);
    connStr.setUser(TEST_CONN_USER);
    connStr.setPassword(TEST_CONN_PASSWORD);
    return connStr.toString();
  }

  /**
   * Helper to populate a list with our test database names.
   *
   * @return A new list containing the test database names.
   */
  private static List<String> generateTestDatabaseNames() {
    List<String> dbNames = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      dbNames.add(String.format("Test%1$s", i));
    }
    return dbNames;
  }

  /**
   * Create and populate the test databases with the data we expect for these unit tests to run
   * correctly.
   * Probably will need to change this to integrate with our test framework better. Will deal with
   * that down the line when the test framework issue has settled out more.
   */
  static void createAndPopulateTables() throws SQLException {
    List<String> commands = new ArrayList<>();
    for (String dbName : testDatabaseNames) {
      commands.add(String.format("USE %1$s;", dbName));

      // First create the table.
      String createTable = getTestTableCreateCommand();
      commands.add(createTable);

      // Then add the records.
      String[] insertValuesCommands = getInsertValuesCommands(3, dbName);
      commands.addAll(Arrays.asList(insertValuesCommands));
    }
    executeNonQueries("master", commands);
  }

  /**
   * Helper that constructs the sql script to create our test table.
   *
   * @return T-SQL to create our test table.
   */
  private static String getTestTableCreateCommand() {
    StringBuilder createCommand = new StringBuilder();

    // Set up the stem of the statement
    createCommand.append(String.format("CREATE TABLE %1$s (%2$s nvarchar(50)", TABLE_NAME,
        DB_NAME_FIELD));

    List<MultiShardTestCaseColumn> fieldInfo = MultiShardTestCaseColumn.getDefinedColumns();
    for (MultiShardTestCaseColumn curField : fieldInfo) {
      createCommand.append(String.format(", %1$s %2$s", curField.getTestColumnName(),
          curField.getColumnTypeDeclaration()));
    }

    createCommand.append(");");
    return createCommand.toString();
  }

  /**
   * Helper to generate random field data for a record in the test table.
   *
   * @param numCommands The number of records to generate random data for.
   * @param dbName The name of the database to put in the dbName column.
   * @return Array filled with the commands to execute to insert the data.
   */
  private static String[] getInsertValuesCommands(int numCommands, String dbName) {
    String[] commandsToReturn = new String[numCommands];

    StringBuilder insertCommand = new StringBuilder();
    List<MultiShardTestCaseColumn> fieldInfo = MultiShardTestCaseColumn.getDefinedColumns();

    for (int i = 0; i < numCommands; i++) {
      insertCommand.setLength(0);

      // Set up the stem, which includes putting the dbName in the dbNameColumn.
      insertCommand.append(String.format("INSERT INTO %1$s (%2$s", TABLE_NAME, DB_NAME_FIELD));

      // Now put in all the column names in the order we will do them.
      for (MultiShardTestCaseColumn curField : fieldInfo) {
        // SpecialCase: if we hit row version field let's skip it; it gets updated automatically.
        if (!isTimestampField(curField.getDbType())) {
          insertCommand.append(String.format(", %1$s", curField.getTestColumnName()));
        }
      }

      // Close the field list
      insertCommand.append(") ");

      // Now put in the VALUES stem
      insertCommand.append(String.format("VALUES ('%1$s'", dbName));

      // Now put in the individual field values
      for (MultiShardTestCaseColumn curField : fieldInfo) {
        // SpecialCase: if we hit row version field let's skip it - it gets updated automatically.
        if (!isTimestampField(curField.getDbType())) {
          String valueToPutIn = getTestFieldValue(curField);
          insertCommand.append(String.format(", %1$s", valueToPutIn));
        }
      }

      // Finally, close the values list, terminate the statement, and add it to the array.
      insertCommand.append(");");
      commandsToReturn[i] = insertCommand.toString();
    }
    return commandsToReturn;
  }

  /**
   * Helper to determine if a particular SqlDbType is a timestamp.
   */
  private static boolean isTimestampField(int curFieldType) {
    return curFieldType == Types.TIMESTAMP;
  }

  /**
   * Helper to generate a tsql fragement that will produce a random value of the given type to
   * insert into the test database.
   *
   * @param dataTypeInfo The datatype of the desired value.
   * @return The tsql fragment that will generate a random value of the desired type.
   */
  private static String getTestFieldValue(MultiShardTestCaseColumn dataTypeInfo) {
    int dbType = dataTypeInfo.getDbType();
    int length = dataTypeInfo.getFieldLength();

    switch (dbType) {
      case Types.BIGINT:
        // SQL Types: bigint
        return getRandomIntCastAsArg(dataTypeInfo);

      case Types.BINARY:
        // SQL Types: image
        return getRandomSqlBinaryValue(length);

      case Types.BIT:
        // SQL Types: bit
        return getRandomSqlBitValue();

      case Types.CHAR:
        // SQL Types: char[(n)]
        return getRandomSqlCharValue(length);

      case Types.DATE:
        // SQL Types: date
        return getRandomSqlDateValue();

      case microsoft.sql.Types.DATETIMEOFFSET:
        // SQL Types: datetimeoffset
        return getRandomSqlDatetimeoffsetValue();

      case Types.DECIMAL:
        // SQL Types: decimal, numeric
        // These are the same.
        return getRandomSqlDecimalValue(dataTypeInfo);

      case Types.DOUBLE:
        // SQL Types: float
        return getRandomSqlFloatValue(dataTypeInfo);

      case Types.LONGVARBINARY:
        // SQL Types: image
        return getRandomSqlBinaryValue(length);

      case Types.INTEGER:
        // SQL Types: int
        return getRandomSqlIntValue();

      case Types.NUMERIC:
        //SQL Types: numeric
        return getRandomSqlDecimalValue(dataTypeInfo);

      case Types.NCHAR:
        // SQL Types: nchar[(n)]
        return getRandomSqlNCharValue(length);

      case Types.LONGNVARCHAR:
        // SQL Types: ntext
        return getRandomSqlNCharValue(length);

      case Types.NVARCHAR:
        // SQL Types: nvarchar[(n)]
        return getRandomSqlNCharValue(length);

      case Types.REAL:
        // SQL Types: real
        return getRandomSqlRealValue(dataTypeInfo);

      case Types.SMALLINT:
        // SQL Types: smallint
        return getRandomSqlSmallIntValue(dataTypeInfo);

      case Types.LONGVARCHAR:
        // SQL Types: text
        return getRandomSqlCharValue(length);

      case Types.TIME:
        // SQL Types: time
        return getRandomSqlDatetimeCastAsArg(dataTypeInfo);

      case Types.TIMESTAMP:
        // SQL Types: rowversion, timestamp
        //exclding it should happen automatically.  should not be here. throw.
        throw new IllegalArgumentException("TIMESTAMP");

      case Types.TINYINT:
        // SQL Types: tinyint
        return getRandomSqlTinyIntValue();

      case Types.VARBINARY:
        // SQL Types: binary[(n)], varbinary[(n)]
        return getRandomSqlBinaryValue(length);

      case Types.VARCHAR:
        // SQL Types: varchar[(n)]
        return getRandomSqlCharValue(length);

      default:
        throw new IllegalArgumentException(Integer.toString(dbType));
    }
  }

  /**
   * Blow away (if necessary) and create fresh versions of the Test databases we expect for our unit
   * tests.
   * DEVNOTE (VSTS 2202802): we should move to a GUID-based naming scheme.
   */
  static void dropAndCreateDatabases() throws SQLException {
    List<String> commands = new ArrayList<>();

    // Set up the test user.
    addDropAndReCreateTestUserCommandsToList(commands);

    // Set up the test databases.
    addCommandsToManageTestDatabasesToList(true, commands);

    // Set up the ShardMapManager database.
    addDropAndCreateDatabaseCommandsToList(SHARD_MAP_MANAGER_DATABASE_NAME, commands);

    executeNonQueries("master", commands);
  }

  /**
   * Drop the test databases (if they exist) we expect for these unit tests.
   * DEVNOTE (VSTS 2202802): We should switch to a GUID-based naming scheme.
   */
  static void dropDatabases() throws SQLException {
    List<String> commands = new ArrayList<>();

    // Drop the test databases.
    addCommandsToManageTestDatabasesToList(false, commands);

    // Drop the test login.
    commands.add(dropLoginCommand());

    //Drop the ShardMapManager database.
    commands.add(dropDatabaseCommand(SHARD_MAP_MANAGER_DATABASE_NAME));

    executeNonQueries("master", commands);
  }

  /**
   * Helper method that alters the column name on one of our test tables in one of our test
   * databases. Useful for inducing a schema mismatch to test our failure handling.
   *
   * @param database The 0-based index of the test database to change the schema in.
   * @param oldColName The current name of the column to change.
   * @param newColName The desired new name of the column.
   */
  static void changeColumnNameOnShardedTable(int database, String oldColName,
      String newColName) throws SQLException {
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(getTestConnectionString(testDatabaseNames.get(database)));
      try (Statement stmt = conn.createStatement()) {

        String tsql = String.format("EXEC sp_rename '[%1$s].[%2$s]', '%3$s', 'COLUMN';", TABLE_NAME,
            oldColName, newColName);

        stmt.executeUpdate(tsql);
      } catch (SQLException ex) {
        ex.printStackTrace();
      }
    } catch (Exception e) {
      System.out.printf("Failed to connect to SQL database: " + e.getMessage());
    } finally {
      if (conn != null && !conn.isClosed()) {
        conn.close();
      }
    }
  }

  static ShardMap createAndGetTestShardMap() {
    ShardMap sm;
    ShardMapManagerFactory.createSqlShardMapManager(MULTI_SHARD_TEST_CONN_STRING,
        ShardMapManagerCreateMode.ReplaceExisting);

    ShardMapManager smm = ShardMapManagerFactory.getSqlShardMapManager(MULTI_SHARD_TEST_CONN_STRING,
        ShardMapManagerLoadPolicy.Lazy);

    sm = smm.createListShardMap(SHARD_MAP_NAME, ShardKeyType.Int32);
    for (String testDatabaseName : testDatabaseNames) {
      sm.createShard(getTestShard(testDatabaseName));
    }
    return sm;
  }

  static String getServerName() {
    return TEST_CONN_SERVER_NAME;
  }

  /**
   * Generates a connection string for the given database name.  Assumes we wish to connect to
   * localhost.
   *
   * @param database The name of the database to put in the connection string.
   * @return The connection string to the passed in database name on local host.
   *
   * Currently assumes we wish to connect to localhost using integrated auth. We will likely need to
   * change this when we integrate with our test framework better.
   */
  static String getTestConnectionString(String database) {
    Preconditions.checkNotNull(database, "null database");
    SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(
        DB_CONN_STRING);
    builder.setDatabaseName(database);
    return builder.getConnectionString();
  }

  /**
   * Generates a ShardLocation object that encapsulates the server location and dbname that we
   * should connect to.
   * Assumes we wish to connect to local host.
   *
   * @param database The name of the database to put in the shard.
   * @return The shard with the specified server location and database parameters.
   *
   * Currently assumes we wish to connect to localhost using integrated auth. We will likely need to
   * change this when we integrate with our test framework better.
   */
  private static ShardLocation getTestShard(String database) {
    Preconditions.checkNotNull(database, "null database");
    return new ShardLocation(TEST_CONN_SERVER_NAME, database);
  }

  /**
   * Helper that iterates through the Test databases and adds commands to drop and, optionally,
   * re-create them, to the passed in list.
   *
   * @param create True if we should create the test databases, false if not.
   * @param output The list to append the commands to.
   */
  private static void addCommandsToManageTestDatabasesToList(boolean create,
      List<String> output) {
    for (String dbName : testDatabaseNames) {
      output.add(dropDatabaseCommand(dbName));

      if (create) {
        output.add(createDatabaseCommand(dbName));
      }
    }
  }

  /**
   * Helper that provides tsql to drop a database if it exists.
   *
   * @param dbName The name of the database to drop.
   * @return The tsql to drop it if it exists.
   */
  private static String dropDatabaseCommand(String dbName) {
    return String.format("IF EXISTS (SELECT name FROM sys.databases WHERE name = N'%1$s')"
        + " DROP DATABASE [%1$s]", dbName);
  }

  /**
   * Helper that provides tsql to create a database.
   *
   * @param dbName The name of the database to create.
   * @return The tsql to create the database.
   */
  private static String createDatabaseCommand(String dbName) {
    return String.format("CREATE DATABASE [%1$s]", dbName);
  }

  /**
   * Helper that prodices tsql to drop a database if it exists and then recreate it.  The tsql
   * statements get appended to the passed in list.
   *
   * @param dbName The name of the database to drop and recreate.
   * @param output The list to append the generated tsql into.
   */
  private static void addDropAndCreateDatabaseCommandsToList(String dbName,
      List<String> output) {
    output.add(dropDatabaseCommand(dbName));
    output.add(createDatabaseCommand(dbName));
  }

  /**
   * Helper that produces tsql to drop the test login if it exists.
   *
   * @return The tsql to drop the test login.
   */
  private static String dropLoginCommand() {
    return String.format("IF EXISTS (SELECT name FROM sys.sql_logins WHERE name = N'%1$s')"
        + " DROP LOGIN %1$s", MULTI_SHARD_TEST_CONN_USER);
  }

  /**
   * Helper that appends the commands to drop and recreate the test login to the passed in list.
   *
   * @param output The list to append the commands to.
   */
  private static void addDropAndReCreateTestUserCommandsToList(List<String> output) {
    // First drop it.
    output.add(dropLoginCommand());

    // Then re create it.
    output
        .add(String.format("CREATE LOGIN %1$s WITH Password = '%2$s';", MULTI_SHARD_TEST_CONN_USER,
            MULTI_SHARD_TEST_CONN_PASSWORD));

    // Then grant it lots of permissions.
    output.add(String.format("GRANT CONTROL SERVER TO %1$s", MULTI_SHARD_TEST_CONN_USER));
  }

  /**
   * Helper to execute a single tsql batch over the given connection.
   *
   * @param theConn The connection to execute the tsql against.
   * @param theCommand The tsql to execute.
   */
  private static void executeNonQuery(Connection theConn, String theCommand) throws SQLException {
    try (Statement stmt = theConn.createStatement()) {
      stmt.executeQuery(theCommand);
    } catch (SQLException ex) {
      ex.printStackTrace();
    }
  }

  /**
   * Helper to execute multiple tsql batches consecutively over the given database.
   *
   * @param initialCatalog The database to execute the tsql against.
   * @param theCommands Array containing the tsql batches to execute.
   */
  private static void executeNonQueries(String initialCatalog, List<String> theCommands)
      throws SQLException {
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(getTestConnectionString(initialCatalog));
      try (Statement stmt = conn.createStatement()) {
        for (String tsql : theCommands) {
          stmt.execute(tsql);
        }
      }
    } catch (Exception e) {
      System.out.printf("Failed to connect to SQL database: " + e.getMessage());
    } finally {
      if (conn != null && !conn.isClosed()) {
        conn.close();
      }
    }
  }

  /**
   * Helper that produces tsql to cast a random int as a particular data type.
   *
   * @param column The column that will determine the data type we wish to insert into.
   * @return The tsql fragment to generate the desired value.
   */
  private static String getRandomIntCastAsArg(MultiShardTestCaseColumn column) {
    int theValue = random.nextInt();
    return getSpecificIntCastAsArg(theValue, column);
  }

  /**
   * Helper that produces tsql to cast a random int as a particular data type drawn from the
   * SmallInt range.
   *
   * @param column The column that will determine the data type we wish to insert into.
   * @return The tsql fragment to generate the desired value.
   */
  private static String getRandomSqlSmallIntValue(MultiShardTestCaseColumn column) {
    int theValue = random.nextInt(Short.MAX_VALUE + 1 - Short.MIN_VALUE) + Short.MIN_VALUE;
    return getSpecificIntCastAsArg(theValue, column);
  }

  /**
   * Helper that produces tsql to cast a specific int as a particular data type.
   *
   * @param column The column that will determine the data type we wish to insert into.
   * @param theValue The specific int to cast and insert.
   * @return The tsql fragment to generate the desired value.
   */
  private static String getSpecificIntCastAsArg(int theValue, MultiShardTestCaseColumn column) {
    return String.format("CAST(%1$s AS %2$s)", theValue, column.getSqlServerDatabaseEngineType());
  }

  /**
   * Helper that produces tsql to cast a random binary value as a particular data type.
   *
   * @param length The length of the binary value to generate.
   * @return The tsql fragment to generate the desired value.
   */
  private static String getRandomSqlBinaryValue(int length) {
    byte[] rawData = new byte[length];
    random.nextBytes(rawData);
    StringBuilder builder = new StringBuilder();
    for (byte b : rawData) {
      builder.append(Byte.toString(b));
    }
    String bytesAsString = builder.toString().replace("-", "");

    // the 2 means hex with no 0x
    return String.format("CONVERT(binary(%1$s), '%2$s', 2)", length, bytesAsString);
  }

  /**
   * Helper that produces tsql to cast a random bit value as a bit.
   *
   * @return The tsql fragment to generate the desired value.
   */
  private static String getRandomSqlBitValue() {
    String theVal = (random.nextInt() > random.nextInt()) ? "TRUE" : "FALSE";
    return String.format("CAST ('%1$s' AS bit)", theVal);
  }

  /**
   * Helper that produces tsql of a random char value.
   *
   * @param length The length of the char value to generate
   * @return The tsql fragment to generate the desired value.
   */
  private static String getRandomSqlCharValue(int length) {
    return String.format("'%1$s'", getRandomString(length));
  }

  /**
   * Helper that produces a random SqlDateValue.
   *
   * @return The tsql to produce the value.
   */
  private static String getRandomSqlDateValue() {
    return "GETDATE()";
  }

  /**
   * Helper that produces a random SqlDatetime value.
   *
   * @return The tsql to produce the value.
   */
  private static String getRandomSqlDatetimeValue() {
    return "SYSDATETIME()";
  }

  /**
   * Helper that produces a random sqldatetime value cast as a particular type.
   *
   * @param column The column whoe type the value should be cast to.
   * @return The tsql to generate the casted value.
   */
  private static String getRandomSqlDatetimeCastAsArg(MultiShardTestCaseColumn column) {
    return String.format("CAST(SYSDATETIME() AS %1$s)", column.getSqlServerDatabaseEngineType());
  }

  /**
   * Helper that produces a random datetimeoffset value.
   *
   * @return The tsql to generate the desired value.
   */
  private static String getRandomSqlDatetimeoffsetValue() {
    return "SYSDATETIMEOFFSET()";
  }

  /**
   * Helper that produces a random double within the smallmoney domain and casts it to the desired
   * column type.
   *
   * @param column The column whose type the value should be cast to.
   * @return The tsql to generate the casted value.
   */
  private static String getRandomSqlSmallMoneyValue(MultiShardTestCaseColumn column) {
    double randomSmallMoneyValue = random.nextDouble() * (214748.3647);
    return getSpecificDoubleCastAsArg(randomSmallMoneyValue, column);
  }

  /**
   * Helper to produce a random double cast as a particular type.
   *
   * @param column The column whose type the value should be cast to.
   * @return Tsql to generate the desired value cast as the desired type.
   */
  private static String getRandomDoubleCastAsArg(MultiShardTestCaseColumn column) {
    double randomDouble = random.nextDouble() * Double.MAX_VALUE;
    return getSpecificDoubleCastAsArg(randomDouble, column);
  }

  /**
   * Helper to produce a random double drawn from the decimal domain.
   *
   * @param column The column whose type the value should be cast to.
   * @return Tsql to generate and cast the value.
   */
  private static String getRandomSqlDecimalValue(MultiShardTestCaseColumn column) {
    // .NET Decimal has less range than SQL decimal, so we need to drop down to the
    // .NET range to test these consistently.
    double theValue = random.nextDouble() * Double.MAX_VALUE;
    return String.format("CAST(%1$s AS %2$s)", theValue, column.getColumnTypeDeclaration());
  }

  /**
   * Helper to generate a random double and cast it as a particular type.
   *
   * @param column The column whose type the value should be cast to.
   * @return Tsql that will generate the desired value.
   */
  private static String getRandomSqlFloatValue(MultiShardTestCaseColumn column) {
    double theValue = random.nextDouble() * Double.MAX_VALUE;
    return getSpecificDoubleCastAsArg(theValue, column);
  }

  /**
   * Helper to produce a random double drawn from the real (sqlsingle) domain.
   *
   * @param column The column whose type the value should be cast to.
   * @return Tsql to generate and cast the value.
   */
  private static String getRandomSqlRealValue(MultiShardTestCaseColumn column) {
    double theValue = random.nextDouble() * Float.MAX_VALUE;
    return getSpecificDoubleCastAsArg(theValue, column);
  }

  /**
   * Helper to cast a particular double as a particular type.
   *
   * @param theValue The value to cast.
   * @param column The column whose type the value should be cast to.
   * @return Tsql to cast the value.
   */
  private static String getSpecificDoubleCastAsArg(double theValue,
      MultiShardTestCaseColumn column) {
    return String.format("CAST(%1$s AS %2$s)", theValue, column.getSqlServerDatabaseEngineType());
  }

  /**
   * Helper to generate a random int.
   *
   * @return The random int.
   */
  private static String getRandomSqlIntValue() {
    return String.valueOf(random.nextInt());
  }

  /**
   * Helper to generate a random nchar value of a particular length.
   *
   * @param length The length of the desired nchar.
   * @return The tsql to produce the desired value.
   */
  private static String getRandomSqlNCharValue(int length) {
    return String.format("N'%1$s'", getRandomString(length));
  }

  /**
   * Helper to generate a random value drawn from the TinyInt domain.
   *
   * @return The tsql to generate the desired value.
   */
  private static String getRandomSqlTinyIntValue() {
    return String.valueOf(random.nextInt(Byte.MAX_VALUE + 1 - Byte.MIN_VALUE) + Byte.MIN_VALUE);
  }

  /**
   * Helper to generate a new guid.
   *
   * @return Tsql to produce the guid.
   */
  private static String getRandomSqlUniqueIdentifierValue() {
    return "NEWID()";
  }

  /**
   * Helper to generate a random string of a particular length.
   *
   * @param length The length of the string to generate.
   * @return Tsql representation of the random string.
   */
  private static String getRandomString(int length) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < length; i++) {
      char nextChar = (char) (int) Math.floor(26 * random.nextDouble() + 65);
      builder.append(nextChar);
    }
    return builder.toString();
  }
}