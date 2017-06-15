package com.microsoft.azure.elasticdb.query.unittests;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

/**
 * Common utilities used by tests
 */
public final class MultiShardTestUtils {
//
//  /**
//   * Connection string for local shard user.
//   */
//  public static String shardConnectionString
//      = "Integrated Security=False;User ID=sa;Password=SystemAdmin;";
//  private static Properties properties = loadProperties();
//  private static final String TEST_SERVER_NAME = properties.getProperty("TEST_CONN_SERVER_NAME");
//  private static final String TEST_CONN_USER = properties.getProperty("TEST_CONN_USER");
//  private static final String TEST_CONN_PASSWORD = properties.getProperty("TEST_CONN_PASSWORD");
//  /**
//   * Name of the database where the ShardMapManager persists its data.
//   */
//  private static String shardMapManagerDbName = "ShardMapManager";
//  /**
//   * Connection string for global shard map manager operations.
//   */
//  public static String shardMapManagerConnectionString = "Data Source=" + TEST_SERVER_NAME
//      + ";Initial Catalog=" + shardMapManagerDbName
//      + ";Integrated Security=False;User ID=sa;Password=SystemAdmin;";
//  /**
//   * Table name for the sharded table we will issue fanout queries against.
//   */
//  private static String tableName = "ConsistentShardedTable";
//  /**
//   * SqlCredential encapsulating the testUserId and testPassword that we will use
//   * when opening connections to shards when issuing a fanout query.
//   */
//  private static SqlCredential s_testCredential = GenerateDefaultSqlCredential();
//  /**
//   * Field on the sharded table where we will store the database name.
//   */
//  private static String dbNameField = "dbNameField";
//  /**
//   * Name of the test shard map to use.
//   */
//  private static String shardMapName = "TestShardMap";
//  /**
//   * Name of the test shard map to use.
//   */
//  private static String s_testShardMapName = "TestShardMap";
//  /**
//   * List containing the names of the test databases.
//   */
//  private static ArrayList<String> s_testDatabaseNames = GenerateTestDatabaseNames();
//  /**
//   * Class level Random object.
//   */
//  private static Random random = new Random();
//
//  private static Properties loadProperties() {
//    InputStream inStream = MultiShardTestUtils.class.getClassLoader()
//        .getResourceAsStream("resources.properties");
//    Properties prop = new Properties();
//    if (inStream != null) {
//      try {
//        prop.load(inStream);
//      } catch (IOException e) {
//        e.printStackTrace();
//      }
//    }
//    return prop;
//  }
//
//  /**
//   * Create and populate the test databases with the data we expect for these unit tests to run
//   * correctly.
//   *
//   *
//   * Probably will need to change this to integrate with our test framework better. Will deal with
//   * that down the line when the test framework issue has settled out more.
//   */
//  public static void CreateAndPopulateTables() {
//    ArrayList<String> commands = new ArrayList<String>();
//    for (int i = 0; i < s_testDatabaseNames.size(); i++) {
//      String dbName = s_testDatabaseNames.get(i);
//      commands.add(String.format("USE %1$s;", dbName));
//
//      // First create the table.
//      //
//      String createTable = GetTestTableCreateCommand();
//      commands.add(createTable);
//
//      // Then add the records.
//      //
//      String[] insertValuesCommands = GetInsertValuesCommands(3, dbName);
//      for (String insertValuesCommand : insertValuesCommands) {
//        commands.add(insertValuesCommand);
//      }
//    }
//    ExecuteNonQueries("master", commands);
//  }
//
//  /**
//   * Blow away (if necessary) and create fresh versions of the Test databases we expect for our unit
//   * tests.
//   *
//   *
//   * DEVNOTE (VSTS 2202802): we should move to a GUID-based naming scheme.
//   */
//  public static void DropAndCreateDatabases() {
//    ArrayList<String> commands = new ArrayList<String>();
//
//    // Set up the test user.
//    //
//    AddDropAndReCreateTestUserCommandsToList(commands);
//
//    // Set up the test databases.
//    //
//    AddCommandsToManageTestDatabasesToList(true, commands);
//
//    // Set up the ShardMapManager database.
//    //
//    AddDropAndCreateDatabaseCommandsToList(s_shardMapManagerDbName, commands);
//
//    ExecuteNonQueries("master", commands);
//  }
//
//  /**
//   * Drop the test databases (if they exist) we expect for these unit tests.
//   *
//   *
//   * DEVNOTE (VSTS 2202802): We should switch to a GUID-based naming scheme.
//   */
//  public static void DropDatabases() {
//    ArrayList<String> commands = new ArrayList<String>();
//
//    // Drop the test databases.
//    //
//    AddCommandsToManageTestDatabasesToList(false, commands);
//
//    // Drop the test login.
//    //
//    commands.add(DropLoginCommand());
//
//    //Drop the ShardMapManager database.
//    //
//    commands.add(DropDatabaseCommand(s_shardMapManagerDbName));
//
//    ExecuteNonQueries("master", commands);
//  }
//
//  /**
//   * Simple helper to obtain the SqlCredential to use in running our tests.
//   *
//   * @return The test username and password packaged up into a SqlCredential object.
//   */
//  public static SqlCredential GetTestSqlCredential() {
//    return s_testCredential;
//  }
//
//  /**
//   * Helper method that alters the column name on one of our test tables in one of our test
//   * databases. Useful for inducing a schema mismatch to test our failure handling.
//   *
//   * @param database The 0-based index of the test database to change the schema in.
//   * @param oldColName The current name of the column to change.
//   * @param newColName The desired new name of the column.
//   */
//  public static void ChangeColumnNameOnShardedTable(int database, String oldColName,
//      String newColName)
//      throws SQLException {
//    Connection conn = null;
//    try {
//      conn = DriverManager.getConnection(shardMapManagerConnectionString);
//      try (Statement stmt = conn.createStatement()) {
//
//        String tsql = String
//            .format("EXEC sp_rename '[%1$s].[%2$s]', '%3$s', 'COLUMN';", tableName, oldColName,
//                newColName);
//
//        stmt.executeUpdate(tsql);
//      } catch (SQLException ex) {
//        ex.printStackTrace();
//      }
//    } catch (Exception e) {
//      System.out.printf("Failed to connect to SQL database: " + e.getMessage());
//    } finally {
//      if (conn != null && !conn.isClosed()) {
//        conn.close();
//      }
//    }
//
//  }
//
//  public static ShardMap CreateAndGetTestShardMap() {
//    ShardMap sm;
//    ShardMapManagerFactory
//        .createSqlShardMapManager(MultiShardTestUtils.shardMapManagerConnectionString,
//            ShardMapManagerCreateMode.ReplaceExisting);
//    ShardMapManager smm = ShardMapManagerFactory
//        .getSqlShardMapManager(MultiShardTestUtils.shardMapManagerConnectionString,
//            ShardMapManagerLoadPolicy.Lazy);
//
//    sm = smm.createListShardMap(s_testShardMapName, ShardKeyType.Int32);
//    for (int i = 0; i < s_testDatabaseNames.size(); i++) {
//      sm.createShard(GetTestShard(s_testDatabaseNames.get(i)));
//    }
//    return sm;
//  }
//
//  public static String GetServerName() {
//    return TEST_SERVER_NAME;
//  }
//
//  /**
//   * Generates a connection string for the given database name.  Assumes we wish to connect to
//   * localhost.
//   *
//   * @param database The name of the database to put in the connection string.
//   * @return The connection string to the passed in database name on local host.
//   *
//   * Currently assumes we wish to connect to localhost using integrated auth. We will likely need to
//   * change this when we integrate with our test framework better.
//   */
//  private static String GetTestConnectionString(String database) {
//    Preconditions.checkNotNull(database, "null database");
//    SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
//    builder.setDataSource(TEST_SERVER_NAME);
//    builder.setIntegratedSecurity(false);
//    builder.setUser(TEST_CONN_USER);
//    builder.setPassword(TEST_CONN_PASSWORD);
//    builder.setDatabaseName(database);
//    return builder.getConnectionString();
//  }
//
//  /**
//   * Generates a ShardLocation object that encapsulates the server location and dbname that we
//   * should connect to.
//   * Assumes we wish to connect to local host.
//   *
//   * @param database The name of the database to put in the shard.
//   * @return The shard with the specified server location and database parameters.
//   *
//   * Currently assumes we wish to connect to localhost using integrated auth. We will likely need to
//   * change this when we integrate with our test framework better.
//   */
//  private static ShardLocation GetTestShard(String database) {
//    Preconditions.checkNotNull(database, "null database");
//    ShardLocation rVal = new ShardLocation(TEST_SERVER_NAME, database);
//    return rVal;
//  }
//
//  /**
//   * Static helper that populates our defualt password into a SecureString.
//   *
//   * @return The default password encoded in a SecureString.
//   */
//  private static SqlCredential GenerateDefaultSqlCredential() {
//    SecureString ss = new SecureString();
//    char[] pwdChars = s_testPassword.toCharArray();
//    for (int i = 0; i < pwdChars.length; i++) {
//      ss.AppendChar(pwdChars[i]);
//    }
//
//    // Need to mark it as read only to avoid an ArgumentException in the SqlCredential c-tor.
//    //
//    ss.MakeReadOnly();
//
//    return new SqlCredential(s_testUserId, ss);
//  }
//
//  /**
//   * Helper to populate a list with our test database names.
//   *
//   * @return A new list containing the test database names.
//   */
//  private static ArrayList<String> GenerateTestDatabaseNames() {
//    ArrayList<String> rVal = new ArrayList<String>();
//    for (int i = 0; i < 3; i++) {
//      rVal.add(String.format("Test%1$s", i));
//    }
//    return rVal;
//  }
//
//  /**
//   * Helper that iterates through the Test databases and adds commands to drop and, optionally,
//   * re-create them, to the passed in list.
//   *
//   * @param create True if we should create the test databases, false if not.
//   * @param output The list to append the commands to.
//   */
//  private static void AddCommandsToManageTestDatabasesToList(boolean create,
//      ArrayList<String> output) {
//    for (int i = 0; i < s_testDatabaseNames.size(); i++) {
//      String dbName = s_testDatabaseNames.get(i);
//      output.add(DropDatabaseCommand(dbName));
//
//      if (create) {
//        output.add(CreateDatabaseCommand(dbName));
//      }
//    }
//  }
//
//  /**
//   * Helper that provides tsql to drop a database if it exists.
//   *
//   * @param dbName The name of the database to drop.
//   * @return The tsql to drop it if it exists.
//   */
//  private static String DropDatabaseCommand(String dbName) {
//    return String.format(
//        "IF EXISTS (SELECT name FROM sys.databases WHERE name = N'%1$s') DROP DATABASE [%1$s]",
//        dbName);
//  }
//
//  /**
//   * Helper that provides tsql to create a database.
//   *
//   * @param dbName The name of the database to create.
//   * @return The tsql to create the database.
//   */
//  private static String CreateDatabaseCommand(String dbName) {
//    return String.format("CREATE DATABASE [%1$s]", dbName);
//  }
//
//  /**
//   * Helper that prodices tsql to drop a database if it exists and then recreate it.  The tsql
//   * statements get appended to the passed in list.
//   *
//   * @param dbName The name of the database to drop and recreate.
//   * @param output The list to append the generated tsql into.
//   */
//  private static void AddDropAndCreateDatabaseCommandsToList(String dbName,
//      ArrayList<String> output) {
//    output.add(DropDatabaseCommand(dbName));
//    output.add(CreateDatabaseCommand(dbName));
//  }
//
//  /**
//   * Helper that produces tsql to drop the test login if it exists.
//   *
//   * @return The tsql to drop the test login.
//   */
//  private static String DropLoginCommand() {
//    return String
//        .format("IF EXISTS (SELECT name FROM sys.sql_logins WHERE name = N'%1$s') DROP LOGIN %1$s",
//            s_testUserId);
//  }
//
//  /**
//   * Helper that appends the commands to drop and recreate the test login to the passed in list.
//   *
//   * @param output The list to append the commands to.
//   */
//  private static void AddDropAndReCreateTestUserCommandsToList(ArrayList<String> output) {
//    // First drop it.
//    //
//    output.add(DropLoginCommand());
//
//    // Then re create it.
//    //
//    output.add(
//        String.format("CREATE LOGIN %1$s WITH Password = '%2$s';", s_testUserId, s_testPassword));
//
//    // Then grant it lots of permissions.
//    //
//    output.add(String.format("GRANT CONTROL SERVER TO %1$s", s_testUserId));
//  }
//
//  /**
//   * Helper to execute a single tsql batch over the given connection.
//   *
//   * @param theConn The connection to execute the tsql against.
//   * @param theCommand The tsql to execute.
//   */
//  private static void ExecuteNonQuery(Connection theConn, String theCommand) throws SQLException {
//    try (Statement stmt = theConn.createStatement()) {
//      stmt.executeQuery(theCommand);
//    } catch (SQLException ex) {
//      ex.printStackTrace();
//    }
//  }
//
//  /**
//   * Helper to execute multiple tsql batches consecutively over the given connection.
//   *
//   * @param theConn The connection to execute the tsql against.
//   * @param theCommands Array containing the tsql batches to execute.
//   */
//  private static void ExecuteNonQueries(String initialCatalog, List<String> theCommands)
//      throws SQLException {
//    Connection conn = null;
//    try {
//      conn = DriverManager.getConnection(shardMapManagerConnectionString);
//      try (Statement stmt = conn.createStatement()) {
//        for (String tsql : theCommands) {
//          stmt.execute(tsql);
//        }
//      }
//    } catch (Exception e) {
//      System.out.printf("Failed to connect to SQL database: " + e.getMessage());
//    } finally {
//      if (conn != null && !conn.isClosed()) {
//        conn.close();
//      }
//    }
//  }
//
//  /**
//   * Helper that constructs the sql script to create our test table.
//   *
//   * @return T-SQL to create our test table.
//   */
//  private static String GetTestTableCreateCommand() {
//    StringBuilder createCommand = new StringBuilder();
//
//    // Set up the stem of the statement
//    //
//    createCommand
//        .append(String.format("CREATE TABLE %1$s (%2$s nvarchar(50)", tableName, dbNameField));
//
//    List<MutliShardTestCaseColumn> fieldInfo = MutliShardTestCaseColumn.DefinedColumns;
//    for (int i = 0; i < fieldInfo.size(); i++) {
//      MutliShardTestCaseColumn curField = fieldInfo.getItem(i);
//      createCommand.append(
//          String.format(", %1$s %2$s", curField.TestColumnName, curField.ColumnTypeDeclaration));
//    }
//
//    createCommand.append(");");
//    return createCommand.toString();
//  }
//
//  /**
//   * Helper to generate random field data for a record in the test table.
//   *
//   * @param numCommands The number of records to generate random data for.
//   * @param dbName The name of the database to put in the dbName column.
//   * @return Array filled with the commands to execute to insert the data.
//   */
//  private static String[] GetInsertValuesCommands(int numCommands, String dbName) {
//    String[] commandsToReturn = new String[numCommands];
//
//    StringBuilder insertCommand = new StringBuilder();
//    List<MutliShardTestCaseColumn> fieldInfo = MutliShardTestCaseColumn.DefinedColumns;
//
//    for (int i = 0; i < numCommands; i++) {
//      insertCommand.setLength(0);
//
//      // Set up the stem, which includes putting the dbName in the dbNameColumn.
//      //
//      insertCommand.append(String.format("INSERT INTO %1$s (%2$s", s_testTableName, s_dbNameField));
//
//      // Now put in all the column names in the order we will do them.
//      //
//      for (int j = 0; j < fieldInfo.size(); j++) {
//        MutliShardTestCaseColumn curField = fieldInfo.getItem(j);
//
//        // Special case: if we hit the rowversion field let's skip it - it gets updated automatically.
//        //
//        if (!IsTimestampField(curField.DbType)) {
//          insertCommand.append(String.format(", %1$s", curField.TestColumnName));
//        }
//      }
//
//      // Close the field list
//      //
//      insertCommand.append(") ");
//
//      // Now put in the VALUES stem
//      //
//      insertCommand.append(String.format("VALUES ('%1$s'", dbName));
//
//      // Now put in the individual field values
//      //
//      for (int j = 0; j < fieldInfo.size(); j++) {
//        MutliShardTestCaseColumn curField = fieldInfo.getItem(j);
//
//        // Special case: if we hit the rowversion field let's skip it - it gets updated automatically.
//        //
//        if (!IsTimestampField(curField.DbType)) {
//          String valueToPutIn = GetTestFieldValue(curField);
//          insertCommand.append(String.format(", %1$s", valueToPutIn));
//        }
//      }
//
//      // Finally, close the values list, terminate the statement, and add it to the array.
//      //
//      insertCommand.append(");");
//      commandsToReturn[i] = insertCommand.toString();
//    }
//    return commandsToReturn;
//  }
//
//  /**
//   * Helper to generate a tsql fragement that will produce a random value of the given type to
//   * insert into the test database.
//   *
//   * @param dataTypeInfo The datatype of the desired value.
//   * @return The tsql fragment that will generate a random value of the desired type.
//   */
//  private static String GetTestFieldValue(MutliShardTestCaseColumn dataTypeInfo) {
//    SqlDbType dbType = dataTypeInfo.DbType;
//    Integer length = dataTypeInfo.FieldLength;
//
//    switch (dbType) {
//      case SqlDbType.BigInt:
//        // SQL Types: bigint
//        return GetRandomIntCastAsArg(dataTypeInfo);
//
//      case SqlDbType.Binary:
//        // SQL Types: image
//        return GetRandomSqlBinaryValue(length.intValue());
//
//      case SqlDbType.Bit:
//        // SQL Types: bit
//        return GetRandomSqlBitValue();
//
//      case SqlDbType.Char:
//        // SQL Types: char[(n)]
//        return GetRandomSqlCharValue(length.intValue());
//
//      case SqlDbType.Date:
//        // SQL Types: date
//        return GetRandomSqlDateValue();
//
//      case SqlDbType.DateTime:
//        // SQL Types: datetime, smalldatetime
//        return GetRandomSqlDatetimeCastAsArg(dataTypeInfo);
//
//      case SqlDbType.DateTime2:
//        // SQL Types: datetime2
//        return GetRandomSqlDatetimeCastAsArg(dataTypeInfo);
//
//      case SqlDbType.DateTimeOffset:
//        // SQL Types: datetimeoffset
//        return GetRandomSqlDatetimeoffsetValue();
//
//      case SqlDbType.Decimal:
//        // SQL Types: decimal, numeric
//        // These are the same.
//        return GetRandomSqlDecimalValue(dataTypeInfo);
//
//      case SqlDbType.Float:
//        // SQL Types: float
//        return GetRandomSqlFloatValue(dataTypeInfo);
//
//      case SqlDbType.Image:
//        // SQL Types: image
//        return GetRandomSqlBinaryValue(dataTypeInfo.FieldLength);
//
//      case SqlDbType.Int:
//        // SQL Types: int
//        return GetRandomSqlIntValue();
//
//      case SqlDbType.Money:
//        // SQL Types: money
//        return GetRandomSqlMoneyValue(dataTypeInfo);
//
//      case SqlDbType.NChar:
//        // SQL Types: nchar[(n)]
//        return GetRandomSqlNCharValue(length.intValue());
//
//      case SqlDbType.NText:
//        // SQL Types: ntext
//        return GetRandomSqlNCharValue(length.intValue());
//
//      case SqlDbType.NVarChar:
//        // SQL Types: nvarchar[(n)]
//        return GetRandomSqlNCharValue(length.intValue());
//
//      case SqlDbType.Real:
//        // SQL Types: real
//        return GetRandomSqlRealValue(dataTypeInfo);
//
//      case SqlDbType.SmallDateTime:
//        // SQL Types: smalldatetime
//        return GetRandomSqlDatetimeCastAsArg(dataTypeInfo);
//
//      case SqlDbType.SmallInt:
//        // SQL Types: smallint
//        return GetRandomSqlSmallIntValue(dataTypeInfo);
//
//      case SqlDbType.SmallMoney:
//        // SQL Types: smallmoney
//        return GetRandomSqlSmallMoneyValue(dataTypeInfo);
//
//      case SqlDbType.Text:
//        // SQL Types: text
//        return GetRandomSqlCharValue(length.intValue());
//
//      case SqlDbType.Time:
//        // SQL Types: time
//        return GetRandomSqlDatetimeCastAsArg(dataTypeInfo);
//
//      case SqlDbType.Timestamp:
//        // SQL Types: rowversion, timestamp
//        //exclding it should happen automatically.  should not be here. throw.
//        throw new IllegalArgumentException(SqlDbType.Timestamp.toString());
//
//      case SqlDbType.TinyInt:
//        // SQL Types: tinyint
//        return GetRandomSqlTinyIntValue();
//
//      case SqlDbType.UniqueIdentifier:
//        // SQL Types: uniqueidentifier
//        return GetRandomSqlUniqueIdentifierValue();
//
//      case SqlDbType.VarBinary:
//        // SQL Types: binary[(n)], varbinary[(n)]
//        return GetRandomSqlBinaryValue(length.intValue());
//
//      case SqlDbType.VarChar:
//        // SQL Types: varchar[(n)]
//        return GetRandomSqlCharValue(length.intValue());
//
//      default:
//        throw new IllegalArgumentException(dbType.toString());
//    }
//  }
//
//  /**
//   * Helper that produces tsql to cast a random int as a particular data type.
//   *
//   * @param column The column that will determine the data type we wish to insert into.
//   * @return The tsql fragment to generate the desired value.
//   */
//  private static String GetRandomIntCastAsArg(MutliShardTestCaseColumn column) {
//    int theValue = random.nextInt();
//    return GetSpecificIntCastAsArg(theValue, column);
//  }
//
//  /**
//   * Helper that produces tsql to cast a random int as a particular data type drawn from the
//   * SmallInt range.
//   *
//   * @param column The column that will determine the data type we wish to insert into.
//   * @return The tsql fragment to generate the desired value.
//   */
//  private static String GetRandomSqlSmallIntValue(MutliShardTestCaseColumn column) {
////TODO TASK: There is no two-argument version of 'nextInt' in Java:
//    int theValue = random.nextInt(Short.MIN_VALUE, Short.MAX_VALUE);
//    return GetSpecificIntCastAsArg(theValue, column);
//  }
//
//  /**
//   * Helper that produces tsql to cast a specific int as a particular data type.
//   *
//   * @param column The column that will determine the data type we wish to insert into.
//   * @param theValue The specific int to cast and insert.
//   * @return The tsql fragment to generate the desired value.
//   */
//  private static String GetSpecificIntCastAsArg(int theValue, MutliShardTestCaseColumn column) {
//    return String.format("CAST(%1$s AS %2$s)", theValue, column.SqlServerDatabaseEngineType);
//  }
//
//  /**
//   * Helper that produces tsql to cast a random binary value as a particular data type.
//   *
//   * @param length The length of the binary value to generate.
//   * @return The tsql fragment to generate the desired value.
//   */
//  private static String GetRandomSqlBinaryValue(int length) {
////WARNING: Unsigned integer types have no direct equivalent in Java:
////ORIGINAL LINE: Byte[] rawData = new Byte[length];
//    byte[] rawData = new byte[length];
//    random.nextBytes(rawData);
//    String bytesAsString = BitConverter.toString(rawData);
//    bytesAsString = bytesAsString.replace("-", "");
//
//    return String.format("CONVERT(binary(%1$s), '%2$s', 2)", length,
//        bytesAsString); // the 2 means hex with no 0x
//  }
//
//  /**
//   * Helper that produces tsql to cast a random bit value as a bit.
//   *
//   * @return The tsql fragment to generate the desired value.
//   */
//  private static String GetRandomSqlBitValue() {
//    String theVal = (random.nextInt() > random.nextInt()) ? "TRUE" : "FALSE";
//    return String.format("CAST ('%1$s' AS bit)", theVal);
//  }
//
//  /**
//   * Helper that produces tsql of a random char value.
//   *
//   * @param length The length of the char value to generate
//   * @return The tsql fragment to generate the desired value.
//   */
//  private static String GetRandomSqlCharValue(int length) {
//    return String.format("'%1$s'", GetRandomString(length));
//  }
//
//  /**
//   * Helper that produces a random SqlDateValue.
//   *
//   * @return The tsql to produce the value.
//   */
//  private static String GetRandomSqlDateValue() {
//    return "GETDATE()";
//  }
//
//  /**
//   * Helper that produces a random SqlDatetime value.
//   *
//   * @return The tsql to produce the value.
//   */
//  private static String GetRandomSqlDatetimeValue() {
//    return "SYSDATETIME()";
//  }
//
//  /**
//   * Helper that produces a random sqldatetime value cast as a particular type.
//   *
//   * @param column The column whoe type the value should be cast to.
//   * @return The tsql to generate the casted value.
//   */
//  private static String GetRandomSqlDatetimeCastAsArg(MutliShardTestCaseColumn column) {
//    return String.format("CAST(SYSDATETIME() AS %1$s)", column.SqlServerDatabaseEngineType);
//  }
//
//  /**
//   * Helper that produces a random datetimeoffset value.
//   *
//   * @return The tsql to generate the desired value.
//   */
//  private static String GetRandomSqlDatetimeoffsetValue() {
//    return "SYSDATETIMEOFFSET()";
//  }
//
//  /**
//   * Helper that produces a random double within the smallmoney domain and casts it to the desired
//   * column type.
//   *
//   * @param column The column whose type the value should be cast to.
//   * @return The tsql to generate the casted value.
//   */
//  private static String GetRandomSqlSmallMoneyValue(MutliShardTestCaseColumn column) {
//    double randomSmallMoneyValue = random.nextDouble() * (214748.3647);
//    return GetSpecificDoubleCastAsArg(randomSmallMoneyValue, column);
//  }
//
//  /**
//   * Helper to produce a random double cast as a particular type.
//   *
//   * @param column The column whose type the value should be cast to.
//   * @return Tsql to generate the desired value cast as the desired type.
//   */
//  private static String GetRandomDoubleCastAsArg(MutliShardTestCaseColumn column) {
//    double randomDouble = random.nextDouble() * Double.MAX_VALUE;
//    return GetSpecificDoubleCastAsArg(randomDouble, column);
//  }
//
//  /**
//   * Helper to produce a random double drawn from the decimal domain.
//   *
//   * @param column The column whose type the value should be cast to.
//   * @return Tsql to generate and cast the value.
//   */
//  private static String GetRandomSqlDecimalValue(MutliShardTestCaseColumn column) {
//    // .NET Decimal has less range than SQL decimal, so we need to drop down to the
//    // .NET range to test these consistently.
//    //
//    double theValue =
//        random.nextDouble() * java.math.BigDecimal.ToDouble(java.math.BigDecimal.MaxValue);
//    return String.format("CAST(%1$s AS %2$s)", theValue, column.ColumnTypeDeclaration);
//  }
//
//  /**
//   * Helper to generate a random double and cast it as a particular type.
//   *
//   * @param column The column whose type the value should be cast to.
//   * @return Tsql that will generate the desired value.
//   */
//  private static String GetRandomSqlFloatValue(MutliShardTestCaseColumn column) {
//    double theValue = random.nextDouble() * SqlDouble.MaxValue.Value;
//    return GetSpecificDoubleCastAsArg(theValue, column);
//  }
//
//  /**
//   * Helper to produce a random double drawn from the money domain.
//   *
//   * @param column The column whose type the value should be cast to.
//   * @return Tsql to generate and cast the value.
//   */
//  private static String GetRandomSqlMoneyValue(MutliShardTestCaseColumn column) {
//    double theValue = random.nextDouble() * SqlMoney.MaxValue.ToDouble();
//    return GetSpecificDoubleCastAsArg(theValue, column);
//  }
//
//  /**
//   * Helper to produce a random double drawn from the real (sqlsingle) domain.
//   *
//   * @param column The column whose type the value should be cast to.
//   * @return Tsql to generate and cast the value.
//   */
//  private static String GetRandomSqlRealValue(MutliShardTestCaseColumn column) {
//    double theValue = random.nextDouble() * SqlSingle.MaxValue.Value;
//    return GetSpecificDoubleCastAsArg(theValue, column);
//  }
//
//  /**
//   * Helper to cast a particular double as a particular type.
//   *
//   * @param theValue The value to cast.
//   * @param column The column whose type the value should be cast to.
//   * @return Tsql to cast the value.
//   */
//  private static String GetSpecificDoubleCastAsArg(double theValue,
//      MutliShardTestCaseColumn column) {
//    return String.format("CAST(%1$s AS %2$s)", theValue, column.SqlServerDatabaseEngineType);
//  }
//
//  /**
//   * Helper to generate a random int.
//   *
//   * @return The random int.
//   */
//  private static String GetRandomSqlIntValue() {
//    return String.valueOf(random.nextInt());
//  }
//
//  /**
//   * Helper to generate a random nchar value of a particular length.
//   *
//   * @param length The length of the desired nchar.
//   * @return The tsql to produce the desired value.
//   */
//  private static String GetRandomSqlNCharValue(int length) {
//    return String.format("N'%1$s'", GetRandomString(length));
//  }
//
//  /**
//   * Helper to generate a random value drawn from the TinyInt domain.
//   *
//   * @return The tsql to generate the desired value.
//   */
//  private static String GetRandomSqlTinyIntValue() {
////TODO TASK: There is no two-argument version of 'nextInt' in Java:
////WARNING: Unsigned integer types have no direct equivalent in Java:
////ORIGINAL LINE: return s_random.Next(Byte.MinValue, Byte.MaxValue).ToString();
//    return String.valueOf(random.nextInt(Byte.MIN_VALUE, Byte.MAX_VALUE));
//  }
//
//  /**
//   * Helper to generate a new guid.
//   *
//   * @return Tsql to produce the guid.
//   */
//  private static String GetRandomSqlUniqueIdentifierValue() {
//    return "NEWID()";
//  }
//
//  /**
//   * Helper to generate a random string of a particular length.
//   *
//   * @param length The length of the string to generate.
//   * @return Tsql representation of the random string.
//   */
//  private static String GetRandomString(int length) {
//    StringBuilder builder = new StringBuilder();
//    for (int i = 0; i < length; i++) {
//      char nextChar = (char) (int) Math.floor(26 * random.nextDouble() + 65);
//      builder.append(nextChar);
//    }
//    return builder.toString();
//  }
//
//  /**
//   * Helper to determine if a particular SqlDbType is a timestamp.
//   */
//  private static boolean IsTimestampField(SqlDbType curFieldType) {
//    return SqlDbType.Timestamp == curFieldType;
//  }
}