package com.microsoft.azure.elasticdb.shard.unittests;

import static org.junit.Assert.assertEquals;

import com.microsoft.azure.elasticdb.shard.category.ExcludeFromGatedCheckin;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerCreateMode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerFactory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerLoadPolicy;
import com.microsoft.azure.elasticdb.shard.schema.ReferenceTableInfo;
import com.microsoft.azure.elasticdb.shard.schema.SchemaInfo;
import com.microsoft.azure.elasticdb.shard.schema.SchemaInfoCollection;
import com.microsoft.azure.elasticdb.shard.schema.SchemaInfoErrorCode;
import com.microsoft.azure.elasticdb.shard.schema.SchemaInfoException;
import com.microsoft.azure.elasticdb.shard.schema.ShardedTableInfo;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import org.apache.commons.lang.ArrayUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class SchemaInfoCollectionTests {

  private JAXBContext jaxbContext = JAXBContext.newInstance(SchemaInfo.class);

  public SchemaInfoCollectionTests() throws JAXBException {
  }

  /**
   * Initializes common state for tests in this class.
   */
  @BeforeClass
  public static void schemaInfoTestsInitialize() {
    try (Connection conn = DriverManager.getConnection(
        Globals.SHARD_MAP_MANAGER_TEST_CONN_STRING)) {
      // Create ShardMapManager database
      try (Statement stmt = conn.createStatement()) {
        String query = String.format(Globals.CREATE_DATABASE_QUERY,
            Globals.SHARD_MAP_MANAGER_DATABASE_NAME);
        stmt.executeUpdate(query);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * Cleans up common state for the all tests in this class.
   */
  @AfterClass
  public static void schemaInfoTestsCleanup() throws SQLException {
    Globals.dropShardMapManager();
  }

  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void testAddAndLookupAndDeleteSchemaInfo() {
    ShardMapManagerFactory.createSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING,
        ShardMapManagerCreateMode.ReplaceExisting);

    SchemaInfo si = new SchemaInfo();

    ShardedTableInfo stmd1 = new ShardedTableInfo("ShardedTableName1", "ColumnName");
    ShardedTableInfo stmd2 = new ShardedTableInfo("dbo", "ShardedTableName2", "ColumnName");

    si.add(stmd1);
    si.add(stmd2);

    assert 2 == si.getShardedTables().size();

    ReferenceTableInfo rtmd1 = new ReferenceTableInfo("ReferenceTableName1");
    ReferenceTableInfo rtmd2 = new ReferenceTableInfo("dbo", "ReferenceTableName2");

    si.add(rtmd1);
    si.add(rtmd2);

    assert 2 == si.getReferenceTables().size();
    // Add an existing sharded table again. Make sure it doesn't create duplicate entries.
    SchemaInfoException siex = AssertExtensions.assertThrows(
        () -> si.add(new ShardedTableInfo("ShardedTableName1", "ColumnName")));
    assert SchemaInfoErrorCode.TableInfoAlreadyPresent == siex.getErrorCode();

    // Add an existing sharded table with a different key column name. This should fail too.
    siex = AssertExtensions.assertThrows(
        () -> si.add(new ShardedTableInfo("ShardedTableName1", "ColumnName_Different")));
    assert SchemaInfoErrorCode.TableInfoAlreadyPresent == siex.getErrorCode();

    siex = AssertExtensions.assertThrows(
        () -> si.add(new ShardedTableInfo("dbo", "ShardedTableName2", "ColumnName_Different")));
    assert SchemaInfoErrorCode.TableInfoAlreadyPresent == siex.getErrorCode();

    assert 2 == si.getShardedTables().size();

    // Add an existing reference tables again. Make sure it doesn't create duplicate entries.
    siex = AssertExtensions.assertThrows(
        () -> si.add(new ReferenceTableInfo("dbo", "ReferenceTableName2")));
    assert SchemaInfoErrorCode.TableInfoAlreadyPresent == siex.getErrorCode();

    assert 2 == si.getReferenceTables().size();

    // Now trying adding a reference table as a sharded table and vice versa. Both operations should
    // fail.
    siex = AssertExtensions.assertThrows(
        () -> si.add(new ShardedTableInfo("ReferenceTableName1", "ColumnName")));
    assert SchemaInfoErrorCode.TableInfoAlreadyPresent == siex.getErrorCode();

    assert 2 == si.getShardedTables().size();

    siex = AssertExtensions.assertThrows(
        () -> si.add(new ReferenceTableInfo("dbo", "ShardedTableName2")));
    assert SchemaInfoErrorCode.TableInfoAlreadyPresent == siex.getErrorCode();

    assert 2 == si.getReferenceTables().size();

    // Try removing an existing table info and adding it back.
    si.remove(stmd1);
    si.add(stmd1);
    assert 2 == si.getReferenceTables().size();

    si.remove(rtmd2);
    si.add(rtmd2);
    assert 2 == si.getReferenceTables().size();

    // Test with NULL inputs.
    IllegalArgumentException arex = AssertExtensions.assertThrows(
        () -> si.add((ShardedTableInfo) null));

    arex = AssertExtensions.assertThrows(() -> si.add((ReferenceTableInfo) null));

    ShardMapManager shardMapManager = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);
    SchemaInfoCollection siCollection = shardMapManager.getSchemaInfoCollection();
    String mdName = String.format("TestSI_%1$s", UUID.randomUUID());
    siCollection.add(mdName, si);

    SchemaInfo sdmdRead = siCollection.get(mdName);

    assertEqual(si, sdmdRead);

    // Trying to add schema info with the same name again will result in a 'name conflict'
    // exception.
    siex = AssertExtensions.assertThrows(() -> siCollection.add(mdName, si));
    assert SchemaInfoErrorCode.SchemaInfoNameConflict == siex.getErrorCode();

    // Try looking up schema info with a non-existent name.
    siex = AssertExtensions.assertThrows(() -> siCollection.get(mdName + "Fail"));
    assert SchemaInfoErrorCode.SchemaInfoNameDoesNotExist == siex.getErrorCode();

    // Try removing any of the recently created schema info.
    siCollection.remove(mdName);

    // Lookup should fail on removed data.
    siex = AssertExtensions.assertThrows(() -> siCollection.get(mdName));
    assert SchemaInfoErrorCode.SchemaInfoNameDoesNotExist == siex.getErrorCode();
  }

  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void testSetSchemaInfoWithSpecialChars() {
    ShardMapManagerFactory.createSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING,
        ShardMapManagerCreateMode.ReplaceExisting);

    ShardMapManager shardMapManager = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    SchemaInfoCollection siCollection = shardMapManager.getSchemaInfoCollection();

    SchemaInfo si = new SchemaInfo();

    ShardedTableInfo sti = new ShardedTableInfo(newNameWithSpecialChars(),
        newNameWithSpecialChars());

    si.add(sti);

    String mdName = String.format("TestSI_%1$s", UUID.randomUUID());
    siCollection.add(mdName, si);

    SchemaInfo sdmdRead = siCollection.get(mdName);

    assertEqual(si, sdmdRead);
  }

  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void testUpdateSchemaInfo() {
    ShardMapManagerFactory.createSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING,
        ShardMapManagerCreateMode.ReplaceExisting);

    ShardMapManager shardMapManager = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    SchemaInfoCollection siCollection = shardMapManager.getSchemaInfoCollection();

    SchemaInfo si = new SchemaInfo();

    ShardedTableInfo sti = new ShardedTableInfo("dbo", "ShardedTableName1", "ColumnName");

    si.add(sti);

    ReferenceTableInfo rtmd = new ReferenceTableInfo("ReferenceTableName1");

    si.add(rtmd);

    String mdName = String.format("TestSI_%1$s", UUID.randomUUID());

    // Try updating schema info without adding it first.
    SchemaInfoException siex = AssertExtensions.assertThrows(
        () -> siCollection.replace(mdName, si));
    assert SchemaInfoErrorCode.SchemaInfoNameDoesNotExist == siex.getErrorCode();

    siCollection.add(mdName, si);

    SchemaInfo sdmdNew = new SchemaInfo();
    sdmdNew.add(new ShardedTableInfo("dbo", "NewShardedTableName1", "NewColumnName"));
    sdmdNew.add(new ReferenceTableInfo("NewReferenceTableName1"));

    siCollection.replace(mdName, sdmdNew);

    SchemaInfo sdmdRead = siCollection.get(mdName);

    assertEqual(sdmdNew, sdmdRead);
  }

  @Test
  @Category(value = ExcludeFromGatedCheckin.class)
  public void testGetAll() {
    ShardMapManagerFactory.createSqlShardMapManager(Globals.SHARD_MAP_MANAGER_CONN_STRING,
        ShardMapManagerCreateMode.ReplaceExisting);

    ShardMapManager shardMapManager = ShardMapManagerFactory.getSqlShardMapManager(
        Globals.SHARD_MAP_MANAGER_CONN_STRING, ShardMapManagerLoadPolicy.Lazy);

    SchemaInfo[] si = new SchemaInfo[]{new SchemaInfo(), new SchemaInfo(), new SchemaInfo()};

    si[0].add(new ShardedTableInfo("ShardedTableName1", "ColumnName1"));
    si[0].add(new ShardedTableInfo("dbo", "ShardedTableName2", "ColumnName1"));

    si[0].add(new ReferenceTableInfo("ReferenceTableName1"));
    si[0].add(new ReferenceTableInfo("dbo", "ReferenceTableName2"));

    si[1].add(new ShardedTableInfo("ShardedTableName3", "ColumnName2"));
    si[1].add(new ShardedTableInfo("dbo", "ShardedTableName4", "ColumnName2"));

    si[1].add(new ReferenceTableInfo("ReferenceTableName3"));

    si[2].add(new ShardedTableInfo("dbo", "ShardedTableName3", "ColumnName2"));

    si[2].add(new ReferenceTableInfo("ReferenceTableName4"));
    si[2].add(new ReferenceTableInfo("dbo", "ReferenceTableName5"));

    SchemaInfoCollection siCollection = shardMapManager.getSchemaInfoCollection();
    String[] siNames = new String[]{String.format("TestSI_%1$s", UUID.randomUUID()),
        String.format("TestSI_%1$s", UUID.randomUUID()),
        String.format("TestSI_%1$s", UUID.randomUUID())};

    siCollection.add(siNames[0], si[0]);
    siCollection.add(siNames[1], si[1]);
    siCollection.add(siNames[2], si[2]);

    int i = 0;
    boolean success = true;
    for (Entry<String, SchemaInfo> kvp : siCollection) {
      SchemaInfo sdmdOriginal;
      try {
        sdmdOriginal = si[ArrayUtils.indexOf(siNames, kvp.getKey())];
      } catch (java.lang.Exception e) {
        success = false;
        break;
      }

      assertEqual(sdmdOriginal, kvp.getValue());
      i++;
    }

    assert success;
    assert 3 == i;
  }

  /**
   * Verifies that the serialization format of <see cref="SchemaInfo"/> matches the serialization
   * format from v1.0.0. If this fails, then an older version of EDCL v1.0.0 will not be able to
   * successfully deserialize the <see cref="SchemaInfo"/>.
   * This test will need to be more sophisticated if new fields are added. Since no fields have been
   * added yet, we can just do a direct string comparison, which is very simple and precise.
   */
  @Test
  public void serializeCompatibility() {
    SchemaInfo schemaInfo = new SchemaInfo();
    schemaInfo.add(new ShardedTableInfo("s1", "s2", "s3"));
    schemaInfo.add(new ReferenceTableInfo("r1", "r2"));

    String expectedSerializedSchemaInfo =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
            + "<Schema xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\">"
            + "<ReferenceTableSet i:type=\"ArrayOfReferenceTableInfo\">"
            + "<ReferenceTableInfo><SchemaName>r1</SchemaName><TableName>r2</TableName>"
            + "</ReferenceTableInfo></ReferenceTableSet>"
            + "<ShardedTableSet i:type=\"ArrayOfShardedTableInfo\">"
            + "<ShardedTableInfo><SchemaName>s1</SchemaName><TableName>s2</TableName>"
            + "<KeyColumnName>s3</KeyColumnName></ShardedTableInfo></ShardedTableSet></Schema>";
    String actualSerializedSchemaInfo = toXml(schemaInfo);
    assert Objects.equals(expectedSerializedSchemaInfo, actualSerializedSchemaInfo);

    // Deserialize it back as a sanity check
    SchemaInfo finalSchemaInfo = fromXml(actualSerializedSchemaInfo);
    assertEqual(schemaInfo, finalSchemaInfo);
  }

  /**
   * Verifies that <see cref="SchemaInfo"/>data from EDCL v1.0.0 can be deserialized.
   */
  @Test
  public void deserializeCompatibilityV100() {
    // Why is this slightly different from the XML in the SerializeCompatibility test?
    // Because this XML comes from SQL Server, which uses different formatting than
    // DataContractSerializer.
    // The Deserialize test uses the XML formatted by SQL Server because SQL Server is where it will
    // come from in the end-to-end scenario.
    String originalSchemaInfo = "<Schema xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\">"
        + "\r\n" + "  <ReferenceTableSet i:type=\"ArrayOfReferenceTableInfo\">" + "\r\n"
        + "    <ReferenceTableInfo>" + "\r\n" + "      <SchemaName>r1</SchemaName>" + "\r\n"
        + "      <TableName>r2</TableName>" + "\r\n" + "    </ReferenceTableInfo>" + "\r\n"
        + "  </ReferenceTableSet>" + "\r\n"
        + "  <ShardedTableSet i:type=\"ArrayOfShardedTableInfo\">" + "\r\n"
        + "    <ShardedTableInfo>" + "\r\n" + "      <SchemaName>s1</SchemaName>" + "\r\n"
        + "      <TableName>s2</TableName>" + "\r\n" + "      <KeyColumnName>s3</KeyColumnName>"
        + "\r\n" + "    </ShardedTableInfo>" + "\r\n" + "  </ShardedTableSet>" + "\r\n"
        + "</Schema>";

    SchemaInfo schemaInfo = fromXml(originalSchemaInfo);
    assert 1 == schemaInfo.getReferenceTables().size();
    assert Objects.equals("r1", schemaInfo.getReferenceTables().stream().findFirst().get()
        .getSchemaName());
    assert Objects.equals("r2", schemaInfo.getReferenceTables().stream().findFirst().get()
        .getTableName());

    assert 1 == schemaInfo.getShardedTables().size();

    assert Objects.equals("s1", schemaInfo.getShardedTables().stream().findFirst().get()
        .getSchemaName());
    assert Objects.equals("s2", schemaInfo.getShardedTables().stream().findFirst().get()
        .getTableName());
    assert Objects.equals("s3", schemaInfo.getShardedTables().stream().findFirst().get()
        .getKeyColumnName());

    // Serialize the data back. It should not contain _referenceTableSet or _shardedTableSet.
    String expectedFinalSchemaInfo = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        + "<Schema xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\">"
        + "<ReferenceTableSet i:type=\"ArrayOfReferenceTableInfo\">"
        + "<ReferenceTableInfo><SchemaName>r1</SchemaName><TableName>r2</TableName>"
        + "</ReferenceTableInfo></ReferenceTableSet>"
        + "<ShardedTableSet i:type=\"ArrayOfShardedTableInfo\">"
        + "<ShardedTableInfo><SchemaName>s1</SchemaName><TableName>s2</TableName>"
        + "<KeyColumnName>s3</KeyColumnName></ShardedTableInfo></ShardedTableSet></Schema>";
    String actualFinalSchemaInfo = toXml(schemaInfo);
    assert Objects.equals(expectedFinalSchemaInfo, actualFinalSchemaInfo);
  }

  /**
   * Verifies that <see cref="SchemaInfo"/>data from EDCL v1.0.0 can be deserialized.
   */
  @Test
  public void deserializeCompatibilityV110() {
    // Why is this slightly different from the XML in the SerializeCompatibility test?
    // Because this XML comes from SQL Server, which uses different formatting than
    // DataContractSerializer.
    // The Deserialize test uses the XML formatted by SQL Server because SQL Server is where it will
    // come from in the end-to-end scenario.
    String originalSchemaInfo = "<Schema xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\">"
        + "\r\n" + "  <_referenceTableSet i:type=\"ArrayOfReferenceTableInfo\">" + "\r\n"
        + "    <ReferenceTableInfo>" + "\r\n" + "      <SchemaName>r1</SchemaName>" + "\r\n"
        + "      <TableName>r2</TableName>" + "\r\n" + "    </ReferenceTableInfo>" + "\r\n"
        + "  </_referenceTableSet>" + "\r\n"
        + "  <_shardedTableSet i:type=\"ArrayOfShardedTableInfo\">" + "\r\n"
        + "    <ShardedTableInfo>" + "\r\n" + "      <SchemaName>s1</SchemaName>" + "\r\n"
        + "      <TableName>s2</TableName>" + "\r\n" + "      <KeyColumnName>s3</KeyColumnName>"
        + "\r\n" + "    </ShardedTableInfo>" + "\r\n" + "  </_shardedTableSet>" + "\r\n"
        + "</Schema>";

    SchemaInfo schemaInfo = fromXml(originalSchemaInfo);

    assert 1 == schemaInfo.getReferenceTables().size();
    assert Objects.equals("r1", schemaInfo.getReferenceTables().stream().findFirst().get()
        .getSchemaName());
    assert Objects.equals("r2", schemaInfo.getReferenceTables().stream().findFirst().get()
        .getTableName());

    assert 1 == schemaInfo.getShardedTables().size();

    assert Objects.equals("s1", schemaInfo.getShardedTables().stream().findFirst().get()
        .getSchemaName());
    assert Objects.equals("s2", schemaInfo.getShardedTables().stream().findFirst().get()
        .getTableName());
    assert Objects.equals("s3", schemaInfo.getShardedTables().stream().findFirst().get()
        .getKeyColumnName());

    // Serialize the data back. It should not contain _referenceTableSet or _shardedTableSet.
    String expectedFinalSchemaInfo = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        + "<Schema xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\">"
        + "<ReferenceTableSet i:type=\"ArrayOfReferenceTableInfo\">"
        + "<ReferenceTableInfo><SchemaName>r1</SchemaName><TableName>r2</TableName>"
        + "</ReferenceTableInfo></ReferenceTableSet>"
        + "<ShardedTableSet i:type=\"ArrayOfShardedTableInfo\">"
        + "<ShardedTableInfo><SchemaName>s1</SchemaName><TableName>s2</TableName>"
        + "<KeyColumnName>s3</KeyColumnName></ShardedTableInfo></ShardedTableSet></Schema>";
    String actualFinalSchemaInfo = toXml(schemaInfo);
    assert Objects.equals(expectedFinalSchemaInfo, actualFinalSchemaInfo);
  }

  private String toXml(SchemaInfo schemaInfo) {
    try (StringWriter sw = new StringWriter()) {
      Marshaller marshaller = jaxbContext.createMarshaller();
      marshaller.marshal(schemaInfo, sw);
      return sw.toString();
    } catch (JAXBException | IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  private SchemaInfo fromXml(String schemaInfo) {
    try (StringReader sr = new StringReader(schemaInfo)) {
      Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
      return (SchemaInfo) unmarshaller.unmarshal(sr);
    } catch (JAXBException e) {
      e.printStackTrace();
    }
    return null;
  }

  private String newNameWithSpecialChars() {
    // We include invalid XML characters in the list of special characters since error messages
    // are sent from SchemaInfo to T-SQL in the form of XML strings.
    //
    char[] specialChars = new char[]{'[', ']', '-', ' ', '\'', '"', '\'', '<', '>', '\\', '&', '%',
        ':'};
    Random rand = new Random();
    int nameLen = rand.nextInt(20) + 1;
    String db = "";

    for (int i = 0; i < nameLen; i++) {
      if (rand.nextInt(2) == 1) {
        db += specialChars[rand.nextInt(specialChars.length - 1)];
      } else {
        db += (char) ('a' + rand.nextInt(26));
      }
    }

    return db;
  }

  private void assertEqual(SchemaInfo x, SchemaInfo y) {
    assertEquals(x.getReferenceTables(), y.getReferenceTables());
    assertEquals(x.getShardedTables(), y.getShardedTables());
  }
}
