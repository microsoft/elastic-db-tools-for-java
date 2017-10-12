package com.microsoft.azure.elasticdb.shardmapscalability;

/*
 * Copyright (c) Microsoft. All rights reserved. Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

import static com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerFactory.getSqlShardMapManager;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.microsoft.azure.elasticdb.shard.base.Shard;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerFactory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerLoadPolicy;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlConnectionStringBuilder;
import com.microsoft.azure.elasticdb.shard.stubhelper.Action1Param;
import com.microsoft.azure.elasticdb.shard.stubhelper.Func1Param;
import com.microsoft.azure.elasticdb.shard.utils.CsvUtils;

public class Program {

    private static final boolean IntegratedSecurity = false;
    private static final String Database = "ShardMapManagerScalabilityTest";
    private static final String ShardMap = "ScalabilityShardMap";
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final int[] MappingCountsToTest = new int[] {10, 20, 50, 100, 200, 500, 1000, 2000, 5000};
    private static Properties properties = loadProperties();
    private static final String Server = properties.getProperty("TEST_CONN_SERVER_NAME");
    private static final String UserName = properties.getProperty("TEST_CONN_USER");
    private static final String Password = properties.getProperty("TEST_CONN_PASSWORD");
    private static ShardMapType shardMapType = ShardMapType.RangeShardMap;

    private static Properties loadProperties() {
        InputStream inStream = Program.class.getClassLoader().getResourceAsStream("resources.properties");
        Properties prop = new Properties();
        if (inStream != null) {
            try {
                prop.load(inStream);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        return prop;
    }

    /**
     * Shard Map Scalability Test.
     *
     * @param args
     *            Input Arguments
     */
    public static void main(String[] args) {
        createDatabase(Database);

        ShardMapManager smm = createShardMapManager();

        /*
         * RecoveryManager rm = smm.getRecoveryManager();
         * 
         * Stopwatch sw1 = Stopwatch.createStarted(); List<RecoveryToken> tokens = rm.detectMappingDifferences(new ShardLocation(Server, Database));
         * sw1.stop(); System.out.println(String.format("Got tokens: %1$s ms", sw1.elapsed(TimeUnit.MILLISECONDS)));
         * 
         * Stopwatch sw2 = Stopwatch.createStarted(); Map<ShardRange, MappingLocation> differences = rm.getMappingDifferences(tokens.stream()
         * .findFirst().get()); sw2.stop(); System.out.println(String.format("Got differences: %1$s ms", sw2.elapsed(TimeUnit.MILLISECONDS)));
         */

        log.info("Creating shard map");
        ShardMapOperations<Integer> shardMapOperations;
        switch (shardMapType) {
            case ListShardMap:
                shardMapOperations = new ListShardMapOperations(smm, ShardMap);
                break;

            case RangeShardMap:
                shardMapOperations = new RangeShardMapOperations(smm, ShardMap);
                break;

            default:
                throw new IllegalArgumentException(shardMapType.toString());
        }

        log.info("Creating shard");
        Shard shard = shardMapOperations.addShard(new ShardLocation(Server, Database));

        File csvFile = new File(String.format("results_%1$s.csv", LocalDateTime.now().atZone(ZoneId.systemDefault()).toEpochSecond()));
        log.info(String.format("Writing to file %1$s", csvFile));

        try (FileWriter writer = new FileWriter(csvFile, true)) {
            List<String> writeValues = new ArrayList<>();
            writeValues.add("MappingCount");
            writeValues.add("CreateMappingTicks");
            writeValues.add("LoadMappingTicks");
            writeValues.add("LookupMappingTicks");
            CsvUtils.writeLine(writer, writeValues);

            int currentMappingCount = shardMapOperations.getCurrentMappingCount();
            for (int mappingCountToTest : MappingCountsToTest) {
                log.info("");
                log.info(String.format("===== Preparing to test %1$s mappings =====", mappingCountToTest));

                if (currentMappingCount > mappingCountToTest) {
                    log.warn(String.format("WARNING: currentMappingCount %1$s is greater than" + "mappingCountToTest %2$s", currentMappingCount,
                            mappingCountToTest));
                    continue;
                }

                // Create mappings
                List<Integer> createMappingRange = IntStream.range(currentMappingCount, mappingCountToTest).boxed().collect(Collectors.toList());
                Result createMappingResult = executeMapOperations("Creating mappings", createMappingRange,
                        i -> shardMapOperations.createMapping(i, shard));

                currentMappingCount = shardMapOperations.getCurrentMappingCount();
                log.info(String.format("Created %1$s mappings, Total time taken: %2$s.\nCurrent mapping" + "count is %3$s", createMappingResult.count,
                        createMappingResult.totalTicks, currentMappingCount));

                if (currentMappingCount != mappingCountToTest) {
                    log.error(String.format("ERROR: currentMappingCount %1$s is not equal to " + "mappingCountToTest %2$s", currentMappingCount,
                            mappingCountToTest));
                }

                // Load mappings
                List<Integer> loadMappingRange = IntStream.range(0, 10).boxed().collect(Collectors.toList());
                Result loadMappingResult = executeMapOperations("Loading mappings", loadMappingRange,
                        i -> shardMapOperations.getCurrentMappingCount());

                // Lookup mappings
                Random r = new Random(LocalDateTime.now().hashCode());
                int count = currentMappingCount;
                List<Integer> lookupMappingRange = IntStream.range(0, 100000).boxed().collect(Collectors.toList());
                Result lookupMappingResult = executeMapOperations("LookupPointMapping", lookupMappingRange, i -> r.nextInt(count),
                        shardMapOperations::lookupMapping);

                log.info("************");
                log.info(String.format("\tNumber of Mappings: %1$s", currentMappingCount));
                log.info(String.format("\tAverage time to create mappings: %1$s ticks", createMappingResult.getAverageTicks()));
                log.info(String.format("\tAverage time to load mappings: %1$s ticks", loadMappingResult.getAverageTicks()));
                log.info(String.format("\tAverage time to lookup 1 mapping: %1$s ticks", lookupMappingResult.getAverageTicks()));

                writeValues = new ArrayList<>();
                writeValues.add(Integer.toString(currentMappingCount));
                writeValues.add(createMappingResult.getAverageTicks().toString());
                writeValues.add(loadMappingResult.getAverageTicks().toString());
                writeValues.add(lookupMappingResult.getAverageTicks().toString());
                CsvUtils.writeLine(writer, writeValues);

                log.info("************");
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static ShardMapManager createShardMapManager() {
        // Create Shard Map Manager in the Shard Map Manager database, if it doesn't already exist
        try {
            ShardMapManagerFactory.createSqlShardMapManager(getSqlShardMapManagerConnectionString().getConnectionString());
            System.out.println("Created SqlShardMapManager");
        }
        catch (ShardManagementException e) {
            if (e.getErrorCode().equals(ShardManagementErrorCode.ShardMapManagerStoreAlreadyExists)) {
                System.out.println("Shard Map Manager already exists");
            }
            else {
                throw e;
            }
        }

        // Get a reference to the shard map manager and return it
        return getSqlShardMapManager(getSqlShardMapManagerConnectionString().getConnectionString(), ShardMapManagerLoadPolicy.Lazy);
    }

    private static SqlConnectionStringBuilder getSqlShardMapManagerConnectionString() {
        SqlConnectionStringBuilder tempVar = new SqlConnectionStringBuilder();
        tempVar.setIntegratedSecurity(IntegratedSecurity);
        tempVar.setUser(UserName);
        tempVar.setPassword(Password);
        tempVar.setDataSource(Server);
        tempVar.setDatabaseName(Database);
        return tempVar;
    }

    private static void createDatabase(String db) {
        System.out.printf("Creating database %1$s" + "\r\n", db);
        try (Connection conn = DriverManager.getConnection(createConnectionString("master").toString())) {
            Statement cmd = conn.createStatement();

            // Skip if the database already exists
            if (cmd.execute(String.format("SELECT COUNT(*) FROM sys.databases WHERE name = '%1$s'", Database)) && cmd.getResultSet().next()
                    && cmd.getResultSet().getInt(1) > 0) {
                // Drop if already exists
                // cmd.execute(String.format("DROP DATABASE [%1$s]", db));/*Comment line to Skip
                return;// */
            }

            // Determine if we are connecting to Azure SQL DB
            ResultSet resultSet = cmd.executeQuery("SELECT CAST(SERVERPROPERTY('EngineEdition') AS NVARCHAR(128))");

            if (resultSet.next()) {
                if (resultSet.getInt(1) == 5) {
                    // Azure SQL DB

                    // Determine the number of successful CREATE operations before
                    int completedCreations = getNumCompletedDatabaseCreations(conn, db);

                    // Begin creation (which is async for Standard/Premium editions)
                    cmd.execute(String.format("CREATE DATABASE [%1$s] (EDITION = 'Business', MAXSIZE=150GB)", db));

                    // Wait for the operation to complete
                    while (getNumCompletedDatabaseCreations(conn, db) <= completedCreations) {
                        System.out.printf("Waiting for creation of %1$s to be completed in" + "sys.dm_operation_status...\r\n", db);
                        TimeUnit.SECONDS.sleep(5);
                    }
                }
                else {
                    // Other edition of SQL DB
                    cmd.execute(String.format("CREATE DATABASE [%1$s]", db));
                }
            }
        }
        catch (SQLException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static int getNumCompletedDatabaseCreations(Connection conn,
            String db) throws SQLException {
        Statement cmd = conn.createStatement();
        ResultSet resultSet = cmd.executeQuery("SELECT COUNT(*) FROM sys.dm_operation_status \r\n"
                + "WHERE resource_type = 0 -- 'Database' \r\n AND major_resource_id = '" + db + "' \r\n" + "AND state = 2 -- ' COMPLETED'");
        if (resultSet.next()) {
            return resultSet.getInt(1);
        }
        return -1;
    }

    private static SqlConnectionStringBuilder createConnectionString(String db) {
        SqlConnectionStringBuilder b = new SqlConnectionStringBuilder();
        b.setIntegratedSecurity(IntegratedSecurity);
        b.setDataSource(Server);
        b.setUser(UserName);
        b.setPassword(Password);
        b.setDatabaseName(db);
        return b;
    }

    private static <T> Result executeMapOperations(String message,
            List<T> items,
            Action1Param<T> a) {
        return executeMapOperations(message, items, i -> i, a);
    }

    private static <T> Result executeMapOperations(String message,
            List<T> items,
            Func1Param<T, T> keySelector,
            Action1Param<T> a) {
        Result result = new Result();

        int latestWrittenCount = Integer.MIN_VALUE;
        int maxCount = items.size();
        Stopwatch sw;
        for (T item : items) {
            int percentComplete = (result.count * 100) / maxCount;
            if (percentComplete / 10 > latestWrittenCount / 10) {
                latestWrittenCount = percentComplete;
                System.out.printf("%1$s %2$s/%3$s (%4$s)%%" + "\r\n", message, item, Collections.max(items, null), percentComplete);
            }

            T key = keySelector.invoke(item);
            sw = Stopwatch.createStarted();
            a.invoke(key);
            sw.stop();

            result.count++;
            result.totalTicks += sw.elapsed(TimeUnit.MILLISECONDS);
        }

        return result;
    }

    private static final class Result {

        int count;
        long totalTicks;

        Double getAverageTicks() {
            if (count == 0) {
                return null;
            }
            return (double) totalTicks / count;
        }

        public Result clone() {
            Result varCopy = new Result();

            varCopy.count = this.count;
            varCopy.totalTicks = this.totalTicks;

            return varCopy;
        }
    }
}
