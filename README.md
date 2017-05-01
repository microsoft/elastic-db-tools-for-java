## Elastic database tools for Azure SQL Database

### Project Description
C# to Java conversion project for C# database tools client library at https://github.com/Azure/elastic-db-tools

### Build
* $ cd [project-dir]/azure-elasticdb-tools
* $ mvn clean install

### Completed Modules
* CreateShardMapManager
* CreateRangeShardMap
* CreateListShardMap
* CreateShard
* CreateRangeMapping
* CreatePointMapping
* GetShardMapManager
* TryGetRangeShardMap and GetRangeShardMap
* TryGetListShardMap and GetListShardMap
* Data Dependent Routing from Cached Mappings
* Unit Test Cases for ShardMapManagerFactory, ShardMapManager and ShardMaps

### Demo
Main class:
https://github.com/virtuositycg/azure-elasticdb-tools/blob/master/azure-samples/src/main/java/com/microsoft/azure/elasticdb/samples/elasticscalestarterkit/Program.java

Data: All Shard keys are assumed to be of type Integer

### Work in progress
* Complete Cache Implementation
* Multi Shard Query Implementation
* Unit Test cases for other modules