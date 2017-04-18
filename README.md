## Elastic database tools for Azure SQL Database

### Project Description
C# to Java conversion project for C# database tools client library at https://github.com/Azure/elastic-db-tools

### Build
* $ cd [project-dir]/azure-elasticdb-tools
* $ mvn clean install

### Completed Modules
* GetShardMapManager
* TryGetRangeShardMap and GetRangeShardMap
* TryGetListShardMap and GetListShardMap

### Demo
Main class:
https://github.com/virtuositycg/azure-elasticdb-tools/blob/20468f0c45a33ecfbf87ec12b1ff1bfa1a1b0861/azure-samples/src/main/java/com/microsoft/azure/elasticdb/samples/elasticscalestarterkit/ProgramPhase1.java

Data: 
C# samples project main class (Program.cs) was used to insert data into db  

### Work in progress
* Create ShardMapManager db
* Created Shard Maps and Empty Shards
* Populating Shards
* Create Mappings between Shards and Shard Maps
