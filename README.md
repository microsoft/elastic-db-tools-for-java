## Elastic database tools for Azure SQL Database

### Project Description
C# to Java conversion project for C# database tools client library at https://github.com/Azure/elastic-db-tools

### Build
* $ cd [project-dir]/azure-elasticdb-tools
* $ mvn clean install

### Completed Modules
* Shard Management
* Data Dependent Routing
* Multi Shard Querying
* Recovery Manager
* Shard Map Scalability
* Shard Management Unit Test Cases (177)

### Demo
Main class:
https://github.com/virtuositycg/azure-elasticdb-tools/blob/master/azure-samples/src/main/java/com/microsoft/azure/elasticdb/samples/elasticscalestarterkit/Program.java

Shard Map Scalability: https://github.com/virtuositycg/azure-elasticdb-tools/blob/master/azure-elasticdb-shard/src/test/java/com/microsoft/azure/elasticdb/shardmapscalability/Program.java

Data: All Shard keys are assumed to be of type Integer

### Work in progress
* Exception Handling in Multi Shard Query Module
* Unit Test cases in Multi Shard Query Module