# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

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
* Unit Test Cases (205)
* Sample: Elastic Scale Starter Kit

### Demo
Main class:
https://github.com/virtuositycg/azure-elasticdb-tools/blob/master/azure-samples/src/main/java/com/microsoft/azure/elasticdb/samples/elasticscalestarterkit/Program.java

Shard Map Scalability: https://github.com/virtuositycg/azure-elasticdb-tools/blob/master/azure-elasticdb-shard/src/test/java/com/microsoft/azure/elasticdb/shardmapscalability/Program.java

Data: All Shard keys are assumed to be of type Integer
