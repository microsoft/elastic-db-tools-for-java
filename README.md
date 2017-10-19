# Elastic database tools for Azure SQL Database
Elastic database tools client library allows JAVA developers to create applications that use database sharding in Azure SQL Database. This repository contains the library (azure-elasticdb-tools) along with a sample project (azure-samples). For C# version of the database tools client library, see https://github.com/Azure/elastic-db-tools.

# Prerequisites
* A Java Developer Kit (JDK), v 1.8 or later
* [Maven](http://maven.apache.org/download.cgi)
* An instance of SQL Server 

# Sample Code
Follow the steps below to build JAR files and get started with the sample project: 
* Clone the repository 
* Run 'mvn install -Dmaven.test.skip=true' in ./azure-elastic-tools directory to build the JAR files in ./Target directory
     - If you want to run the tests, use 'mvn clean install' instead
     - You have to configure a test connection to either a local SQL Server or a logical server in Azure in ./azure-elastic-tools/src/test/resources/resource.properties 
* Edit ./azure-samples/src/main/resources/resource.propertires and configure your logical server in Azure
* Run 'mvn install' in ./azure-samples
* Run 'mvn -q exec:java "-Dexec.mainClass=com.microsoft.azure.elasticdb.samples.elasticscalestarterkit.Program"' in ./azure-samples directory

# Download
For using released builds, simply add the following dependancy to your POM file
```xml
<dependency>
	      <groupId>com.microsoft.azure</groupId>
	      <artifactId>azure-elasticdb-tools</artifactId>
	      <version>1.0.0</version>
</dependency>
```

# Contribute Code
This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

If you would like to become an active contributor to this project please follow the instructions provided in [Microsoft Azure Projects Contribution Guidelines](http://azure.github.io/guidelines.html).

1. Fork the repository
2. Create your branch (`git checkout -b my-new-branch`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

# License
The Microsoft JDBC Driver for SQL Server is licensed under the MIT license. See the [LICENSE](https://github.com/Microsoft/mssql-jdbc/blob/master/LICENSE) file for more details.

# Code of conduct
This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
