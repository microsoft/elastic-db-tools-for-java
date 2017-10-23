# Elastic database tools for JAVA (Azure SQL Database)
Elastic database tools client library allows JAVA developers to create applications that use database sharding in Azure SQL Database. This repository contains the library/tools along with a sample project. For C# version of the database tools client library, see https://github.com/Azure/elastic-db-tools.

# Prerequisites
* A Java Developer Kit (JDK), v 1.8 or later
* [Maven](http://maven.apache.org/download.cgi)
* A logical server in Azure or local SQL Server

# Building jars and running the sample code in [sample](https://github.com/Microsoft/elastic-db-tools-for-java/tree/develop/samples)
Follow the steps below to build JAR files and get started with the sample project: 
* Clone the repository 
* From the _./elastic-db-tools_ directory, build the jars using `mvn install -D maven.test.skip=true`. This creates the jars in the _./target_ directory.
     - If you want to run the tests, use `mvn clean install` instead. You will also have to configure a test connection by adding your username, password, and logical server name for either a logical server in Azure or local SQL Server in the _./tools/src/test/resources/resource.properties_ file.
* Edit the _./sample/src/main/resources/resource.properties_ file to add your username, password, and logical server name.
* From the _./sample_ directory, run `mvn install` to build the sample project.
* From the _./sample_ directory, run `mvn -q exec:java "-Dexec.mainClass=com.microsoft.azure.elasticdb.samples.elasticscalestarterkit.Program"` to start the sample project. 

# Download
For using the released JAR, simply add the following dependancy to your POM file:
```xml
<dependency>
      <groupId>com.microsoft.azure</groupId>
      <artifactId>elastic-db-tools</artifactId>
      <version>1.0.0</version>
</dependency>
```

# Contribute code
This project welcomes contributions and suggestions. Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

If you would like to become an active contributor to this project please follow the instructions provided in [Microsoft Azure Projects Contribution Guidelines](http://azure.github.io/guidelines.html).

1. Fork the repository
2. Create your branch (`git checkout -b my-new-branch`)
3. Commit your changes (`git commit -am 'Add some change/feature'`)
4. Push to the branch (`git push origin my-new-branch`)
5. Create new Pull Request

# License
The Elastic database tools client library for Java is licensed under the MIT license. See the [LICENSE](https://github.com/Microsoft/mssql-jdbc/blob/master/LICENSE) file for more details.

# Code of conduct
This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
