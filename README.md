# Oracle Blockchain Table Sample Code

This distribution contains [Java](http://www.oracle.com/technetwork/java/javase/downloads/index.html) source code and configuration file samples that enable independent verification of Oracle Blockchain table data.  The sample program also provide an example for publishing Oracle crypto-hashes to a Hyperledger fabric.

Oracle Database version 19.10 and above support the creation of Blockchain tables.  The data format for Blockchain table column data is described in the Oracle Database Administrator's [guide](https://docs-uat.us.oracle.com/en/database/oracle/oracle-database/21/admin/blockchain_reference.html#GUID-AF2B1AA3-CFDD-4CAF-84AC-F6C6CDFEB6D1)


## Prerequisites 

**Software:**
- Oracle Database 19.10 or higher
- Java version 9 or higher
- [JSON-Java](https://github.com/stleary/JSON-java) distributable

**Services:**
- [Oracle Blockchain Platform Cloud Service](https://www.oracle.com/application-development/cloud-services/blockchain-platform/)

## Blockchain Verification

### Configuration
The Blockchain verification program **`Verify_Rows.java`** requires the following information for connecting to the Database.  This information should be updated in the configuration file `config.properties` in the `VerifyRows` directory.

- **`hostname`:** Name of the Oracle Database Host
- **`oracle_sid`:** Oracle Database SID
- **`port`:** Port for Database connections

### Build
`Verify_Rows.java` requires [Oracle JDBC Driver Jar version 8.0](https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/), please ensure that ojdbc8.jar is available in the Java CLASSPATH before building `Verify_Rows.java`.  To build, execute the following:


    cd VerifyRows
    javac *.java


### Parameters and Sample Output
**`Verify_Rows.java`** verifies the rows in a Blockchain table, the program requires the following input parameters:


- **`COPY_BYTESFILE_FOR_FAILED`:** Boolean parameter specifying whether a failed verification should create an output file. The output file name contains contains the instance, chain and sequence number of the first row failing verification, for example bytes_1_1_2.dat indicates instance 1, chain_id 1, and sequence_no 2 failed verification.
- **`SCHEMA`:** Database schema for accessing the Blockchain table 
- **`TABLE`:**  Table name for an Oracle Blockchain table
- **`INSTANCE_ID`:** (Optional) The instance ID for the RAC instance whose chains should be verified
- **`CHAIN_ID`:** (Optional) Chain ID for a specific chain (valid values: 1-32)
- **`SEQUENCE_NO`:** (Optional) Sequeuence number for a specific row in the blockchain table


`Verify_Rows.java` uses the current directory for creating intermediate files used for the purpose of row verification.  The following is a sample output for Verify_Rows.java when a schema name and the blockchain table name are provided as input, implying the verification of all rows in the table across all instances and chains.

    $java Verify_Rows true sample_schema sample_table
    Verified 2 rows for instance id : 2 , chain id : 1 
    Verified 2 rows for instance id : 2 , chain id : 2
    Verified 4 rows for instance id : 2
    Verified 2 rows for instance id : 3 , chain id : 1
    Verified 2 rows for instance id : 3 , chain id : 2
    Verified 4 rows for instance id : 3
    Verified a total of 8 rows
    Deleted the file : bytesfile.dat
    
     
## Publish Hash
  
### Configuration
**PublishHash.java** requires the following configuration information to connect to an Oracle Database and the Oracle Blockchain platform. This information should be updated in the configuration file config.properties in the PublishHash dir.

- **`hostname`:** Name of the Oracle Database Host
- **`oracle_sid`:** Oracle Database SID
- **`port`:** Port for Database connections
- **`rest_server_url`:** URL for the REST server
- **`rest_server_port`:** Port for the REST server
- **`channel_id`:** Channel ID
- **`chaincode_name`:** Chaincode Name

### Build
 `PublishHash.java` requires [json-java.jar](https://search.maven.org/artifact/org.json/json/20210307/bundle) and[ Oracle JDBC Driver Jar version 8.0](https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/), .  Please ensure json-java.jar and ojdbc8.jar are available in the Java CLASSPATH before building `PublishHash.java`.  To build execute the following
    
    cd PublishHash
    javac *.java


### Parameters and Sample Output

**Parameters**:  `PublishHash.java` requires the following input parameters:

- **`Operation`:**  Specifying whether the action is to `read` or `write` from/to the Blockchain platform
- **`SCHEMA`:**` Schema name for the Blockchain table
- **`Table`:**  Blockchain table Name
- **`Instance_Id`:** Database instance ID
- **`Chain_Id`:**  Chain Id for the row whose crypto-hash is being read/written from/to the Blockchain platform 
- **`Sequence_No`:** Sequence number for the row whose crypto-hash is being read/written from/to the Blockchain platform

Sample usage output for `PublishHash.java` is listed below:

    $java PublishHash write sample_schema sample_table 1 1 1
    Insert Sucessful!
    Txn Id : 03a16b4e24f8e3ff12b3112ece791bc6aad8cd53abf520389e972bb13607be64
    Published Hash : FCAD2F699877B17054EC5FD13D54478BB12ED3F5F98B8FD014046A4460124A82516663A9EA421F974C5E135656AFE9AE39B3CC742E280FBDAA2FACAD950A0D1F

