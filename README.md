Map Reduce - Map Only - Cassandra Import Example
=========

This project provides a simple example of how to write map only tasks that import data into Cassandra using a Cassandra driver.

The purpose of this simple example is that it was heard users are doing/interested in this approach but there was little documentation available for this.


Problem
=========
The problem that this example solves is, how to load a lot of data into Cassandra from Hadoop in parallel without using a reducer.  

Solution
=========
The solution approach taken in this example was to create a map only mr job that leverages the DataStax 2.0 driver to insert data directly into Cassandra from within the map tasks.

This is a very simplified example that simply inserts key/value pairs (the full split is inserted).  The approach can be exapnded upon by either leveraging a different InputFormat to create unique splits, or by transforming data within a map task.  

Steps to Execute
=========
The following steps should be followed to execute this example:

1. Setup your environment
	1. We used hadoop 2.2 + yarn
	2. We used DataStax Enterprise 4.0.1 (OSS C* 2.0 could be used as well)
2. Create .ddl in Cassandra using the schema.ddl file
	1. ./cqlsh <ip> -f schema.ddl
3. Copy pom.xml and src directory into a directoy
4. Use Maven to create a project
5. In the MRExample.java file change the following line to include your node ip addresses
	6. private static final String NODES = "ENTER YOUR NODES LIST HERE";
6. Make a jar containing the mrexample files
7. Explicitly download the dependencies for the DataStax driver to pass into Hadoop
	1. Find dependencies [here](http://mvnrepository.com/artifact/com.datastax.cassandra/cassandra-driver-core/2.0.1) for the DataStax Java Driver 2.0.1
8. Execute the following command for hadoop 
	1. hadoop jar {yourjar.jar} com.datastax.mrexample.MRExample -libjars cassandra-driver-core-2.0.1.jar,guava-16.0.1.jar,metrics-core-3.0.2.jar,netty-3.9.0.Final.jar,lz4-1.2.0.jar,testng-6.8.8.jar,snappy-java-1.0.4.1.jar {input path on hadoop} {output path on hadoop}