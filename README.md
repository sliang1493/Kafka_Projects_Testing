# Kafka_Projects_Testing
Two short projects working with Kafka to push and pull messages and connecting to a PostgreSQL database for writing changes.


## Project_1:
**Project Overview:**  
Read a filtered set of rows from a csv file into Kafka, then pull the messages to insert into a PostgreSQL table.  
In addition, track the running total of salaries by department in a second table.  

This folder contains the raw csv source file, the python code for creating Kafka consumers and producers, and the SQL queries to create the destination table and confirm that data has been populated.

* Employee_Salaries.csv - the raw data csv
* kafka_consumer_one.py - creates the consumer to consume messages from Kafka (sent by kafka_producer_one) and writes the results into PostgreSQL
* kafka_consumer_test_one.py - tests the connection to the Kafka cluster
* kafka_producer_one.py - creates the producer to read the csv file and send each row as a message to the Kafka cluster (to be used by kafka_consumer_one)
* proj1_queries.sql - contains SQL queries to create the two destination tables in PostgreSQL
* test_csv_reader.py - deprecated file for testing reading from the csv file

## Project_2:
**Project Overview:**  
Monitor an employees data table for changes using triggers and functions in postgres to create a separate audit table.  
Send the changes (inserts) to this audit table as messages to the Kafka cluster.  
Then, pull the messages from Kafka to make CRUD operations to another table in a separate PostgreSQL port. 

This folder contains the python code for creating the Kafka consumers and producers and the SQL queries to create the source and destination tables, create triggers, and some test CRUD operations.

* dest_table_queries.sql - SQL queries for creating the employees table in the destination database and verifying that the data has been entered correctly
* kafka_consumer_two.py - creates the consumer to consume messages from Kafka (sent by kafka_producer_two) and writes the results into the PostgreSQL destination
* kafka_producer_two.py - creates the producer to read changes to the audit table in the PostgreSQL source and write messages to the Kafka cluster (to be used by kafka_consumer_two)
* source_table_queries.sql - SQL queries for creating the source tables (employees and audit log), the trigger and functions to activate on CRUD updates to employees, and some basic test CRUD operations


## Configuration:
Both projects use the same yaml configuration, with db-1 for project_1 and db_source and db_dest for project_2.
