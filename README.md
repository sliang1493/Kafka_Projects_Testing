# Kafka_Projects_Testing
Two short projects working with Kafka to push and pull messages and connecting to a PostgreSQL database for writing changes.


# Project_1:
Read a filtered set of rows from a csv file into Kafka, then pull the messages to insert into a PostgreSQL table.
In addition, track the running total of salaries by department in a second table.
This folder contains the raw csv source file, the python code for creating Kafka consumers and producers, and the SQL queries to create the destination table and confirm that data has been populated.

# Project_2:
Monitor an employees data table for changes using triggers and functions in postgres to create a separate audit table.
Send the changes (inserts) to this audit table as messages to the Kafka cluster.
Then, pull the messages from Kafka to make CRUD operations to another table in a separate PostgreSQL port.
This folder contains the python code for creating the Kafka consumers and producers and the SQL queries to create the source and destination tables, create triggers, and some test CRUD operations.

# Configuration:
Both projects use the same yaml configuration, with db-1 for project_1 and db_source and db_dest for project_2.
