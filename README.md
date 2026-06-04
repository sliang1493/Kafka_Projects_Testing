# Kafka_Projects_Testing
Two short projects working with Kafka to push and pull messages and connecting to a PostgreSQL database for writing changes.

# Project_1:
Read a filtered set of rows from a csv file into Kafka, then pull the messages to insert into a PostgreSQL table.
In addition, track the running total of salaries by department in a second table.

# Project_2:
Monitor an employees data table for changes using triggers and functions in postgres to create a separate audit table.
Send the changes (inserts) to this audit table as messages to the Kafka cluster.
Then, pull the messages from Kafka to make CRUD operations to another table in a separate PostgreSQL port.

Both projects use the same yaml configuration, with db-1 for project_1 and db_source and db_dest for project_2.
