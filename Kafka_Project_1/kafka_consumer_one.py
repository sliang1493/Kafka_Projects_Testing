from confluent_kafka import Consumer, KafkaError
import json
import psycopg2
import time

# postgresql database configuration
db_config = {
    "host": "localhost",
    "port": "5432",
    "database": "postgres",
    "user": "postgres",
    "password": "postgres"
}

# kafka consumer configuration
kafka_config = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'data_consumer',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': False
}

# function for writing results to the postgres Department_Employee table using INSERT
def write_to_db_base(cursor, data):
    sql_query = """
    INSERT INTO Department_Employee (department, department_division, position_title, hire_date, salary)
    VALUES (%s, %s, %s, TO_DATE(%s , 'DD-Mon-YYYY'), CAST(%s AS integer))

    """
    cursor.execute(sql_query, (data.get('Department'), data.get('Department Division'), data.get('Position Title'), data.get('Initial Hire Date'), data.get('Salary')))

# function to update the running totals in the Department_Totals table by using INSERT ON CONFLICT UPDATE
def update_totals_db(cursor, data):
    sql_query = """
    INSERT INTO Department_Totals (department, total_salary)
    VALUES (%s, CAST(%s AS integer))
    ON CONFLICT (department)
    DO UPDATE SET total_salary = Department_Totals.total_salary + EXCLUDED.total_salary
    """
    cursor.execute(sql_query, (data.get('Department'), data.get('Salary')))

# establishes the connections and consumer to consume data from "salary_data" topic
def execute_connection():
    conn = psycopg2.connect(**db_config)
    cursor_one = conn.cursor()

    consumer_one = Consumer(kafka_config)
    consumer_one.subscribe(["salary_data"])

    print("Listening for messages")
    try:
        while True:
            msg = consumer_one.poll(timeout = 1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARITION_EOF:
                    continue
                else:
                    print(f"Error in consumer: {msg.error()}")
                    break
            
            # using the latest message, attempt to insert into Department_Employee and update Department_Totals
            try:
                msg_value = json.loads(msg.value().decode('utf-8'))
                write_to_db_base(cursor_one, msg_value)
                update_totals_db(cursor_one, msg_value)
                conn.commit()
                print("Inserted to employees tables and updated department totals")
                consumer_one.commit(asynchronous=False)
            except Exception as e:
                print(f"Error in processing message: {e}")
                
        
    except KeyboardInterrupt:
        pass
    finally:
        consumer_one.close()
        cursor_one.close()
        conn.close()


if __name__ == '__main__':
    execute_connection()


