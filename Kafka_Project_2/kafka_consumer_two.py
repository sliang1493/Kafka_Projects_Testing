from confluent_kafka import Consumer, KafkaError
import json
import time
import psycopg2

class chg_consumer():
    def __init__(self, kafka_dict, db_dict, topic_nm):
        kafka_config_settings = kafka_dict
        db_config_settings = db_dict
        
        # set up kafka consumer and postgres connection
        # subscribe the consumer to the correct topic
        self.cdc_consumer = Consumer(kafka_config_settings)
        self.topic_name = topic_nm
        self.cdc_consumer.subscribe([self.topic_name])
        print("Consumer subscribing to ", self.topic_name)

        self.conn = psycopg2.connect(**db_config_settings)
        self.cursor = self.conn.cursor()
    
    def load_data(self, msg_row):
        try:
            to_do_act = msg_row.get('crud_action')
            emp_id = msg_row.get('emp_id')
            first_name = msg_row.get('first_name')
            last_name = msg_row.get('last_name')
            dob = msg_row.get('dob')
            city = msg_row.get('city')
            salary = msg_row.get('salary')

            ins_query = """
                INSERT INTO employees(emp_id, first_name, last_name, dob, city, salary)
                VALUES(%s, %s, %s, %s, %s, %s)
            """
            upd_query = """
                UPDATE employees SET first_name = %s, last_name = %s, dob = %s, city = %s, salary = %s
                WHERE emp_id = %s
            """
            del_query = """
                DELETE FROM employees WHERE emp_id = %s
            """
            if to_do_act == 'INSERT':
                self.cursor.execute(ins_query, (emp_id, first_name, last_name, dob, city, salary))
                print("Performed insert with id ", emp_id)
            elif to_do_act == 'UPDATE':
                self.cursor.execute(upd_query, (first_name, last_name, dob, city, salary, emp_id))
                print("Performed update on id ", emp_id)
            elif to_do_act == 'DELETE':
                self.cursor.execute(del_query, (emp_id,))
                print("Performed deletion on id ", emp_id)
            else:
                print("Unknown action, please try again: ", to_do_act)
            self.conn.commit()
        except Exception as e:
            print(f"Error occurred during writing process: {e}")
        

    def execute_connection(self):
        print("Listening for messages")
        try:
            while True:
                msg = self.cdc_consumer.poll(timeout = 1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARITION_EOF:
                        continue
                    else:
                        print(f"Error in consumer: {msg.error()}")
                        break
                
                # using the latest message, attempt to load it into the employees table
                try:
                    new_data = json.loads(msg.value().decode('utf-8'))
                    self.load_data(new_data)
                except Exception as e:
                    print(f"Error processing message from kafka: {e}")
                    # send message to dead letter here
        except KeyboardInterrupt:
            print("Manual interruption, consumer will close")
        finally:
            self.cursor.close()
            self.cdc_consumer.close()
            self.conn.close()


if __name__ == '__main__':
    # postgresql dest database configuration
    db_config = {
        "host": "localhost",
        "port": "5434",
        "database": "postgres",
        "user": "postgres",
        "password": "postgres"
    }

    # kafka producer configuration
    kafka_config = {
        'bootstrap.servers': 'localhost:29092',
        'group.id': 'employee_general_consumer',
        'auto.offset.reset': 'latest'
    }

    topic_name = 'bf_employee_cdc'
    # create a chg consumer and call the execution method
    consumer_two = chg_consumer(kafka_config, db_config, topic_name)
    consumer_two.execute_connection()
