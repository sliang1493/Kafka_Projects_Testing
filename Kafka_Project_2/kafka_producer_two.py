from confluent_kafka import Producer
import json
import time
import psycopg2

class chg_producer():
    def __init__(self, kafka_dict, db_dict, topic_nm):
        kafka_config_settings = kafka_dict
        db_config_settings = db_dict
        
        # set up kafka producer and postgres connection
        self.cdc_producer = Producer(kafka_config_settings)

        self.conn = psycopg2.connect(**db_config_settings)
        self.cursor = self.conn.cursor()

        # set the topic name and the internal index to 0
        self.topic_name = topic_nm
        self.index = 0

    # return the current index. index is the audit_id or primary key from the table
    def get_index(self):
        return self.index
    
    # update the index to a new value
    def set_index(self, new_value):
        self.index = new_value

    # print if the message was delivered or if message delivery has failed
    def delivery_error_msg(self, err, msg):
        if err:
            print("Message delivery failed: ", err)
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def read_database_to_kafka(self):
        curr_index = self.get_index()
        try:
            while True:
                self.cursor.execute("""SELECT * FROM emp_audit WHERE audit_id > %s ORDER BY audit_id""", (curr_index,))
                new_rows = self.cursor.fetchall()

                if not new_rows:
                    print("Waiting for changes to employees table")
                    time.sleep(10)
                    continue
                for row in new_rows:
                    extr_data = {
                        "audit_id": row[0],
                        "emp_id": row[1],
                        "first_name": row[2],
                        "last_name": row[3],
                        "dob": row[4],
                        "city": row[5],
                        "salary": row[6],
                        "crud_action": row[7]
                    }

                    print(f"Retrieved data for row {row[0]}, entering into Kafka")
                    msg_val = json.dumps(extr_data, default=str).encode('utf-8')
                    self.cdc_producer.produce(
                        topic = self.topic_name,
                        value = msg_val,
                        callback = self.delivery_error_msg
                    )
                    self.cdc_producer.flush()
                    curr_index = row[0]
                    self.set_index(curr_index)
        except KeyboardInterrupt:
            print("Manual interruption, producer will close")
        except Exception as e:
            print(f"An error has occurred: {e}")
        finally:
            self.cursor.close()
            self.cdc_producer.close()
            self.conn.close()



if __name__ == '__main__':
    # postgresql source database configuration
    db_config = {
        "host": "localhost",
        "port": "5433",
        "database": "postgres",
        "user": "postgres",
        "password": "postgres"
    }

    # kafka producer configuration
    kafka_config = {
        'bootstrap.servers': 'localhost:29092'
    }

    topic_name = 'bf_employee_cdc'
    # create a chg producer and call the read from postgres to kafka method
    producer_two = chg_producer(kafka_config, db_config, topic_name)
    producer_two.read_database_to_kafka()