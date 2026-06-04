from confluent_kafka import Producer
import csv
import json


# can restart the container to remove all messages
# kafka producer configuration
config = {
    'bootstrap.servers': 'localhost:29092',
    'enable.idempotence': True
}

producer_one = Producer(config)
topic_nm = "salary_data"

# print if the message was delivered or if message delivery has failed
def delivery_error_msg(err, msg):
    if err:
        print("Message delivery failed: ", err)
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# using the file path, open up a csv reader and stream the data to Kafka
def stream_csv_to_kafka(file_path):
    valid_depts = ['ECC', 'CIT', 'EMS']
    row_count = 0
    with open(file_path, mode = 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            try:
                # only include data from the three departments ECC, CIT, EMS with hire date during or after 2010:
                if row.get('Department') in valid_depts and row.get('Initial Hire Date')[-4:] >= '2010':
                    row['Salary'] = int(float(row['Salary']))
                    message_value = json.dumps(row)
                    producer_one.produce(
                        topic = topic_nm,
                        value = message_value,
                        callback = delivery_error_msg
                    )
                    # handle delivery reports
                    producer_one.poll(0)
                    row_count += 1
            except Exception as e:
                print(f"Skipping row due to invalid data and error: {row} and {e}")
    
    producer_one.flush()
    print("CSV extraction complete with row count ", row_count)

if __name__ == "__main__":
    csv_file_loc = 'Employee_Salaries.csv'
    stream_csv_to_kafka(csv_file_loc)
