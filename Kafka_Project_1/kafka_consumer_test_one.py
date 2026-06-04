import sys
from confluent_kafka import Consumer

def test_consumer_kafka_connection():
    # kafka configuration
    config = {
        'bootstrap.servers': 'localhost:29092',
        'group.id': 'test_connector',
        'auto.offset.reset': 'earliest'
    }

    print("Connecting to Kafka...")

    # try to connect to the kafka cluster and list out some metadata (the open topics)
    try:
        consumer = Consumer(config)

        cluster_meta = consumer.list_topics(timeout = 3.0)
        print("Connection succeeded")
        print(f"Cluster ID: {cluster_meta.cluster_id}")
        print(f"Open Topics: {list(cluster_meta.topics.keys())}")

        consumer.close()
    except Exception as e:
        print(f"Connection failed, error here: {e}")

if __name__ == "__main__":
    test_consumer_kafka_connection()
