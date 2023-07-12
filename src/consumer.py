import json

from kafka import KafkaConsumer

if __name__ == "__main__":
    consumer = KafkaConsumer(
        bootstrap_servers=["localhost:9092"],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="spotify_streaming_consumer",
        auto_offset_reset="earliest",
    )
    consumer.subscribe(["spotify_streaming_topic"])

    try:
        # Process messages
        for message in consumer:
            print(f"Received data: {message.value}")

    except KeyboardInterrupt:
        print("Stopping consumer..")
        consumer.close()
        print("Consumer stopped.")
