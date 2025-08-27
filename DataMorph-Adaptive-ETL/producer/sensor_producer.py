import os, json, time, random, uuid, datetime
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "sensors")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

devices = [f"device-{i}" for i in range(1,6)]

def sample_record(schema_version:int):
    base = {
        "device_id": random.choice(devices),
        "ts": datetime.datetime.utcnow().isoformat() + "Z",
        "temperature": round(random.uniform(18.0, 32.0), 2),
        "humidity": round(random.uniform(30.0, 90.0), 2),
    }
    # Simulate schema drift / extra fields
    if schema_version == 2:
        base["battery"] = round(random.uniform(20.0, 100.0), 1)
    if schema_version == 3:
        base["firmware"] = "v" + str(random.randint(1,5)) + "." + str(random.randint(0,9))
    return base

if __name__ == "__main__":
    print(f"Producing to {BOOTSTRAP} topic {TOPIC} ... CTRL+C to stop")
    i = 0
    try:
        while True:
            schema_version = 1 + (i % 3)
            rec = sample_record(schema_version)
            producer.send(TOPIC, rec)
            if i % 25 == 0:
                print("Sent:", rec)
            i += 1
            time.sleep(0.25)
    except KeyboardInterrupt:
        print("Stopping producer...")
