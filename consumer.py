import json
from kafka import KafkaConsumer

# Kafka se connect karo
consumer = KafkaConsumer(
    'patient-vitals',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("🏥 Patient Monitor Consumer starting...")
print("Kafka se data padh raha hoon...\n")

for message in consumer:
    data = message.value
    
    # Normal ya anomaly check karo
    if data['heart_rate'] > 120 or data['spo2'] < 90:
        print(f"🚨 ALERT! Patient {data['patient_id']} — HR: {data['heart_rate']}, SpO2: {data['spo2']}%")
    else:
        print(f"✅ Normal — Patient {data['patient_id']} — HR: {data['heart_rate']}, SpO2: {data['spo2']}%")