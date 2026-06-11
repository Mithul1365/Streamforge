import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Kafka se connect karo
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Patient list
patients = ['P001', 'P002', 'P003']

def generate_patient_data(patient_id):
    # Normal data
    data = {
        'patient_id': patient_id,
        'heart_rate': random.randint(60, 100),
        'spo2': random.randint(95, 100),
        'bp_systolic': random.randint(110, 130),
        'temperature': round(random.uniform(97.0, 99.0), 1),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    # 10% chance anomaly aaye
    if random.random() < 0.1:
        data['heart_rate'] = random.randint(130, 160)
        data['spo2'] = random.randint(80, 88)
        print(f"🚨 ANOMALY generated for {patient_id}!")
    
    return data

print("🏥 Patient Monitor Producer starting...")
print("Kafka mein data bhej raha hoon - Ctrl+C se band karo\n")

while True:
    for patient_id in patients:
        data = generate_patient_data(patient_id)
        producer.send('patient-vitals', value=data)
        print(f"✅ Sent: {data}")
    
    time.sleep(2)