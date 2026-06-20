import torch
import torch.nn as nn
import numpy as np
import json
import random
from kafka import KafkaConsumer, KafkaProducer

# ============================================
# STEP 1: Generate training data (NORMAL patients)
# ============================================
def generate_normal_training_data(num_samples=500):
    data = []
    for _ in range(num_samples):
        sample = [
            random.uniform(60, 100),
            random.uniform(95, 100),
            random.uniform(110, 130),
            random.uniform(97.0, 99.0)
        ]
        data.append(sample)
    return np.array(data, dtype=np.float32)

# ============================================
# STEP 2: Build the LSTM Autoencoder model
# ============================================
class LSTMAutoencoder(nn.Module):
    def __init__(self, input_size=4, hidden_size=8):
        super(LSTMAutoencoder, self).__init__()
        self.encoder = nn.LSTM(input_size, hidden_size, batch_first=True)
        self.decoder = nn.LSTM(hidden_size, input_size, batch_first=True)

    def forward(self, x):
        encoded, _ = self.encoder(x)
        decoded, _ = self.decoder(encoded)
        return decoded

# ============================================
# STEP 3: Train the model
# ============================================
def train_model():
    print("Generating training data...")
    training_data = generate_normal_training_data(500)

    mean = training_data.mean(axis=0)
    std = training_data.std(axis=0)
    normalized_data = (training_data - mean) / std

    tensor_data = torch.tensor(normalized_data).unsqueeze(1)

    model = LSTMAutoencoder(input_size=4, hidden_size=8)
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)

    print("Training the model...")
    epochs = 100
    for epoch in range(epochs):
        optimizer.zero_grad()
        output = model(tensor_data)
        loss = criterion(output, tensor_data)
        loss.backward()
        optimizer.step()

        if (epoch + 1) % 20 == 0:
            print(f"   Epoch {epoch+1}/{epochs} - Loss: {loss.item():.4f}")

    print("Model training complete!\n")
    return model, mean, std

# ============================================
# STEP 4: Check for anomaly
# ============================================
def check_anomaly(model, mean, std, patient_data, threshold=1.5):
    sample = np.array([
        patient_data['heart_rate'],
        patient_data['spo2'],
        patient_data['bp_systolic'],
        patient_data['temperature']
    ], dtype=np.float32)

    normalized = (sample - mean) / std
    tensor_input = torch.tensor(normalized).unsqueeze(0).unsqueeze(1)

    model.eval()
    with torch.no_grad():
        reconstructed = model(tensor_input)
        error = torch.mean((tensor_input - reconstructed) ** 2).item()

    is_anomaly = error > threshold
    return is_anomaly, error

# ============================================
# STEP 5: Live monitoring from Kafka
# ============================================
def live_monitor(model, mean, std):
    consumer = KafkaConsumer(
        'patient-vitals-processed',
        bootstrap_servers=['localhost:29092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    producer = KafkaProducer(
        bootstrap_servers=['localhost:29092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    print("Live monitoring started - reading from Kafka...\n")

    for message in consumer:
        patient_data = message.value
        is_anomaly, error = check_anomaly(model, mean, std, patient_data)

        if is_anomaly:
            alert = {
                'patient_id': patient_data['patient_id'],
                'heart_rate': patient_data['heart_rate'],
                'spo2': patient_data['spo2'],
                'reconstruction_error': round(error, 4),
                'timestamp': patient_data['timestamp'],
                'severity': 'CRITICAL'
            }
            producer.send('alerts', value=alert)
            print(f"ALERT SENT - {alert}")
        else:
            print(f"Normal - {patient_data['patient_id']}: HR={patient_data['heart_rate']}, Error={error:.4f}")

# ============================================
# MAIN
# ============================================
if __name__ == "__main__":
    model, mean, std = train_model()
    live_monitor(model, mean, std)