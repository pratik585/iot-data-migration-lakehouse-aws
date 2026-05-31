import threading
import time
import json
import random
from datetime import datetime
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

# CONFIG
ENDPOINT       = "<iot-endpoint>"
PORT           = 8883
ROOT_CA        = r"<Path-to-ROOT_CA-File>"
PRIVATE_KEY    = r"<Path-to-Private-key-file>"
CERTIFICATE    = r"<Path-to-Certificate-file>"
TOPIC          = "iot-o2-arena-motion"
DEVICE_COUNT   = 10
DURATION_SEC   = 60

def generate_random_coordinates():
    lat = round(random.uniform(51.500, 51.506), 6)
    lon = round(random.uniform(-0.005, 0.010), 6)
    return {"latitude": lat, "longitude": lon}

def device_thread(device_id):
    random_suffix = str(random.randint(0, 1000)).zfill(2)
    client_id = f"iot-device-motion-sensor-{device_id}"
    device_name = f"NXG1P{random_suffix}"

    mqtt = AWSIoTMQTTClient(client_id)
    mqtt.configureEndpoint(ENDPOINT, PORT)
    mqtt.configureCredentials(ROOT_CA, PRIVATE_KEY, CERTIFICATE)
    mqtt.configureAutoReconnectBackoffTime(1, 32, 20)
    mqtt.configureOfflinePublishQueueing(-1)
    mqtt.configureDrainingFrequency(2)
    mqtt.configureConnectDisconnectTimeout(10)
    mqtt.configureMQTTOperationTimeout(5)

    mqtt.connect()
    start_time = time.time()

    while time.time() - start_time < DURATION_SEC:
        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

        payload = {
            "_id_": device_name,
            "ts": timestamp,
            "geo_coordinates": generate_random_coordinates(),
            "motion_detected": random.choice(["0", "1"]),
            "device_status": "online"
        }

        mqtt.publish(TOPIC, json.dumps(payload), 1)
        print(f"[{client_id}] Sent: {payload}")
        time.sleep(1)

    mqtt.disconnect()

if __name__ == "__main__":
    threads = []
    for i in range(1, DEVICE_COUNT + 1):
        t = threading.Thread(target=device_thread, args=(i,))
        t.start()
        threads.append(t)
        time.sleep(0.1)  # slight stagger between starting threads

    for t in threads:
        t.join()

    print("All devices finished publishing.")
