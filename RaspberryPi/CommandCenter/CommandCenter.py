# https://github.com/influxdata/influxdb-client-python/blob/master/examples/query_response_to_json.py

# Importing relevant modules
import os
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import paho.mqtt.client as mqtt
import json
import time

# Load environment variables from ".env"
load_dotenv()

# InfluxDB config
BUCKET = os.environ.get('INFLUXDB_BUCKET')
INFLUXDB_URL = os.environ.get('INFLUXDB_URL')
INFLUXDB_TOKEN = os.environ.get('INFLUXDB_TOKEN')
INFLUXDB_ORG = os.environ.get('INFLUXDB_ORG')
POLLING_INTERVAL = 60
print("connecting to",os.environ.get('INFLUXDB_URL'))
 
client = InfluxDBClient(
    url=str(os.environ.get('INFLUXDB_URL')),
    token=str(os.environ.get('INFLUXDB_TOKEN')),
    org=os.environ.get('INFLUXDB_ORG')
)
query_api = client.query_api()

# MQTT broker config
MQTT_BROKER_URL = os.environ.get('MQTT_URL')
MQTT_PUBLISH_TOPIC = "@msg/predict"
print("connecting to MQTT Broker", MQTT_BROKER_URL)
mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqttc.connect(MQTT_BROKER_URL,1883)


def poll_and_publish():
    try:
        query = 'from(bucket:"kla")\
            |> range(start: -10m)\
            |> filter(fn: (r) => r._measurement == "predicted_temperature")\
            |> filter(fn: (r) => r._field == "next_temperature")'
        tables = query_api.query(query)
        # Process query results
        for table in tables:
            for record in table.records:
                # Extract predicted temperature and timestamp
                predicted_temperature = record.get_value()
                timestamp = record.get_time()
                
                # Create a JSON object to publish
                data_to_publish = {
                    'timestamp': str(timestamp),
                    'predicted_temperature': predicted_temperature
                }
                
                # Publish data to MQTT
                mqttc.publish(MQTT_PUBLISH_TOPIC, json.dumps(data_to_publish))
                print(f"Published data to MQTT: {data_to_publish}")
    except Exception as e:
        # Handle any exceptions that might occur
        print(f"An error occurred: {e}")

try:
    while True:
        poll_and_publish()
        # Sleep for the specified polling interval
        time.sleep(POLLING_INTERVAL)
except KeyboardInterrupt:
    print("Polling interrupted.")
finally:
    # Clean up and close connections
    mqttc.disconnect()
    client.close()