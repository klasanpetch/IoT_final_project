import os
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import paho.mqtt.client as mqtt
import json
import requests

# Load environment variables from ".env" file
load_dotenv()

# InfluxDB Configuration
BUCKET = os.environ.get('INFLUXDB_BUCKET')
INFLUXDB_URL = os.environ.get('INFLUXDB_URL')
INFLUXDB_TOKEN = os.environ.get('INFLUXDB_TOKEN')
INFLUXDB_ORG = os.environ.get('INFLUXDB_ORG')

# Initialize InfluxDB Client
influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

# MQTT Configuration
MQTT_BROKER_URL = os.environ.get('MQTT_URL')
MQTT_USERNAME = os.environ.get('MQTT_USERNAME')
MQTT_PASSWORD = os.environ.get('MQTT_PASSWORD')
MQTT_PUBLISH_TOPIC = "@msg/data"
MQTT_CLIENT_ID = os.environ.get('MQTT_CLIENT_ID')

# Initialize the MQTT Client
mqtt_client = mqtt.Client(client_id=MQTT_CLIENT_ID)
mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

# REST API endpoint for predicting output
predict_url = os.environ.get('PREDICT_URL')

# Callback for connection
def on_connect(client, userdata, flags, rc):
    """The callback for when the client connects to the broker."""
    if rc == 0:
        print(f"Connected to MQTT Broker at {MQTT_BROKER_URL}")
        client.subscribe(MQTT_PUBLISH_TOPIC)
    else:
        print(f"Failed to connect, return code {rc}")

# Callback for message handling
def on_message(client, userdata, msg):
    """The callback for when a message is received."""
    try:
        # Decode the message payload
        payload = json.loads(msg.payload.decode('utf-8'))

        # Write data to InfluxDB
        write_to_influxdb(payload)

        # Send data to the prediction API
        post_to_predict(payload)
        
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")

# Function to post data to prediction API
def post_to_predict(data):
    try:
        response = requests.post(predict_url, json=data)
        if response.status_code == 200:
            print("POST request successful")
        else:
            print(f"POST request failed with status code {response.status_code}")
    except requests.RequestException as e:
        print(f"Error in POST request: {e}")

# Function to write data to InfluxDB
def write_to_influxdb(data):
    try:
        # Create a point to represent the data
        point = Point("sensor_data") \
            .field("temp_BMP280", data["temp_BMP280"]) \
            .field("temp_HTS221", data["temp_HTS221"]) \
            .field("humid_HTS221", data["humid_HTS221"]) \
            .field("pressure_BMP280", data["pressure_BMP280"])

        # Write data to the specified bucket
        write_api.write(bucket=BUCKET, org=INFLUXDB_ORG, record=point)
        print("Data written to InfluxDB")
        
    except Exception as e:
        print(f"Error writing to InfluxDB: {e}")

# Register callback functions
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Connect to MQTT broker and start the MQTT client loop
try:
    mqtt_client.connect(MQTT_BROKER_URL, 1883)
    mqtt_client.loop_forever()
except Exception as e:
    print(f"Error with MQTT client: {e}")
