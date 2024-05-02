import os
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import paho.mqtt.client as mqtt
import json
import requests

# Load environment variables from ".env"
load_dotenv()

# InfluxDB config
BUCKET = os.environ.get('INFLUXDB_BUCKET')
INFLUXDB_URL = os.environ.get('INFLUXDB_URL')
INFLUXDB_TOKEN = os.environ.get('INFLUXDB_TOKEN')
INFLUXDB_ORG = os.environ.get('INFLUXDB_ORG')

# Initialize InfluxDB client
influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

# MQTT broker config
MQTT_BROKER_URL = os.environ.get('MQTT_URL')
MQTT_USERNAME = os.environ.get('MQTT_USERNAME')
MQTT_PASSWORD = os.environ.get('MQTT_PASSWORD')
MQTT_PUBLISH_TOPIC = "@msg/data"
MQTT_CLIENT_ID = os.environ.get('MQTT_CLIENT_ID')

# Initialize the MQTT client
mqtt_client = mqtt.Client(client_id=MQTT_CLIENT_ID)

# Authenticate with the broker
mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

# Connect to the broker
mqtt_client.connect(MQTT_BROKER_URL, 1883)

# REST API endpoint for predicting output
predict_url = os.environ.get('PREDICT_URL')

def on_connect(client, userdata, flags, rc):
    """Callback function when the client connects to the broker."""
    if rc == 0:
        print("Connected to MQTT Broker!")
        client.subscribe(MQTT_PUBLISH_TOPIC)
    else:
        print(f"Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    """Callback function when a message is received."""
    try:
        payload = json.loads(msg.payload.decode('utf-8'))
        
        # Write data to InfluxDB
        write_to_influxdb(payload)
        
        # Send data to prediction API
        post_to_predict(payload)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")

def post_to_predict(data):
    try:
        response = requests.post(predict_url, json=data)
        if response.status_code == 200:
            print("POST request successful")
        else:
            print(f"POST request failed with status code {response.status_code}")
    except requests.RequestException as e:
        print(f"Error in POST request: {e}")

def write_to_influxdb(data):
    try:
        # Format data for InfluxDB
        point = Point("sensor_data") \
            .field("temp_BMP280", data["temp_BMP280"]) \
            .field("temp_HTS221", data["temp_HTS221"]) \
            .field("humid_HTS221", data["humid_HTS221"]) \
            .field("pressure_BMP280", data["pressure_BMP280"])

        # Write data
        write_api.write(bucket=BUCKET, record=point)
        print("Data written to InfluxDB")
    except Exception as e:
        print(f"Error writing to InfluxDB: {e}")

# Set the MQTT callback functions
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Start the MQTT loop
mqtt_client.loop_forever()
