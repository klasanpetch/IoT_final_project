"""
Real-Time prediction
Retrieve streaming data from the consumer and predict the number 
of people inside the room. Utilize Flask, a simple REST API server, 
as the endpoint for data feeding.
"""

# Importing relevant modules
import joblib
import pandas as pd
from flask import Flask, request, jsonify
from dotenv import load_dotenv
import os
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import ASYNCHRONOUS
import json

# Load the trained model
#joblib.load(filename, mmap_mode=None)
#Reconstruct a Python object from a file persisted with joblib.dump.
svm_model = joblib.load('svm_model.pkl')
svm_columns = joblib.load("svm_model_columns.pkl")

# Load environment variables from ".env"
load_dotenv()

# InfluxDB config
BUCKET = os.environ.get('INFLUXDB_BUCKET')
print("connecting to",os.environ.get('INFLUXDB_URL'))
client = InfluxDBClient(
    url=str(os.environ.get('INFLUXDB_URL')),
    token=str(os.environ.get('INFLUXDB_TOKEN')),
    org=os.environ.get('INFLUXDB_ORG')
)
write_api = client.write_api()

# Create simple REST API server
app = Flask(__name__)

# Create a list to store the last five temperature readings
recent_temperatures = []

# Default route: check if model is available.
@app.route('/')
def check_model():
    if svm_model:
        return "Model is ready for prediction"
    return "Server is running but something wrongs with the model"

# Predict route: predict the output from streaming data
@app.route('/predict', methods=['POST'])
def predict():
    try:
        # Parse the JSON data from the request
        json_text = request.data
        json_data = json.loads(json_text)

        # Check if the data contains the expected key 'temp_BMP280'
        if 'temp_BMP280' not in json_data:
            return "Invalid input: 'temp_BMP280' key not found in input data.", 400
        
        # Extract the temperature value
        temp = json_data['temp_BMP280']
        print("temp = ", temp)
        # Append the temperature reading to the list of recent temperatures
        recent_temperatures.append(temp)
        print("recent_temperature = ",recent_temperatures)
        # Maintain the list at a length of exactly five readings
        if len(recent_temperatures) > 5:
            recent_temperatures.pop(0)
        
        # Check if we have enough readings to make a prediction
        if len(recent_temperatures) < 5:
            return "Insufficient data: Waiting for five temperature readings before predicting.", 200
        
        # Create a DataFrame from the recent temperatures
        query = pd.DataFrame([recent_temperatures], columns=svm_columns)
        
        # Make the prediction using the SVM model
        predict_sample = svm_model.predict(query)
        
        # Convert the predicted value to float
        predicted_output = float(predict_sample[0])
        
        # Log the predicted output
        print(f"Predicted output: {predicted_output}")

        # Create a Point with the predicted temperature and write it to InfluxDB
        point = Point("predicted_temperature")\
            .field("next_temperature", predicted_output)\
            .field("Error", abs((predicted_output-float(temp))*100/float(temp)))
        write_api.write(BUCKET, os.environ.get('INFLUXDB_ORG'), point)

        # Return the predicted output as JSON response
        return jsonify({"Predicted output": predicted_output}), 200
    
    except:
        # Something error with data or model
        return "Recheck the data", 400
    
if __name__ == '__main__':
    app.run()