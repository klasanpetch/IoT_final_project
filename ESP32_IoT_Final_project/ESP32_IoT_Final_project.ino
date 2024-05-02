
#include <WiFi.h>
#include <PubSubClient.h>

#include <Wire.h>
#include <SPI.h>
#include <Adafruit_Sensor.h>
#include <Adafruit_BMP280.h>
#include <Adafruit_HTS221.h>    //IIC humidity sensor
#include "Adafruit_SHT4x.h"  // New board has upgraged HTS221 to SHT4x
#include <ArduinoJson.h>
#define SDA_PIN 41
#define SCL_PIN 40

// Wifi Credentials
const char* ssid     = "Maxone1 2G";
const char* password = "0891117564";

// MQTT Broker Setting
const char* mqtt_broker = "192.168.1.48";
const char *mqtt_topic = "@msg/data";
const char* mqtt_username = "sanpetch";
const char* mqtt_password = "siriwuthinon";
const int mqtt_port = 1883;

// char msg[100];

WiFiClient espClient;
PubSubClient mqtt_client(espClient);

class Lock {
  SemaphoreHandle_t IIC, Serial;
  //constructure
public:
  Lock() {
    // create mutexes
    IIC = xSemaphoreCreateMutex();
    Serial = xSemaphoreCreateMutex();
  }
  // inline function is kind of text replacement
  inline void lockIIC() {
    while (xSemaphoreTake(IIC, 100) == pdFALSE)
      ;
  }
  inline void lockSerial() {
    while (xSemaphoreTake(Serial, 100) == pdFALSE)
      ;
  }
  inline void unlockIIC() {
    xSemaphoreGive(IIC);
  }
  inline void unlockSerial() {
    xSemaphoreGive(Serial);
  }
};
Lock *lock = new Lock;

QueueHandle_t BMP280_PreQ{ NULL };   // FIFO between BMP280 and TaskPreprocessing_Temp task
QueueHandle_t HTS221_PreQ{ NULL};    // FIFO between HTS221 and TaskPreprocessing_Temp task

struct float_sensor{
  float data1;
  float data2;
  float_sensor(float d1 = 0, float d2 = 0)
    : data1(d1), data2(d2) {}
};

class circularBuffer {
private:
  unsigned bufferLength, next;
  float_sensor *buffer;
public:
  circularBuffer(unsigned len = 1)
    : bufferLength(len) {
    buffer = new float_sensor[bufferLength];
    next = 0;
  }
  void newData(float_sensor *Data) {
    // put info into the new array
    buffer[next].data1 = Data->data1;
    buffer[next].data2 = Data->data2;
    // update pointer
    next += 1;
    next %= bufferLength;
  }
  float Data1(){
    float data1 = 0;
    for (int i = 0; i < bufferLength; i++) {
      data1 = buffer[i].data1;
    }
    return data1;
  }
  float Data2(){
    float data2 = 0;
    for (int i = 0; i < bufferLength; i++) {
      data2 = buffer[i].data2;
    }
    return data2;
  }
};


void mqttCallback(char *mqtt_topic, byte *payload, unsigned int length);

void setup() {
    Serial.begin(115200);
    extern TwoWire Wire;
    Wire.begin(SDA_PIN, SCL_PIN);
    connectToWiFi();
    mqtt_client.setServer(mqtt_broker, mqtt_port);
    // mqtt_client.setKeepAlive(60);
    mqtt_client.setCallback(mqttCallback); // Corrected callback function name
    connectToMQTT();
    xTaskCreate(
      TaskBMP280,
      "BMP280",
      2048,
      (void *)lock,
      1,
      NULL);
    xTaskCreate(
      TaskReadHTS221, "Task Read HTS221", 2048,  // Stack size
      (void *)lock,                              // pass lock as a parameter
      1,                                         // Priority
      NULL);
    xTaskCreate(
      TaskSendData, "Task Send Data", 2048,  // Stack size
      (void *)lock,                              // pass lock as a parameter
      1,                                         // Priority
      NULL);
    xTaskCreate(
      Reconnect_MQTT, "Task Reconnect MQTT", 2048,  // Stack size
      (void *)lock,                              // pass lock as a parameter
      3,                                         // Priority
      NULL);
}

void loop() {

}
    

void connectToWiFi() {
    WiFi.begin(ssid, password);
    Serial.print("Connecting to WiFi");
    while (WiFi.status() != WL_CONNECTED) {
        delay(500);
        Serial.print(".");
    }
    Serial.println("\nConnected to WiFi");
}

void connectToMQTT() {
    while (!mqtt_client.connected()) {
        String client_id = "esp32-client-";
        Serial.printf("Connecting to MQTT Broker as %s.....\n", client_id.c_str());
        if (mqtt_client.connect(client_id.c_str(), mqtt_username, mqtt_password)) {
            Serial.println("Connected to MQTT broker");
            mqtt_client.subscribe(mqtt_topic);
        } else {
            Serial.print("Failed, rc=");
            Serial.print(mqtt_client.state());
            Serial.println(" try again in 5 seconds");
            delay(5000);
        }
    }
}

void mqttCallback(char *mqtt_topic, byte *payload, unsigned int length) {
    Serial.print("Message received on mqtt_topic: ");
    Serial.println(mqtt_topic);
    Serial.print("Message: ");
    for (unsigned int i = 0; i < length; i++) {
        Serial.print((char) payload[i]);
    }
    Serial.println("\n-----------------------");
}

void TaskBMP280(void *pvParameters) {
  Lock *lock = (Lock *)pvParameters;
  Adafruit_BMP280 pressureSensor(&Wire);
  lock->lockIIC();
  bool status = pressureSensor.begin(BMP280_ADDRESS_ALT, BMP280_CHIPID);
  lock->unlockIIC();
  if (!status) {
    lock->lockSerial();
    Serial.println(F("Could not find a valid BMP280 sensor, check wiring or "
                     "try a different address!"));
    lock->unlockSerial();
    while (1) vTaskDelay(10000000L);
  }  // if BMP280 is not found then this task is doing nothing
  lock->lockIIC();
  /* Default settings from datasheet. */
  pressureSensor.setSampling(Adafruit_BMP280::MODE_NORMAL,     /* Operating Mode. */
                             Adafruit_BMP280::SAMPLING_X2,     /* Temp. oversampling */
                             Adafruit_BMP280::SAMPLING_X16,    /* Pressure oversampling */
                             Adafruit_BMP280::FILTER_X16,      /* Filtering. */
                             Adafruit_BMP280::STANDBY_MS_500); /* Standby time. */
  lock->unlockIIC();

  // create a queue between this task and preprocessor and pass the handle to a global variable
  // queue with 8 spaces for the temperature with timestamp (8 * 16 bytes)
  // BMP280_Temp_PreQ = xQueueCreate(8, sizeof(struct float_timestamp));

  BMP280_PreQ = xQueueCreate(8, sizeof(struct float_sensor));

  // sleep for a while to let other tasks start themselves up
  vTaskDelay(10);

 for (float temp, pressure, altitude; 1;) {
    float_sensor Data;
      lock->lockIIC();
    temp = pressureSensor.readTemperature();
    pressure = pressureSensor.readPressure();
      lock->unlockIIC();
      Data = float_sensor(temp, pressure);
      // send the new Data to the Q, if the Q is full, wait for 100 ticks.
      while (xQueueSend(BMP280_PreQ, &Data, (TickType_t)100) != pdTRUE);  
      // wait until the preprocessor do something about the data
      altitude = pressureSensor.readAltitude();
      lock->lockSerial();
    Serial.printf("Temperature from BMP280: %3.2f degree C.\n", temp);
    Serial.printf("Pressure from BMP280: %3.2f Pa.\n", pressure);
    // Serial.printf("Approx altitude from BMP280: %3.2f m.\n\n", altitude);
    // unlock Serial Port
      lock->unlockSerial();
    vTaskDelay(5000L);
  }
  // end of the task
}

void TaskReadHTS221(void *pvParameters) {
  // ------------- setup() part of this task --------------------
  // get the pointer of the lock from the parameter received
  Lock *lock = (Lock *)pvParameters;
  // declare the sensor
  Adafruit_HTS221 humiditySensor;
  Adafruit_SHT4x sht4;
  String sensor = "HTS221";
  // try to lock the shared IIC
  lock->lockIIC();
  // after locked the IIC successfully, start & read the sensor's status from the alternative adr
  bool status = humiditySensor.begin_I2C();
  bool status2 = sht4.begin();
  // then give the shared resource back ASAP
  lock->unlockIIC();

  if ((status || status2) == false) {
    // serial port is also another shared resource, take & give before & after use.
    lock->lockSerial();
    Serial.println("Failed to find HTS221 and SHT4x chip");
    lock->unlockSerial();
    // this task is going nowhere if the chip is not found
    while (1) { delay(1000000L); }
  } else if (status == true) {
    lock->lockSerial();
    Serial.printf("Great! found a HTS221 @Address=0x%X.\n", HTS221_I2CADDR_DEFAULT);
    lock->unlockSerial();
    // try to lock the shared IIC
    lock->lockIIC();
    // after locked the IIC successfully, set the data rate to one shot to save energy
    humiditySensor.setDataRate(HTS221_RATE_1_HZ);
    // then give the shared resource back ASAP
    lock->unlockIIC();
  } else { // status2 == true
    sensor = "SHT4x";
    lock->lockSerial();
    Serial.printf("Great! found a SHT4x @Address=0x%X.\n", SHT4x_DEFAULT_ADDR);
    lock->unlockSerial();

  }
  HTS221_PreQ = xQueueCreate(8, sizeof(struct float_sensor));
  // sleep for a while to let other tasks start themselves up
  vTaskDelay(10);
  
  for (sensors_event_t temp, humidity; 1;) {
    float temp1;
    float humidity1;
    // try to lock the shared IIC
    lock->lockIIC();
    // populate temp and humidity objects with fresh data
    if (status) 
      humiditySensor.getEvent(&humidity, &temp);
    else
      sht4.getEvent(&humidity, &temp);  
    // then give the shared resource back ASAP
    lock->unlockIIC();

    float_sensor Data;
      temp1 = temp.temperature;
      humidity1 = humidity.relative_humidity;
      
      Data = float_sensor(temp1, humidity1);
      Serial.printf("Temperature from %s: %3.2f degrees C.\n",sensor, temp1);
      Serial.printf("Hunidity from %s: %3.2f percent.\n",sensor, humidity1);
      // send the new Data to the Q, if the Q is full, wait for 100 ticks.
      while (xQueueSend(HTS221_PreQ, &Data, (TickType_t)100) != pdTRUE);
      // wait until the preprocessor do something about the data
    vTaskDelay(5000L);
  }
  // end of the task
}

void TaskSendData(void *pvParameters) {
  // ------------- setup() part of this task --------------------
  // get the pointer of the lock from the parameter received
  Lock *lock = (Lock *)pvParameters;

  float_sensor *buf1 = new float_sensor;
  float_sensor *buf2 = new float_sensor;
  circularBuffer read_BMP280Buffer(1);
  circularBuffer read_HTS221Buffer(1);
  vTaskDelay(2000);
  // forever loop
  for (;;) {
    // read the shared Q, put the data to buf. if Q is empty, wait for 10 ticks
    if (xQueueReceive(BMP280_PreQ, buf1, (TickType_t)10)){
        read_BMP280Buffer.newData(buf1);
        // lock->lockSerial();
        // Serial.printf("read BMP280 Buffer1 = ");
        // Serial.println(read_BMP280Buffer.Data1());
        // Serial.printf("read BMP280 Buffer2 = ");
        // Serial.println(read_BMP280Buffer.Data2());
        // lock->unlockSerial();
    }
    if (xQueueReceive(HTS221_PreQ, buf2, (TickType_t)10)){
        read_HTS221Buffer.newData(buf2);
        // lock->lockSerial();
        // Serial.printf("read HTS221 Buffer1 = ");
        // Serial.println(read_HTS221Buffer.Data1());
        // Serial.printf("read HTS221Buffer2 = ");
        // Serial.println(read_HTS221Buffer.Data2());
        // lock->unlockSerial();
    }
    // String data = "{\"data\": {\"temp_BMP280\":" + String(read_BMP280Buffer.Data1()) + ", \"temp_HTS221\":" + String(read_HTS221Buffer.Data1()) + ",\"humid_HTS221\":" + String(read_HTS221Buffer.Data2()) + ",\"pressure_BMP280\":" +  String(read_BMP280Buffer.Data2()) + "}}";
    // String data = "{\"temp_BMP280\":" + String(read_BMP280Buffer.Data1()) + ", \"temp_HTS221\":" + String(read_HTS221Buffer.Data1()) + ",\"humid_HTS221\":" + String(read_HTS221Buffer.Data2()) + ",\"pressure_BMP280\":" +  String(read_BMP280Buffer.Data2()) + "}";

    // Serial.println(data);
    // data.toCharArray(msg, (data.length() + 1));
    // mqtt_client.publish("@msg/data", msg); 
    // Serial.print("check message = ");
    // Serial.println(msg);
    // Create a StaticJsonDocument with a sufficient capacity for your JSON data
        StaticJsonDocument<256> doc;
        
        // Add the sensor data to the JSON document
        doc["temp_BMP280"] = read_BMP280Buffer.Data1();
        doc["temp_HTS221"] = read_HTS221Buffer.Data1();
        doc["humid_HTS221"] = read_HTS221Buffer.Data2();
        doc["pressure_BMP280"] = read_BMP280Buffer.Data2();

        // Serialize the JSON document to a string
        char jsonStr[256];
        size_t jsonStrSize = serializeJson(doc, jsonStr, sizeof(jsonStr));

        // Publish the JSON string to the MQTT topic
        mqtt_client.publish(mqtt_topic, jsonStr);

        // Print the JSON data to the Serial console for debugging
        lock->lockSerial();
        Serial.println(jsonStr);
        lock->unlockSerial();

        // Delay for 60 seconds before the next iteration
        vTaskDelay(5000L);
    }
    }
    
void Reconnect_MQTT(void *pvParameters){
for(;;)
  {
  if (!mqtt_client.connected()) {
        connectToMQTT();
    }
    mqtt_client.loop();
}
  vTaskDelay(1000); // one tick delay (1s) in between reads for stability 
  }