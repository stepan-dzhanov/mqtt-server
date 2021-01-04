
import paho.mqtt.client as mqtt
import json
import time
from influxdb import InfluxDBClient
from datetime import datetime
# Define Variables
MQTT_HOST = "localhost"
MQTT_PORT = 1883
MQTT_KEEPALIVE_INTERVAL = 45
MQTT_TOPIC = "stepan/test"

dbclient = InfluxDBClient('localhost', 8086, 'superadmin', 'casper77','sensor_db')


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, rc, properties=None):
    print("Connected with result code "+str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(MQTT_TOPIC)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(str(msg.payload))
    m_decode=str(msg.payload.decode("utf-8","ignore"))
    m_in=json.loads(m_decode)
    temperature = m_in["temperature"]
    time_stamp = m_in["time_stamp"]
    net_address = m_in["net_address"]
    sensor_type = m_in["type"]
    battery_status = m_in["battery_status"]
    event = m_in["event"]
    city_temperature = m_in["city_temperature"]
    print(temperature,time_stamp, net_address, sensor_type, battery_status,event,city_temperature)
    json_body = [
		{
			"measurement": "event_log",
			"tags": {
				"user": "Gate",
			},
			"time": datetime.now(),
			"fields": {
				"net_address": net_address,
				"type":sensor_type,
				"battery_status": battery_status,
				"event": event,
				"temperature": int(temperature)
				
			}
		}
	]
    dbclient.write_points(json_body)
    json_body1 = [
		{
			"measurement": "city_weather",
			"tags": {
				"user": "Weather",
			},
			"time": datetime.now(),
			"fields": {
				"city_temperature": city_temperature
				
				
			}
		}
	]
    dbclient.write_points(json_body1)



client = mqtt.Client()




client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_HOST,MQTT_PORT,MQTT_KEEPALIVE_INTERVAL)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.

client.subscribe(MQTT_TOPIC)
client.loop_forever()
