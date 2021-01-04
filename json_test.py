import paho.mqtt.client as mqtt
import json
import time
from influxdb import InfluxDBClient

# Define Variables
MQTT_HOST = "broker.emqx.io"
MQTT_PORT = 1883
MQTT_KEEPALIVE_INTERVAL = 45
MQTT_TOPIC = "stepan/test"


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, rc, properties=None):
    print("Connected with result code "+str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(MQTT_TOPIC)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(str(msg.payload))
    dbclient = InfluxDBClient('localhost', 8086, 'superadmin', 'casper77','sensor_db')
    m_decode=str(msg.payload.decode("utf-8","ignore"))
    m_in=json.loads(m_decode)
    
    print(m_in["temperature"])



client = mqtt.Client()




client.on_connect = on_connect
client.on_message = on_message

client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_HOST,MQTT_PORT,MQTT_KEEPALIVE_INTERVAL)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.

client.subscribe(MQTT_TOPIC)
client.loop_forever()


