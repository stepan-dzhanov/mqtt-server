from influxdb import InfluxDBClient
from datetime import datetime
import serial
import time
import paho.mqtt.client as mqtt
import json
import requests

# Define Variables
MQTT_HOST = "174.138.14.251"
MQTT_PORT = 1883
MQTT_KEEPALIVE_INTERVAL = 45
MQTT_TOPIC = "stepan/test"

api_key = "b3b32b81368e5ad9e74ffc5a9c2805d3"
  
# base_url variable to store url 
base_url = "http://api.openweathermap.org/data/2.5/weather?"
  
def get_city_temperature():
	complete_url = base_url + "appid=" + api_key + "&q=" + "Galway"
	response = requests.get(complete_url) 
	x = response.json()
	if x["cod"] != "404": 
  
		# store the value of "main" 
		# key in variable y 
		y = x["main"] 
	  
		# store the value corresponding 
		# to the "temp" key of y 
		current_temperature = y["temp"] -273.15
	  
		# store the value corresponding 
		# to the "pressure" key of y 
		current_pressure = y["pressure"] 
	  
		# store the value corresponding 
		# to the "humidity" key of y 
		current_humidiy = y["humidity"] 
	  
		# store the value of "weather" 
		# key in variable z 
		z = x["weather"] 
	  
		# store the value corresponding  
		# to the "description" key at  
		# the 0th index of z 
		weather_description = z[0]["description"]  
		return current_temperature
	else:
		print("City not found")
  

	 



def on_publish(client, userdata, mid):
	print()

mqttc = mqtt.Client()

ser = serial.Serial('/dev/ttyUSB0', timeout=0.1)  # Open COM5, 5 second timeout
ser.baudrate = 9600
ser.isOpen()
time.sleep(1)

ser.flushInput()
ser.flushOutput()


def wait_for_gate():

	data_left = 0
	while True:
		while(data_left<34):
			data_left += ser.inWaiting()
			time.sleep(0.1)
			#print("data in buff %d",data_left)

		data = ser.read(64)
		print("data received:", data)
		if(data.decode('utf-8').find('lbt')>0):
			temperature = data.decode('utf-8')[9:11]
			event = "REGULAR"
			net_address = data[3]
			sensor_type = "3min_ONline"
			battery_status = "LOW"
			#print("lowbat detected")
			return net_address, sensor_type, event,battery_status, temperature
		if(data.decode('utf-8').find('nbt')>0):
			temperature = data.decode('utf-8')[9:11]
			event = "REGULAR"
			net_address = data[3]
			sensor_type = "3min_ONline"
			battery_status = "OK"
			#print("regular detected")
			return net_address, sensor_type, event,battery_status, temperature
		if(data.decode('utf-8').find('button')>0):
			temperature = 0
			event = "BUTTON"
			net_address = data[3]
			sensor_type = "3min_ONline"
			battery_status = "N/A"
			#print("button detected")
			return net_address, sensor_type, event,battery_status, temperature
		if(data.decode('utf-8').find('door')>0):
			temperature = 0
			event = "DOOR"
			net_address = data[3]
			sensor_type = "3min_ONline"
			battery_status = "N/A"
			#print("door detected")
			return net_address, sensor_type, event,battery_status, temperature
			
		
		
		

client = InfluxDBClient('localhost', 8086, 'admin', 'casper77','sensor_db')

while True:
	net_address, sensor_type, event,battery_status, temperature = wait_for_gate()
	print("Temperature:",temperature)
	print("Address:",net_address)
	print("Type:",sensor_type)
	print("Battery",battery_status)
	print("Event",event)
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
	client.write_points(json_body)
	city_temperatre = get_city_temperature()
	json_body1 = [
		{
			"measurement": "city_weather",
			"tags": {
				"user": "Weather",
			},
			"time": datetime.now(),
			"fields": {
				"city_temperature": city_temperatre
				
				
			}
		}
	]
	client.write_points(json_body1)
	
	
	
	
	mqttc.connect(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE_INTERVAL)
	string_datetime = datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)")

	MQTT_MSG=json.dumps({"time_stamp":string_datetime,"net_address":  net_address,"type": sensor_type,"battery_status":  battery_status, "temperature":temperature,"city_temperature": int(city_temperatre), "event": event})
	print("Publishing to broker",MQTT_HOST, MQTT_TOPIC,MQTT_MSG)
	
	mqttc.publish(MQTT_TOPIC,MQTT_MSG)
	mqttc.disconnect()

	results = client.query('SELECT * FROM "sensor_db"."autogen"."event_log"  GROUP BY "user"')
	#print(results)
	points = results.get_points(tags={'user':'Gate'})
	#for point in points:
		#print("Time: %s, Temperature: %i, Event: %s" % (point['time'], point['temperature'], point['event']))








