# example_consumer.py
import pika, os, csv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

my_bucket = "device1"
my_org    = "taller3"
my_token  = "oT3IoJqa9dllthFQ4ByP5yRlqz53jtLkSszbfpaseWxxm_06jic1RMGOVnZyxUu1dMmjIareFBx1kT-kKAmhbg=="

client = InfluxDBClient(url="http://influxdb:8086", token=my_token, org=my_org)

def process_function(msg):
  mesage = msg.decode("utf-8")
  print(mesage)
      
  write_api = client.write_api(write_options=SYNCHRONOUS)
  query_api = client.query_api()

  point = Point("Medicion").tag("Ubicacion", "Santa Marta").field("temperatura", float(mesage)).time(datetime.utcnow(), WritePrecision.NS)

  write_api.write(my_bucket, my_org, point)
    
  return

while 1:
  url = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@rabbit:5672/%2f')
  params = pika.URLParameters(url)
  connection = pika.BlockingConnection(params)
  channel = connection.channel() # start a channel
  channel.queue_declare(queue='mensajes') # Declare a queue
  # create a function which is called on incoming messages
  def callback(ch, method, properties, body):
    process_function(body)

  # set up subscription on the queue
  channel.basic_consume('mensajes',
    callback,
    auto_ack=True)

  # start consuming (blocks)
  channel.start_consuming()
  connection.close()