import json
from datetime import datetime

from kafka import KafkaConsumer
import boto3

TOPIC_NAME = "hello-world-topic"
SERVERS = [
    "b-1.kafka-enroute.mx95vv.c11.kafka.us-west-2.amazonaws.com:9092",
    "b-2.kafka-enroute.mx95vv.c11.kafka.us-west-2.amazonaws.com:9092",
    "b-3.kafka-enroute.mx95vv.c11.kafka.us-west-2.amazonaws.com:9092",
]

consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=SERVERS)

print(
    f"========= Consumer process running, reading messages from {TOPIC_NAME} ===========\n"
)

session = boto3.Session(
    aws_access_key_id="", aws_secret_access_key=""
)  # your session to aws
res = boto3.resource("s3")  # access to s3 bucket as resource
client = boto3.client("s3")  # access to s3 bucket as client
# get s3 object from enroute-bucket
object = client.get_object(
    Bucket="enroute-bucket", Key="hello-world-topic-2022-05-11 14:48:24.098654"
)
print(object["Body"].read())  # read object body as bytes

for message in consumer:
    s3_object = res.Object("enroute-bucket", f"{TOPIC_NAME}-{datetime.now()}")
    s3_object.put(Body=message.value)
    print(message.value)  # ._asdict() to get message data
    # seen as:
    # {'topic': 'hello-world-topic', 'partition': 0, 'offset': 8, 'timestamp': 1652280288746, 'timestamp_type': 0,
    #  'key': None, 'value': b'hola', 'headers': [], 'checksum': None, 'serialized_key_size': -1,
    #  'serialized_value_size': 4, 'serialized_header_size': -1}
