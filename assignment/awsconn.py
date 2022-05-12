import os
import boto3
from kafka import KafkaConsumer, KafkaProducer

# to be modified with confluent-kafka
import subprocess
from json import dumps

# import re


def awsMenu():
    # POST
    # AWS_ACCESS_KEY_ID
    # AWS_SECRET_ACCESS_KEY
    # aws configure import --csv file://credentials.csv

    # UPDATE
    # import getpass
    # pswd = getpass.getpass('Password:')

    # Set environment variables
    # os.environ["AWS_ACCESS_KEY_ID"] = "username"
    # os.environ["AWS_SECRET_ACCESS_KEY"] = "secret"

    # ~/.aws/config on Linux, macOS, or Unix
    # Set the AWS_REGION environment variable.
    # On Linux, macOS, or Unix, use:
    # export AWS_REGION = your_aws_region

    # GET
    # Get environment variables
    # KEY = os.getenv("AWS_ACCESS_KEY_ID")
    # PASSWORD = os.environ.get("AWS_SECRET_ACCESS_KEY")

    # print(os.environ["DEBUSSY"])
    return True


def s3menu():
    start = False
    while not start:
        start = True
        state = True
        while state:
            # input = list(map(int, ((input()).strip().split())))
            command = input(str("\nSelect your S3 option: ").lower())
            if command in "help":
                print(
                    """
                    S3:
                    - help: for help
                    - ls: run ls of default bucket
                    - upload: uploads local file
                    - read: displays a file
                    - quit: exit aplication
                    """
                )
            elif command in "ls":
                bucketName = "enroute-bucket"
                print(f"========= File List in S3 Bucket: {bucketName} ===========\n")
                # ls: s3 ls <bucket-name>
                result = subprocess.run(
                    ["aws", "s3", "ls", bucketName], stderr=subprocess.PIPE, text=True
                )
                print(result.stderr)

            elif command in "upload":
                # # upload s3
                bucketName = "enroute-bucket"
                fileName = input(f"Type file name for Bucket: {bucketName}:\n ").strip()
                # directory = os.path.join(os.curdir, fileName)
                # os.chdir(directory)
                print(
                    f"========= Uploading File: {fileName} to S3 Bucket: {bucketName} ===========\n"
                )
                s3_upload = boto3.resource("s3")
                s3_upload.Object(bucketName, fileName).upload_file(Filename=fileName)
                client = boto3.client("s3")  # access to s3 bucket as client
                object = client.get_object(Bucket=bucketName, Key=fileName)
                print(object["Body"].read().decode())  # read object body as bytes

            elif command in "read":
                bucketName = "enroute-bucket"
                fileName = input(f"Type file name for Bucket: {bucketName}:\n ").strip()
                print(
                    f"========= Reading S3 Bucket: {bucketName} File: {fileName} ===========\n"
                )
                client = boto3.client("s3")  # access to s3 bucket as client
                object = client.get_object(Bucket=bucketName, Key=fileName)
                print(object["Body"].read().decode())  # read object body as bytes
                # read s3 * extract s3 serialized Credentials
                # convert serialized csv

            elif command in "quit":
                break
    return True


def kafkaMenu():
    start = False
    while not start:
        start = True
        state = True
        while state:
            # input = list(map(int, ((input()).strip().split())))
            command = input(str("\nSelect your Kafka option: ").lower())
            if command in "help":
                print(
                    """
                    Kafka MSK:
                    - help: for help
                    - ls: list current Topics on MSK
                    - brokers: list of MSK Brokers
                    - topic: creates topic
                    - producer: create a producer
                    - consumer: create a consumer
                    - quit: exit aplication
                    """
                )
            elif command in "ls":
                # list TOPICS: tls, iam
                # secure = 92,94,96,9098
                # port = str(input("Input secure Port:\n")).strip()
                print(f"========= List Topics ===========\n")

                port = "9094"

                awsmsk = "kafka-enroute.mx95vv.c11.kafka.us-west-2.amazonaws.com"
                servers = f"b-2.{awsmsk}:{port},b-1.kafka-enroute.mx95vv.c11.kafka.us-west-2.amazonaws.com:{port},b-3.kafka-enroute.mx95vv.c11.kafka.us-west-2.amazonaws.com:{port}"
                result = subprocess.run(
                    [
                        "./kafka-topics.sh",
                        "--list",
                        "--bootstrap-server",
                        servers,
                        "--command-config",
                        "client.properties",
                    ],
                    # stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                print(result.stdout)

            elif command in "broker":
                # list BROKERS
                print(f"========= List Brokers ===========\n")
                arnAws = "arn:aws:kafka:us-west-2:934841374016:cluster/kafka-enroute/f869a19b-656e-4050-8438-28bbacfbddec-11"
                result = subprocess.run(
                    [
                        "aws",
                        "kafka",
                        "get-bootstrap-brokers",
                        "--cluster-arn",
                        arnAws,
                    ],
                    # stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                print(result.stdout)

            elif command in "topic":
                # create TOPIC: tls, iam
                print(f"========= Create TOPIC ===========\n")
                topicName = str(input("Please name your TOPIC: \n> ")).strip()
                port = str(input("Input secure Port:\n> ")).strip()

                awsmsk = "kafka-enroute.mx95vv.c11.kafka.us-west-2.amazonaws.com"
                servers = f"b-2.{awsmsk}:{port},b-1.{awsmsk}:{port},b-3.{awsmsk}:{port}"
                result = subprocess.run(
                    [
                        "./kafka-topics.sh",
                        "--create",
                        "--topic",
                        topicName,
                        "--bootstrap-server",
                        servers,
                        "--command-config",
                        "client.properties",
                    ],
                    # stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    # stderr=subprocess.PIPE,
                    text=True,
                )
                print(result.stdout)

            elif command in "producer":
                # create producer:
                topicName = input(str("Please select your TOPIC: \n> "))
                port = str(input("Input secure Port:\n> ")).strip()

                awsmsk = "kafka-enroute.mx95vv.c11.kafka.us-west-2.amazonaws.com"
                servers = f"b-2.{awsmsk}:{port},b-1.{awsmsk}:{port},b-3.{awsmsk}:{port}"
                # cp /usr/lib/jvm/java-11-openjdk-11.0.13.0.8-2.amzn2022.x86_64/lib/security/cacerts /tmp/kafka.client.truststore.jks

                producer = KafkaProducer(
                    bootstrap_servers=servers,
                    value_serializer=lambda x: dumps(x).encode("utf-8"),
                    security_protocol="SSL",
                    ssl_truststore_location=r"/tmp/kafka.client.truststore.jks"
                    # ssl_cafile="/tmp/kafka.client.truststore.jks",
                    # ssl_crlfile
                )
                print(
                    f"========= Producer process running, sending messages to {topicName} ===========\n"
                )

                for _ in range(10):
                    raw = input(str("> "))
                    message = f"Message: {raw}"
                    producer.send(topicName, message)
                    producer.flush()
                    print(f"{message} sent!")
                break

            elif command in "consumer":
                topicName = input(str("Please select your TOPIC: \n> "))
                port = str(input("Input secure Port:\n")).strip()

                awsmsk = "kafka-enroute.mx95vv.c11.kafka.us-west-2.amazonaws.com"
                servers = f"b-2.{awsmsk}:{port},b-1.kafka-enroute.mx95vv.c11.kafka.us-west-2.amazonaws.com:{port},b-3.kafka-enroute.mx95vv.c11.kafka.us-west-2.amazonaws.com:{port}"

                consumer = KafkaConsumer(topicName, bootstrap_servers=servers)
                print(
                    f"========= Consumer process running, reading messages from {topicName} ===========\n"
                )
                for num, message in zip(range(10), consumer):
                    print(f"{num}: {message}")
                break

            elif command in "quit":
                break


def menu():
    start = False

    while not start:
        print("\nYour k8sService is ready, ")
        start = True
        state = True
        while state:
            # input = list(map(int, ((input()).strip().split())))
            command = input(str("\nPlease select your option: ").lower())
            if command in "help":
                print(
                    """
                Instructions:
                    - help: for help
                    - aws: more aws options
                    - s3: more s3 options
                    - kafka: more kafka options
                    - quit: exit aplication
                """
                )
            # elif command in "aws":
            #   pass
            # state = aws()
            elif command in "s3":
                state = s3menu()
            elif command in "kafka":
                state = kafkaMenu()
            elif command in "quit":
                break
            else:
                print("\nSorry. Type a valid input. Use Help.")
        # break


if __name__ == "__main__":
    menu()
