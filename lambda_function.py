import os
import json
import psycopg2
import boto3
import logging

from botocore.exceptions import ClientError
from psycopg2.extensions import STATUS_BEGIN

# rds settings
rds_host = "localhost"
user = "tanos"
password = os.environ.get("PASSWORD")
database = "tanos"
port = 5432

conn = None


def openConnection():
    global conn
    try:
        print("Opening Connection")
        if conn is None:
            conn = psycopg2.connect(
                host=rds_host, dbname=database, user=user, password=password, port=port, connect_timeout=5)
            print("conn.status : ", conn.status)
        elif conn.status == STATUS_BEGIN:
            '''
                conn.closed 
                1 -> STATUS_BEGIN
                0 -> STATUS_READY
            '''
            conn = psycopg2.connect(
                host=rds_host, dbname=database, user=user, password=password, port=port, connect_timeout=5)
            print("conn.status : ", conn.status)

    except Exception as e:
        print(e)
        print("ERROR: Unexpected error: Could not connect to RDS instance.")
        raise e


def send_sqs_message(sqs_queue_url, msg_body):
    # Send the SQS message
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='main-queue.fifo')
    try:
        response = queue.send_message(MessageBody=msg_body,
                                      MessageGroupId="messageGroup1")
    except ClientError as e:
        print("ERROR: [send_sqs_message]")
        logging.error(e)
        return None
    return response


def lambda_handler():
    item_count = 0
    try:
        openConnection()
        with conn.cursor() as cur:
            cur.execute("select 1")
            for row in cur:
                item_count += 1
                print(row)
    except Exception as e:
        # Error while opening connection or processing
        print(e)
    finally:
        print("Closing Connection")
        if conn is not None and conn.status == STATUS_BEGIN:
            conn.close()

    _params = {'message': "hello"}
    msg_body = json.dumps(_params)
    send_sqs_message(msg_body)

    return "Selected %d items from RDS table" % item_count


if __name__ == '__main__':
    lambda_handler()
