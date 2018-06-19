import boto3
import json
import time
import datetime
import time
import os
from django.conf import settings

class KinesisDataProducer(object):
  def __init__(self):
    # the partition key to use when communicating records to the stream
    partkey_template = 'MyKinesis-001'
    self.partition_key = partkey_template
    pass

  def get_kinesis_client(self):
    self.kinesis = boto3.client('kinesis',
                        aws_access_key_id=settings.AWS_KDS_ACCESS_KEY,
                        aws_secret_access_key=settings.AWS_KDS_SECRET_ACCESS_KEY,
                        region_name=settings.KDS_REGION)
    print ("{}".format(self.kinesis))
    return self.kinesis

  def put_records(self, records):
    '''Put the given records in the Kinesis stream.'''
    string_payload = json.dumps(records)
    response = self.kinesis.put_record(StreamName='<kinesis-stream-name>', 
                                Data=string_payload, 
                                PartitionKey=self.partition_key)
    return response

  def process_kinesis_stream(self, client, data):
    response = self.put_records(data)
    print("-= put seqNum:", response['SequenceNumber'])

