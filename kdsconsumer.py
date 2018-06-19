import sys
import re
import boto3
import argparse
import json
import time
import datetime
from django.conf import settings
from argparse import RawTextHelpFormatter
from boto.kinesis.exceptions import ProvisionedThroughputExceededException
from dataxfer import DataStreamCarrier as SDR # Stream data record

# To preclude inclusion of aws keys into this code, you may temporarily add
# your AWS credentials to the file:
#     ~/.aws/credential
# as follows:
#     [Credentials]
#     aws_access_key_id = <your access key>
#     aws_secret_access_key = <your secret key>

iter_type_at = 'AT_SEQUENCE_NUMBER'
iter_type_after = 'AFTER_SEQUENCE_NUMBER'
iter_type_trim = 'TRIM_HORIZON'
iter_type_latest = 'LATEST'

minutes_running = 30

def send_record_to_endpoint(awsres, records):
  
  dsc = SDR()
  if awsres == 'sqs':
    dsc.send_sqs_message(dsc.make_sqs_connection(), records)
  else:
    dsc.send_s3_message(dsc.make_s3_connection(), records)

class KinesisDataConsumer(object):
  def __init__(self):
    pass  

  def get_kinesis_client(self):
    self.kinesis = boto3.client('kinesis',
                        aws_access_key_id=settings.AWS_KDS_ACCESS_KEY,
                        aws_secret_access_key=settings.AWS_KDS_SECRET_ACCESS_KEY,
                        region_name=settings.KDS_REGION)
    print ("{}".format(self.kinesis))
    return self.kinesis

  # gets all shard iterators in a stream,
  # and fetches most recent data
  def get_kinesis_data_iterator(self, stream):
    """Return list of shard iterators, one for each shard of stream."""
    
    dsc = VenueSDR()
    # Get data about Kinesis stream for Tag Monitor
    stream = self.kinesis.describe_stream(stream)
    
    # Get the shards in that stream
    shards = stream['StreamDescription']['Shards']

    # Collect together the shard IDs
    shard_ids = [shard[u"ShardId"] for shard in shards]

    seq_no = dsc.get_sequence_num(dsc.make_s3_connection())
    if seq_no == "":
      seq_no = shards["SequenceNumberRange"]["StartingSequenceNumber"]

    shard_iters = self.kinesis.get_shard_iterator(StreamName=stream, 
                                                  ShardId=shard_ids[0], 
                                                  ShardIteratorType=iter_type_trim,
                                                  StartingSequenceNumber=seq_no)

    shard_iterator = shard_iters['ShardIterator']

    end_time = datetime.now() + timedelta(minutes=minutes_running)
    while True:
      try:
        # Get data
        record_reponse = kinesis.get_records(ShardIterator=shard_iters[u"ShardIterator"], Limit=1)
        # Only run for sometime and stop looping if not records found
        now = datetime.now()
        print ("Time: {0}".format(now.strftime('%Y/%m/%d %H:%M:%S')))
        if end_time < now or not record_response:
          break

        # yield data to outside calling iterator
        for record in record_response['Records']:
          last_sequence = record['SequenceNumber']
          set_sequence_num(dsc.make_s3_connection(), last_sequence)
          yield json.loads(record['Data'])
        # Get next iterator for shard from previous request
        shard_iterator = record_response['NextShardIterator']

      # Catch exception meaning hitting API too much
      except boto.kinesis.exceptions.ProvisionedThroughputExceededException:
        print ("ProvisionedThroughputExceededException found. Sleeping for 0.5 seconds...")
        time.sleep(0.5)

      # Catch exception meaning iterator has expired
      except boto.kinesis.exceptions.ExpiredIteratorException:
        shard_iters = self.kinesis.get_shard_iterator(StreamName=stream,
                                                      ShardId=shard_ids[0], 
                                                      ShardIteratorType="AFTER_SEQUENCE_NUMBER", 
                                                      StartingSequenceNumber=last_sequence)
        shard_iterator = shard_iters['ShardIterator']


if __name__ == '__main__':
  parser = argparse.ArgumentParser(usage='%(prog)s [options]',
    description='''Connect to a Kinesis stream and create workers that hunt for record and sent into respect aws resource.''',
    formatter_class=RawTextHelpFormatter)
  parser.add_argument('stream_name',
    help='''the name of the Kinesis stream to connect''')
  parser.add_argument('--ops', type=str, default='sqs',
    help='''operation to be performed to send data to sqs or s3 [default: sqs]''')

  args = parser.parse_args()
  kdsObj = KinesisDataConsumer()

  kinesis = kdsObj.get_kinesis_client()
  records = kdsObj.get_kinesis_data_iterator(args.stream_name)

  send_record_to_endpoint(args.ops, records)
