import datetime
import json
import boto.sqs
import boto.s3
from boto.s3.key import Key
from boto.sqs.message import Message
from django.conf import settings

class DataStreamCarrier(object):

  def __init__(self):
    pass

  def make_sqs_connection(self):
    conn = boto.sqs.connect_to_region(settings.KDS_SQS_REGION,
                                      aws_access_key_id=settings.KDS_SQS_ACCESS_KEY,
                                      aws_secret_access_key=settings.KDS_SQS_SECRECT)
    return conn

  def send_sqs_message(self, conn, payload):
    m = Message()
    if isinstance(payload, dict) or isinstance(payload, list):
      payload = json.dumps(payload)
    m.set_body(payload)
    sqs_q = conn.get_queue(settings.KDS_SQS_QUEUE_NAME)
    response = sqs_q.write(m)
        
  def make_s3_connection(self):
    conn = boto.s3.connect_to_region(settings.KDS_S3_REGION,
                                     aws_access_key_id=settings.KDS_S3_ACCESS_KEY,
                                     aws_secret_access_key=settings.KDS_S3_SECRECT)
    return conn

  def send_s3_message(self, conn, records):

    import tempfile
    import pickle

    temp_dir = tempfile.mkdtemp()
    fileName = tempfile.mkstemp()

    filepath = os.path.join(temp_dir, str(fileName))
    with open(filepath, 'wb') as batch:
      pickle.dump(records, batch)

    bucket=conn.get_bucket(settings.KDS_S3_BUCKET_NAME)
    #Get the Key object of the bucket
    k = Key(bucket)
    k.key = fileName
    #Upload the file
    result = k.set_contents_from_file(filepath)

    if os.path.exists(temp_dir):
      shutil.rmtree(temp_dir)

  def set_sequence_num(self, conn, seq_num):
    bucket=conn.get_bucket(settings.KDS_S3_BUCKET_NAME)
    key = bucket.new_key('seq_num.txt')
    key.set_contents_from_string(seq_num)

  def get_sequence_num(self, conn):
    from botocore.errorfactory import ClientError
    temp_dir = tempfile.mkdtemp()
    seq_filepath = os.path.join(temp_dir, 'seq_num.txt')	
    try:
      bucket=conn.get_bucket(settings.KDS_S3_BUCKET_NAME)
      key = bucket.get_key('seq_num.txt')
      key.get_contents_to_filename(seq_filepath)
    except ClientError:
      pass
    seq_num = ""
    if os.path.exists(seq_filepath):
      with open(seq_filepath, 'r') as myfile:
        seq_num = myfile.read()
    
    if os.path.exists(seq_filepath):
      shutil.rmtree(seq_filepath)    
    
    return seq_num
    