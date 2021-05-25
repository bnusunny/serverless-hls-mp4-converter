import boto3
import json
import os
import uuid

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

patch_all()

create_hls = os.environ['ENABLE_HLS']
segment_time = os.environ['DEFAULT_SEGMENT_TIME']
sfn_client = boto3.client('stepfunctions')

def lambda_handler(event, context):
    # print(options)
    print(event)
    hls_option = event['body']
    
    job_id = str(uuid.uuid4())
    
    #send message to sqs
    # Get the service resource
    sqs_client = boto3.resource('sqs')
    
    # Get the queue
    queue = sqs_client.get_queue_by_name(QueueName='serverless-transcoding-tasks')
    
    options = json.loads(hls_option)
    
    queue.send_message(MessageBody=json.dumps({
            'job_id': job_id,
            'bucket': 'fake',
            'key': 'fake',
            'object_prefix': 'fake',
            'object_name': 'fake',
            "segment_time": 'fake',
            'create_hls': 'fake',
            'options': options
        }))
        
    response = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'HLS merging started',
                'job_id': job_id
            })
        }
        
    return response    

