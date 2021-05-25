import boto3
import os
import subprocess
import uuid
from botocore.config import Config
from urllib.parse import urlparse


s3_client = boto3.client('s3', 'eu-west-1', config=Config(
    s3={'addressing_style': 'path', 'signature_version': 's3v4'}))


def lambda_handler(event, context):
    contact_file = '/tmp/playlists.txt'
    with open(contact_file, 'w') as writer:
        for playlist in event['playlist_videos']:
            _parsed = urlparse(playlist['recording_s3_url'])
            presigned_url = s3_client.generate_presigned_url(
                            ClientMethod='get_object',
                            Params={'Bucket': _parsed.netloc, 'Key': _parsed.path.lstrip('/')},
                            ExpiresIn=1200
                        )
            writer.write('file {} \n'.format(presigned_url))

    # merge playlists
    print("merge video segments ....")
    _parsed = urlparse(event['playlist_videos'][0]['recording_s3_url'])
    bucket = _parsed.netloc
    prefix = _parsed.path.lstrip('/')
    channel_prefix = '/'.join(prefix.split('/')[:4])
    object_prefix = '{}/recordings'.format(channel_prefix)
    object_name = '{}.mp4'.format(str(uuid.uuid4()))
    output_key = '{}/{}'.format(object_prefix, object_name)
    s3_output = 's3://{}/{}'.format(bucket, output_key)
    cmd = 'ffmpeg -protocol_whitelist file,http,https,tls,tcp -f concat -safe 0 -i ' + contact_file + ' -f mp4 -movflags frag_keyframe+empty_moov -bsf:a aac_adtstoasc -c copy - | /opt/awscli/aws s3 cp - ' + s3_output
    print(cmd)
    subprocess.check_output(cmd, shell=True)

    os.remove(contact_file)

    return {
        "job_id": event['job_id'],
        "bucket": bucket,
        "key": output_key,
        "object_prefix": object_prefix,
        "object_name": object_name,
        "segment_time": "60",
        "create_hls": "0",
        "options": event['options'],
        "merged_recording_url": s3_output
    }
