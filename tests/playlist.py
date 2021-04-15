import boto3
import os
from botocore.config import Config


bucket = 'mediaconvert-ap-sourtheast-1'
prefix = 'ivs/123456789012/AbCdef1G2hij/2020-06-23T20-12-32.152Z/a1B2345cdeFg/media/hls/chunked/'
s3_client = boto3.client('s3', os.environ['AWS_REGION'], config=Config(
    s3={'addressing_style': 'path', 'signature_version': 's3v4'}))

local_playlist = 'playlist.m3u8'
processed_playlist = 'processed_playlist.m3u8'
with open(local_playlist, 'r') as reader:
    with open(processed_playlist, 'w') as writer:
        for line in reader:
            if line.startswith('#'):
                writer.write(line)
            else:
                new_line = s3_client.generate_presigned_url(
                    ClientMethod='get_object',
                    Params={'Bucket': bucket, 'Key': prefix+line.strip()},
                    ExpiresIn=1200
                )
                writer.write(new_line+'\n')
