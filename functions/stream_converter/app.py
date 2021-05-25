import boto3
import os
import subprocess
import uuid
from botocore.config import Config


s3_client = boto3.client('s3', os.environ['AWS_REGION'], config=Config(
    s3={'addressing_style': 'path', 'signature_version': 's3v4'}))


def lambda_handler(event, context):
    bucket = event['bucket']
    prefix = event['prefix']
    playlist_object_name = event['playlist']

    local_playlist = '/tmp/' + playlist_object_name
    # print('bucket: ' + bucket)
    # print('object: ' + prefix + playlist_object_name)
    s3_client.download_file(
        bucket, prefix + playlist_object_name, local_playlist)

    processed_playlist = '/tmp/' + 'processed_' + playlist_object_name
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

    # merge the hls files on the fly and stream the merged video to s3
    print('Start to convert hls to mp4')
    output_name = 's3://'+bucket+'/'+prefix+str(uuid.uuid4())+'_recording.mp4'
    cmd = 'ffmpeg -v error -protocol_whitelist file,http,https,tls,tcp -i '+processed_playlist+' -f mp4 -movflags frag_keyframe+empty_moov -bsf:a aac_adtstoasc -c copy - | /opt/awscli/aws s3 cp - ' + output_name
    subprocess.check_output(cmd, shell=True)

    # delete temprary files
    os.remove(local_playlist)
    os.remove(processed_playlist)

    return {
        'bucket': bucket,
        'prefix': prefix,
        'recording': 'recording.mp4'
    }
