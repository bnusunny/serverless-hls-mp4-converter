import boto3
import json
import m3u8
import os
import subprocess
import shutil
import uuid
from botocore.config import Config


s3_client = boto3.client('s3', 'eu-west-1', config=Config(
    s3={'addressing_style': 'path', 'signature_version': 's3v4'}))


def lambda_handler(event, context):
    # extract playlist object info from event
    bucket = event['bucket']
    s3_ojbect_key = event['key']
    prefix = os.path.dirname(s3_ojbect_key)+'/'
    master_playlist_name = os.path.basename(s3_ojbect_key)

    # create temp output directory
    output_dir = os.path.join('/tmp', str(uuid.uuid4()))
    try:
        os.mkdir(output_dir)
    except FileExistsError as error:
        print('directory exist')
    output_filename = 'recording.mp4'
    s3_output_key = prefix + output_filename

    # parse master.m3u8 to find out highest bandwidth stream and get the segment file names
    print('downloading ' + bucket + '/' + s3_ojbect_key)
    s3_client.download_file(bucket, s3_ojbect_key, output_dir + master_playlist_name)
    playlist_obj = m3u8.load(output_dir + master_playlist_name)
    # if playlist_obj.is_variant:
    playlist = playlist_obj.playlists[0]

    playlist_object_name = os.path.basename(playlist.uri)
    local_playlist = os.path.join(output_dir, playlist_object_name)
    s3_client.download_file(
        bucket, prefix + playlist.uri, local_playlist)

    playlist_folder = playlist.uri.split('/')[0]+'/'
    processed_playlist = os.path.join(output_dir, 'processed_' + playlist_object_name)
    with open(local_playlist, 'r') as reader:
        with open(processed_playlist, 'w') as writer:
            for line in reader:
                if line.startswith('#'):
                    writer.write(line)
                else:
                    new_line = s3_client.generate_presigned_url(
                        ClientMethod='get_object',
                        Params={'Bucket': bucket, 'Key': prefix+playlist_folder+line.strip()},
                        ExpiresIn=1200
                    )
                    writer.write(new_line+'\n')

    # merge the hls files on the fly and stream the merged video to s3
    print('Start to convert hls to mp4')
    output_name = 's3://'+bucket + '/'+prefix + 'recording.mp4'
    cmd = 'ffmpeg -protocol_whitelist file,http,https,tls,tcp -i '+processed_playlist+' -f mp4 -movflags frag_keyframe+empty_moov -bsf:a aac_adtstoasc -c copy - | /opt/awscli/aws s3 cp - ' + output_name
    print(cmd)
    subprocess.check_output(cmd, shell=True)

    # delete the temp output directory
    shutil.rmtree(output_dir)

    return  {
        "recording_s3_url": output_name
    }
