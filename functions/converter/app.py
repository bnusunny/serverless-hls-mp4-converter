import boto3
import concurrent.futures
import m3u8
import os
import subprocess
import shutil
import uuid
from botocore.config import Config


s3_client = boto3.client('s3', os.environ['AWS_REGION'], config=Config(
    s3={'addressing_style': 'path'}))
efs_path = os.environ['EFS_PATH']

def lambda_handler(event, context):
    # extract playlist object info from event
    bucket = event['detail']['recording_s3_bucket_name']
    prefix = event['detail']['recording_s3_key_prefix']
    s3_ojbect_key = prefix + '/media/hls/master.m3u8'

    # create temp output directory
    output_dir = os.path.join(efs_path, str(uuid.uuid4()))
    try:
        os.mkdir(output_dir)
    except FileExistsError as error:
        print('directory exist')
    output_filename = 'recording.mp4'
    output_name = os.path.join(output_dir, output_filename)

    # parse master.m3u8 to find out highest bandwidth stream and get the segment file names
    print('downloading ' + s3_ojbect_key)
    s3_client.download_file(bucket, s3_ojbect_key, output_dir + '/master.m3u8')
    playlist_obj = m3u8.load(output_dir + '/master.m3u8')
    if playlist_obj.is_variant: 
        playlist = playlist_obj.playlists[0]
        playlist_filename = os.path.join(output_dir, playlist.uri)
        # check if the playlist.uri contains directory
        if playlist.uri.find('/'): 
            dir = os.path.dirname(playlist_filename)
            if not os.path.exists(dir):
                os.makedirs(os.path.dirname(dir))
        s3_client.download_file(bucket, prefix + '/media/hls/'+playlist.uri, playlist_filename)
        target_playlist_obj = m3u8.load(playlist_filename)

    #download hls segments from S3 to oupput_dir
    download_segments(bucket, prefix+'/media/hls', target_playlist_obj.segments, output_dir)

    # convert hls to mp4 using ffmpeg
    print('Start to convert hls to mp4')
    cmd = ['ffmpeg', '-loglevel', 'error', '-i', output_dir+'/master.m3u8', '-c', 'copy', '-y', output_name]
    subprocess.check_output(cmd)

    # upload the output video to S3
    print('Start to upload mp4 to S3')
    s3_output_key = prefix + '/media/' + output_filename
    s3_client.upload_file(output_name, bucket, s3_output_key, ExtraArgs={'ContentType': 'video/mp4'})

    # delete the temp output directory
    shutil.rmtree(output_dir)

    print('Done')
    
    return {
        'bucket': bucket,
        'key': s3_output_key
    }

def download_object(bucket, object_key, target_file):
    session = boto3.session.Session()
    s3 = session.client('s3', os.environ['AWS_REGION'], config=Config(
        s3={'addressing_style': 'path'}))
    s3.download_file(bucket, object_key, target_file)


def download_segments(bucket_name, prefix, segments, local_dir=None):
    with concurrent.futures.ThreadPoolExecutor() as e:
        for segment in segments:
            e.submit(download_object, bucket_name, prefix+'/'+segment.uri, local_dir+'/'+segment.uri)
