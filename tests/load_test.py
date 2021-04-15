import boto3
import json


client = boto3.client('lambda', 'ap-southeast-1')
payload = {
    "bucket": "mediaconvert-ap-sourtheast-1",
    "prefix": "ivs/123456789012/AbCdef1G2hij/2020-06-23T20-12-32.152Z/a1B2345cdeFg/media/hls/chunked/",
    "playlist": "playlist.m3u8"
}
with open('../events/streamconverter.json', 'r') as event:
    for i in range(1, 300000):
        response = client.invoke(
            FunctionName='hls-mp4-converter-StreamConvertFunction-J3ORABJA3Y36',
            InvocationType='Event',
            LogType='None',
            Payload=bytes(json.dumps(payload), encoding='utf8'),
        )
