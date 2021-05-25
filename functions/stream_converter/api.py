import json
import app

def lambda_handler(event, context):
    # print(json.dumps(event))

    return app.lambda_handler({
        "bucket": "mediaconvert-ap-sourtheast-1",
        "prefix": "ivs/123456789012/AbCdef1G2hij/2020-06-23T20-12-32.152Z/a1B2345cdeFg/media/hls/chunked/",
        "playlist": "playlist.m3u8"
    }, context)

    