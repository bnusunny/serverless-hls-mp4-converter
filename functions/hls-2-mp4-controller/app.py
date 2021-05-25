import json
from urllib.parse import urlparse

def lambda_handler(event, context):
    master_playlists = event['playlists']
    
    playlist_data = []
    for playlist in master_playlists:
        _parsed = urlparse(playlist, allow_fragments=False)
        playlist_data.append({
            "bucket": _parsed.netloc,
            "key": _parsed.path.lstrip('/'),
            "channel_prefix": '/'.join(_parsed.path.lstrip('/').split('/')[:4])
        })
    
    return playlist_data
