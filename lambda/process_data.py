import os
import json
import boto3
import urllib3
import uuid
from datetime import datetime
from decimal import Decimal
from urllib.parse import quote
import re

# Initialize AWS and HTTP clients
dynamodb = boto3.resource('dynamodb')
http = urllib3.PoolManager()

def extract_imdb_id(url_or_id):
    """Extract IMDB ID from a URL or return the ID if already in the correct format."""
    print(f"-> extract_imdb_id: Received input '{url_or_id}'")
    imdb_pattern = r'tt\d{7,10}'

    # Check if the input is already a valid IMDB ID
    if re.match(f'^{imdb_pattern}$', url_or_id):
        print(f"   Input is already a valid IMDB ID: {url_or_id}")
        return url_or_id

    # If not, search for the pattern within the input (assuming it's a URL)
    match = re.search(imdb_pattern, url_or_id)
    if match:
        extracted_id = match.group()
        print(f"   Extracted IMDB ID from URL: {extracted_id}")
        return extracted_id

    print("   Could not find a valid IMDB ID in the input.")
    return None

def search_yts_by_imdb(imdb_id):
    """Search the YTS API by IMDB ID."""
    url = f"https://yts.mx/api/v2/list_movies.json?query_term={imdb_id}&limit=1"
    print(f"-> search_yts_by_imdb: Querying YTS API with URL: {url}")

    try:
        response = http.request('GET', url)
        data = json.loads(response.data.decode('utf-8'))
        print(f"   YTS API Response Status: {data.get('status')}")
        # For deep debugging, you can uncomment the next line to see the full response
        # print(f"   YTS API Full Response: {json.dumps(data, indent=2)}")

        if data.get('status') == 'ok' and data.get('data', {}).get('movie_count', 0) > 0:
            movies = data['data']['movies']
            # Ensure the found movie's IMDB ID matches the one we searched for
            for movie in movies:
                if movie.get('imdb_code') == imdb_id:
                    print(f"   Found matching movie: '{movie.get('title_long')}'")
                    return [movie]
            print("   Movie found, but IMDB ID did not match.")
            return []
        print("   Movie not found in YTS database.")
        return []
    except Exception as e:
        print(f"   Error searching YTS: {str(e)}")
        return []

def handler(event, context):
    """Main Lambda handler to find a movie on YTS and add it to a DynamoDB table."""
    print(f"## STARTING EXECUTION ##")
    print(f"Received event: {json.dumps(event, indent=2)}")
    
    table_name = os.environ.get('TABLE_NAME')
    if not table_name:
        print("Error: TABLE_NAME environment variable is not set.")
        return {'statusCode': 500, 'body': json.dumps({'error': 'Server configuration error'})}
    
    table = dynamodb.Table(table_name)
    print(f"Initialized DynamoDB table: {table_name}")

    try:
        # Parse the incoming request body
        body = json.loads(event['body']) if isinstance(event.get('body'), str) else event
        print(f"Parsed request body: {json.dumps(body, indent=2)}")
        
        # Get IMDB input from common fields
        imdb_input = body.get('imdb', body.get('url', body.get('query', '')))
        print(f"IMDB input from body: '{imdb_input}'")
        
        if not imdb_input:
            print("Error: No IMDB input provided.")
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Missing IMDB URL or ID.'})
            }
        
        # Extract the IMDB ID
        imdb_id = extract_imdb_id(imdb_input)
        
        if not imdb_id:
            print(f"Error: Invalid IMDB format for input '{imdb_input}'.")
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Invalid IMDB format', 'provided': imdb_input})
            }
        
        # Search YTS by the clean IMDB ID
        movies = search_yts_by_imdb(imdb_id)
        
        if not movies:
            return {
                'statusCode': 404,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'message': 'Movie not found on YTS', 'imdb_id': imdb_id})
            }
        
        # Process the found movie
        movie = movies[0]
        torrents = movie.get('torrents', [])
        torrents_1080p = [t for t in torrents if t.get('quality') == '1080p']
        print(f"Found {len(torrents_1080p)} torrent(s) with 1080p quality.")
        
        if not torrents_1080p:
            return {
                'statusCode': 404,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'message': 'No 1080p version available', 'movie': movie.get('title')})
            }
            
        # Select the best torrent (highest seeds)
        best_torrent = max(torrents_1080p, key=lambda t: t.get('seeds', 0))
        print(f"Selected best torrent with {best_torrent.get('seeds')} seeds.")
        
        # Prepare the item for DynamoDB
        item_id = f"{imdb_id}-1080p-{uuid.uuid4().hex[:8]}"
        rss_title = f"{movie.get('title')} ({movie.get('year')}) [1080p] [{best_torrent.get('size')}]"
        
        description = f"""<![CDATA[
        <p><strong>{movie.get('title')} ({movie.get('year')})</strong></p>
        <p>IMDB: {imdb_id} | Rating: {movie.get('rating')}/10 | Runtime: {movie.get('runtime')} min</p>
        <p>Quality: 1080p | Size: {best_torrent.get('size')}</p>
        <p>Seeds: {best_torrent.get('seeds')} | Peers: {best_torrent.get('peers')}</p>
        <p>{movie.get('summary', 'No summary available.')}</p>
        <img src="{movie.get('medium_cover_image', '')}" alt="Poster">
        ]]>"""
        
        item = {
            'id': item_id,
            'timestamp': int(datetime.now().timestamp()),
            'title': rss_title,
            'description': description.strip(),
            'link': best_torrent.get('url'),
            'guid': best_torrent.get('hash'),
            'category': 'Movies/1080p',
            'size': best_torrent.get('size'),
            'seeds': best_torrent.get('seeds', 0),
            'peers': best_torrent.get('peers', 0),
            'movie_id': movie.get('id'),
            'imdb_code': imdb_id,
            'year': movie.get('year'),
            'rating': Decimal(str(movie.get('rating', 0))),
            'added_date': datetime.now().isoformat()
        }
        
        print(f"Preparing to write the following item to DynamoDB:\n{json.dumps(item, indent=2, default=str)}")
        
        # Store in DynamoDB
        table.put_item(Item=item)
        print("Successfully added item to DynamoDB.")
        
        # Return success response
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'message': 'Movie added successfully to RSS feed', 'item_id': item_id})
        }
        
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'Failed to process request', 'message': str(e)})
        }