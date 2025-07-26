import os
import json
import boto3
from datetime import datetime
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

dynamodb = boto3.resource('dynamodb')

def handler(event, context):
    """Generate RSS feed compatible with qBittorrent"""
    
    # Get configuration from environment variables
    table_name = os.environ['TABLE_NAME']
    feed_title = os.environ.get('FEED_TITLE', 'YTS 1080p Movies Feed')
    feed_description = os.environ.get('FEED_DESCRIPTION', 'Latest 1080p movies from YTS for qBittorrent')
    
    # Construct the feed URL
    api_id = os.environ.get('API_GATEWAY_REST_API_ID')
    region = os.environ.get('API_GATEWAY_REGION')
    stage = os.environ.get('API_GATEWAY_STAGE', 'prod')
    
    if api_id and region:
        feed_link = f"https://{api_id}.execute-api.{region}.amazonaws.com/{stage}/rss"
    else:
        host = event.get('headers', {}).get('Host', 'example.com')
        feed_link = f"https://{host}{event.get('path', '/rss')}"
    
    try:
        # Get items from DynamoDB
        table = dynamodb.Table(table_name)
        
        # Scan for all items (you might want to add pagination for large datasets)
        response = table.scan()
        items = response.get('Items', [])
        
        # Sort by timestamp descending (newest first)
        items.sort(key=lambda x: x.get('timestamp', 0), reverse=True)
        
        # Create RSS feed with torrent namespace
        rss = Element('rss', version='2.0')
        rss.set('xmlns:torrent', 'http://xmlns.ezrss.it/0.1/')
        
        channel = SubElement(rss, 'channel')
        
        # Add channel metadata
        SubElement(channel, 'title').text = feed_title
        SubElement(channel, 'description').text = feed_description
        SubElement(channel, 'link').text = feed_link
        SubElement(channel, 'language').text = 'en-US'
        SubElement(channel, 'lastBuildDate').text = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        SubElement(channel, 'ttl').text = '30'  # Cache for 30 minutes
        
        # Add items to feed (limit to recent items)
        for item in items[:50]:  # Limit to 50 most recent items
            rss_item = SubElement(channel, 'item')
            
            # Title with quality and size info for qBittorrent filtering
            SubElement(rss_item, 'title').text = item.get('title', 'Untitled')
            
            # Description with CDATA for HTML content
            description = item.get('description', '')
            SubElement(rss_item, 'description').text = description
            
            # Direct torrent URL as link
            SubElement(rss_item, 'link').text = item.get('link', '')
            
            # Use torrent hash as GUID if available
            guid = item.get('guid', item.get('id', ''))
            SubElement(rss_item, 'guid', isPermaLink='false').text = guid
            
            # Category for filtering
            SubElement(rss_item, 'category').text = item.get('category', 'Movies/1080p')
            
            # Publication date
            timestamp = item.get('timestamp', 0)
            if timestamp:
                pub_date = datetime.fromtimestamp(int(timestamp))
                SubElement(rss_item, 'pubDate').text = pub_date.strftime('%a, %d %b %Y %H:%M:%S GMT')
            
            # Add torrent-specific elements for qBittorrent
            if item.get('seeds'):
                torrent_seeds = SubElement(rss_item, '{http://xmlns.ezrss.it/0.1/}seeds')
                torrent_seeds.text = str(item.get('seeds', 0))
            
            if item.get('peers'):
                torrent_peers = SubElement(rss_item, '{http://xmlns.ezrss.it/0.1/}peers')
                torrent_peers.text = str(item.get('peers', 0))
            
            if item.get('size'):
                torrent_size = SubElement(rss_item, '{http://xmlns.ezrss.it/0.1/}contentLength')
                torrent_size.text = item.get('size', '')
            
            if item.get('guid'):
                torrent_hash = SubElement(rss_item, '{http://xmlns.ezrss.it/0.1/}infoHash')
                torrent_hash.text = item.get('guid', '')
        
        # Convert to pretty XML string
        rough_string = tostring(rss, encoding='unicode')
        reparsed = minidom.parseString(rough_string)
        xml_string = reparsed.toprettyxml(indent="  ")
        
        # Clean up extra blank lines
        xml_lines = [line for line in xml_string.split('\n') if line.strip()]
        xml_string = '\n'.join(xml_lines)
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/rss+xml; charset=utf-8',
                'Cache-Control': 'max-age=1800',  # Cache for 30 minutes
                'Access-Control-Allow-Origin': '*'  # Allow qBittorrent to access
            },
            'body': xml_string
        }
        
    except Exception as e:
        print(f"Error generating RSS feed: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'error': 'Failed to generate RSS feed',
                'message': str(e)
            })
        }