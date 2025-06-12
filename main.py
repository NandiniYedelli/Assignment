import json
import os

# Imports required libraries
# Initializes Pub/Sub and Storage clients

from google.cloud import pubsub_v1
from google.cloud import storage
from cloudevents.http import CloudEvent
import functions_framework

# Initialize clients
publisher = pubsub_v1.PublisherClient()
storage_client = storage.Client()

@functions_framework.cloud_event
# Declares this as a CloudEvent-triggered function (i.e., triggered by a storage event)

def process_file_upload(cloud_event: CloudEvent) -> None:
    """
    Cloud Run function triggered by Cloud Storage events.
    Extracts file information and publishes to Pub/Sub.
    """
    
    try:
        # Get event data
        data = cloud_event.data
        
        # Extract file information
        # Gets the uploaded file’s bucket and name from the event
        # Uses the Storage API to get file size and metadata
        bucket_name = data.get('bucket')
        file_name = data.get('name')
        
        # Get file details from Cloud Storage
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        
        # Reload blob to get updated metadata
        blob.reload()
        
        # Extract file information
        file_size = blob.size
        file_format = file_name.split('.')[-1] if '.' in file_name else 'unknown'
        
        # Create message payload
        # Builds a dictionary with file name, size, format, bucket, and timestamp
      
        message_data = {
            'file_name': file_name,
            'file_size': file_size,
            'file_format': file_format,
            'bucket_name': bucket_name,
            'timestamp': str(cloud_event.get_time()) if cloud_event.get_time() else None
        }
        
        # Publish to Pub/Sub
        project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
        topic_path = publisher.topic_path(project_id, os.environ.get('my-assig-nan'))
        # Publishes the message to a Pub/Sub topic as a JSON string
        
        # Convert message to JSON string
        message_json = json.dumps(message_data)
        message_bytes = message_json.encode('utf-8')
        
        # Publish message
        future = publisher.publish(topic_path, message_bytes)
        message_id = future.result()
        
        print(f"File processed successfully:")
        # Logs file info and Pub/Sub message ID
        print(f"  - File Name: {file_name}")
        print(f"  - File Size: {file_size} bytes")
        print(f"  - File Format: {file_format}")
        print(f"  - Message ID: {message_id}")
        
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return f'Error: {str(e)}', 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return 'OK', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
