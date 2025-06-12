import json
import os
from flask import Flask, request
from google.cloud import pubsub_v1, storage
from cloudevents.http import from_http

app = Flask(__name__)
publisher = pubsub_v1.PublisherClient()
storage_client = storage.Client()

@app.route('/', methods=['POST'])
def process_file_upload():
    try:
        # Parse the incoming CloudEvent
        event = from_http(request.headers, request.get_data())
        data = event.data

        bucket_name = data.get('bucket')
        file_name = data.get('name')

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.reload()

        file_size = blob.size
        file_format = file_name.split('.')[-1] if '.' in file_name else 'unknown'

        message_data = {
            'file_name': file_name,
            'file_size': file_size,
            'file_format': file_format,
            'bucket_name': bucket_name,
            'timestamp': str(event.get_time()) if event.get_time() else None
        }

        project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
        topic_id = 'my-assig-nan'  # hardcoded topic name
        topic_path = publisher.topic_path(project_id, topic_id)

        message_json = json.dumps(message_data)
        future = publisher.publish(topic_path, message_json.encode('utf-8'))
        message_id = future.result()

        print("✅ File processed successfully:")
        print(f"  - File Name: {file_name}")
        print(f"  - File Size: {file_size}")
        print(f"  - Format: {file_format}")
        print(f"  - Message ID: {message_id}")

        return 'OK', 200

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return f'Error: {str(e)}', 500

@app.route('/health', methods=['GET'])
def health():
    return 'OK', 200

if __name__ == '__main__':
    app.run(port=8080, host='0.0.0.0')
