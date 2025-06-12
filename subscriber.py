from google.cloud import pubsub_v1
import json
import os

# Set these to your project and subscription
project_id = "your-gcp-project-id"   # <-- Replace with your actual project ID
subscription_id = "my-sub"

# Create a subscriber client
subscriber = pubsub_v1.SubscriberClient()

# Fully-qualified subscription path
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    print("📩 Received message:")
    try:
        data = json.loads(message.data.decode('utf-8'))
        print(json.dumps(data, indent=4))
    except Exception as e:
        print("⚠️ Error decoding message:", str(e))
    message.ack()

print(f"🔁 Listening on subscription: {subscription_path}...\nPress Ctrl+C to stop.\n")

# Start streaming messages
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

# Keep the main thread alive
try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    print("🛑 Stopped by user.")
    streaming_pull_future.cancel()
