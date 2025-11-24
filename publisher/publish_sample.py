# Simple Pub/Sub publisher for testing
# Usage:
# python publish_sample.py --project PROJECT_ID --topic projects/PROJECT_ID/topics/user-events --num 10

import argparse
import json
import time
from google.cloud import pubsub_v1

def publish_messages(project, topic, num):
    publisher = pubsub_v1.PublisherClient()
    for i in range(num):
        message = {
            "user_id": f"user_{i%5}",
            "event_type": "click" if i%2==0 else "view",
            "event_time": time.strftime('%Y-%m-%dT%H:%M:%S'),
            "metadata": f"sample-{i}"
        }
        data = json.dumps(message).encode("utf-8")
        future = publisher.publish(topic, data)
        print(f"Published message id: {future.result()} payload: {message}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--topic", required=True, help="Full topic path: projects/PROJECT/topics/your-topic")
    parser.add_argument("--num", type=int, default=10)
    args = parser.parse_args()
    publish_messages(args.project, args.topic, args.num)
