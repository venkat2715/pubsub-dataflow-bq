#!/bin/bash
# Example deploy script to run pipeline on Dataflow Runner
# Edit the variables below before running.

PROJECT_ID="YOUR_PROJECT_ID"
REGION="us-central1"
TEMP_LOCATION="gs://YOUR_BUCKET/temp"
STAGING_LOCATION="gs://YOUR_BUCKET/staging"
PUBSUB_TOPIC="projects/$PROJECT_ID/topics/user-events"
BQ_TABLE="$PROJECT_ID:your_dataset.user_events"

python pipeline.py \
  --project $PROJECT_ID \
  --region $REGION \
  --temp_location $TEMP_LOCATION \
  --staging_location $STAGING_LOCATION \
  --pubsub_topic $PUBSUB_TOPIC \
  --bq_table $BQ_TABLE \
  --runner DataflowRunner \
  --experiments=use_runner_v2
