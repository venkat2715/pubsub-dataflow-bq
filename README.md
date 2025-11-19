# Real-time Pipeline: Pub/Sub → Dataflow → BigQuery

This repository contains a complete, ready-to-use example for a real-time streaming pipeline:
- Publisher script to send JSON events to Pub/Sub
- Dataflow (Apache Beam) streaming pipeline that reads from Pub/Sub and writes to BigQuery
- BigQuery schema and deployment instructions

## Contents
- `dataflow_pipeline/` - Dataflow pipeline code (Python)
- `publisher/` - simple publisher script to send test messages to Pub/Sub
- `bq_schema/` - BigQuery schema JSON
- `requirements.txt` - Python dependencies
- `README.md` - this file

## Quickstart (high-level)
1. Create a GCP project and enable APIs:
   - Dataflow API, Pub/Sub API, BigQuery API, IAM
2. Create Pub/Sub topic:
   ```
   gcloud pubsub topics create projects/PROJECT_ID/topics/user-events
   ```
3. Create BigQuery dataset & table (or let pipeline create table using schema):
   ```
   bq mk dataset_name
   bq mk --table dataset_name.user_events bq_schema/user_events_schema.json
   ```
4. Update placeholders in `dataflow_pipeline/run_pipeline.sh` and `dataflow_pipeline/pipeline.py` with your `PROJECT_ID`, `TEMP_LOCATION`, `STAGING_LOCATION`, `PUBSUB_TOPIC`, and `BQ_TABLE`.
5. Install dependencies and run locally (DirectRunner) or deploy to Dataflow (DataflowRunner). See `dataflow_pipeline/README.md` for commands.
6. Run the publisher to send sample events:
   ```
   python publisher/publish_sample.py --project PROJECT_ID --topic projects/PROJECT_ID/topics/user-events
   ```

## What to customize before deploying
- Replace `PROJECT_ID`, `GCP_REGION`, `TEMP_LOCATION` and `STAGING_LOCATION`.
- Ensure service account used by Dataflow has `roles/bigquery.dataEditor`, `roles/pubsub.subscriber` and `roles/storage.objectCreator`.

## License
MIT
