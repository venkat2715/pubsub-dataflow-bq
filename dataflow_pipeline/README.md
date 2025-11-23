# Dataflow Pipeline (Python)

## Run locally (DirectRunner)
1. Create virtualenv and install:
   ```
   python -m venv venv
   source venv/bin/activate
   pip install -r ../requirements.txt
   ```
2. Run:
   ```
   python pipeline.py --project YOUR_PROJECT_ID --temp_location gs://YOUR_BUCKET/temp \
     --staging_location gs://YOUR_BUCKET/staging \
     --pubsub_topic projects/YOUR_PROJECT_ID/topics/user-events \
     --bq_table YOUR_PROJECT:dataset.user_events --runner DirectRunner
   ```

## Deploy to Dataflow (DataflowRunner)
Update `run_pipeline.sh` with your project info, then:
```
cd dataflow_pipeline
bash run_pipeline.sh
```

Make sure the GCS bucket exists and Dataflow service account has permissions.
