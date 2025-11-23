# Apache Beam streaming pipeline: read from Pub/Sub and write to BigQuery
# Replace placeholders: PROJECT_ID, TEMP_LOCATION, STAGING_LOCATION, PUBSUB_TOPIC, BQ_TABLE
import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions

class ParseJsonDoFn(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element.decode('utf-8') if isinstance(element, bytes) else element)
            # Ensure event_time is a BigQuery-compatible timestamp (RFC3339)
            yield {
                'user_id': record.get('user_id'),
                'event_type': record.get('event_type'),
                'event_time': record.get('event_time'),
                'metadata': record.get('metadata', None)
            }
        except Exception as e:
            # In production you might log or send to dead-letter topic
            return

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True)
    parser.add_argument('--region', required=False, default='us-central1')
    parser.add_argument('--temp_location', required=True)
    parser.add_argument('--staging_location', required=True)
    parser.add_argument('--pubsub_topic', required=True, help='projects/PROJECT/topics/TOPIC')
    parser.add_argument('--bq_table', required=True, help='PROJECT:DATASET.TABLE')
    parser.add_argument('--runner', required=False, default='DirectRunner')
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = known_args.project
    google_cloud_options.region = known_args.region
    google_cloud_options.temp_location = known_args.temp_location
    google_cloud_options.staging_location = known_args.staging_location
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (p
         | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic=known_args.pubsub_topic)
         | 'DecodeBytes' >> beam.Map(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)
         | 'ParseJson' >> beam.ParDo(ParseJsonDoFn())
         | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                known_args.bq_table,
                schema='user_id:STRING,event_type:STRING,event_time:TIMESTAMP,metadata:STRING',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    run()
