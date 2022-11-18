"""`eda_results_csv_pipeline.py` is a Dataflow pipeline which reads a csv files and writes its
contents to a BigQuery table.
"""

import json
import argparse
import logging
import re
import pandas as pd
import pyarrow
import apache_beam as beam
import time
from datetime import datetime
from google.cloud.storage import Client
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import DataflowRunner, DirectRunner


def parse_json_schema(schema_uri):

    path_match = re.match(r"gs://(.*?)/(.*)", schema_uri)
    if path_match:
        bucket, schema_path = path_match.groups()

    client = Client()
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(schema_path)
    download_file = blob.download_as_text(encoding="utf-8")
    return download_file


def parse_method(row_string, keys, types):
    values = re.split(",", re.sub('\r\n', '', re.sub('"', '', row_string)))

    i = 0
    for value in values:
        if types[i] == 'DATE':
            values[i] = datetime.strptime(value, '%Y-%m-%d').date()

        i += 1

    row = dict(zip(keys, values))
    return row


def run():

    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from csv into BigQuery')
    parser.add_argument('--project', required=True,
                        help='Specify Google Cloud project')
    parser.add_argument('--region', required=True,
                        help='Specify Google Cloud region')
    parser.add_argument('--runner', required=True,
                        help='Specify Apache Beam Runner')
    parser.add_argument('--input', required=True, help='Path to csv data file')
    parser.add_argument('--output', required=True, help='Bigquery table name')
    parser.add_argument('--schema', required=True,
                        help='BigQuery table schema file in json format path')

    opts, pipeline_opts = parser.parse_known_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions(pipeline_opts)
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format(
        'my-pipeline-', time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner

    input_path = opts.input
    table_name = opts.output

    # Table schema for BigQuery
    download_file = parse_json_schema(opts.schema)
    json_schema = json.loads(download_file)
    schema = {"fields": json_schema}

    # Column type list
    schema_type_list = [field['type'] for field in json_schema]
    schema_name_list = [field['name'] for field in json_schema]

    # Create the pipeline
    p = beam.Pipeline(options=options)

    (p
     | 'Read_from_GCS' >> beam.io.ReadFromText(input_path, skip_header_lines=1)
     | 'String To BigQuery Row' >> beam.Map(parse_method, keys=schema_name_list, types=schema_type_list)
     | 'Write_to_BigQuery' >> beam.io.WriteToBigQuery(
         table_name,
         schema=schema,
         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
         write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
