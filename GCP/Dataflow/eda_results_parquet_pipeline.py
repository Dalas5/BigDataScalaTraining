"""`eda_results_parquet_pipeline.py` is a Dataflow pipeline which reads a parquet files and writes its
contents to a BigQuery table.
"""

import argparse
import logging
import pandas as pd
import pyarrow
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


def run(argv=None):
    """The main function which creates the pipeline and runs it."""

    parser = argparse.ArgumentParser()

    # Here we add some specific command line arguments we expect.
    # Specifically we have the input file to read, the output table to write and it's BQ schema.
    # This is the final stage of the pipeline, where we define the destination
    # of the data. In this case we are writing to BigQuery.

    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input file to read. This can be a local file or '
        'a file in a Google Storage Bucket.',
        default='gs://dalas-amazon-reviews/eda-results/avg-product-rating/part-00002-65a944df-d8c2-4b39-873c-1f3a57a87684-c000.snappy.parquet')
        

    parser.add_argument('--output',
                        dest='output',
                        required=False,
                        help='Output BQ table to write results to.',
                        default='test_dataset.avg_product_rating')

    parser.add_argument('--schema',
                        dest='schema',
                        required=True,
                        help='Output BQ table schema string definition')


    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    
    # Initiate the pipeline using the pipeline arguments passed in from the
    # command line. This includes information such as the project ID and
    # where Dataflow should store temp files.
    p = beam.Pipeline(options=PipelineOptions(pipeline_args))    

    (p
     # Read the file. This is the source of the pipeline.
     # We use the input
     # argument from the command line.

     | 'Read from Parquet file' >> beam.io.ReadFromParquet(known_args.input) 
     | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(
             # The table name is a required argument for the BigQuery sink.
             # In this case we use the value passed in from the command line.
             known_args.output,
             # Here we use the simplest way of defining a schema:
             # fieldName:fieldType
             schema=known_args.schema,           
             # Creates the table in BigQuery if it does not yet exist.
             create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             # Deletes all data in the BigQuery table before writing.
             write_disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

