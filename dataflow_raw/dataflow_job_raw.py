import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import os

# Set Google Cloud credentials environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/adith/Downloads/key.json"

# Pipeline options
options = PipelineOptions(
    project='practiceproject-454918',  # Replace with your GCP project ID
    region='us-central1',  # Ensure consistency with your resources
    temp_location='gs://dataflow_job_tmp_bkt'  # Temp location for Dataflow staging
)

# Define the Apache Beam pipeline
with beam.Pipeline(options=options) as pipeline:
    # Read the CSV file from GCS
    rows = (
        pipeline
        | 'Read CSV' >> beam.io.ReadFromText('gs://practice_emp_bucket/emp_data.csv', skip_header_lines=1)
    )

    # Define the function to parse CSV rows
    def parse_csv(row):
        fields = row.split(',')
        return {
            'first_name': fields[0],
            'last_name': fields[1],
            'joining_date': fields[2],
            'monthly_salary': fields[3],
            'employee_id': fields[4]
        }

    # Apply the parsing function to the rows
    parsed_rows = rows | 'Parse CSV' >> beam.Map(parse_csv)

    # Write the parsed rows to BigQuery using an inline schema string
    parsed_rows | 'Write to BigQuery' >> WriteToBigQuery(
        table='practiceproject-454918:employee_data.employee_details',
        schema='first_name:STRING, last_name:STRING, joining_date:STRING, monthly_salary:STRING, employee_id:STRING',
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )