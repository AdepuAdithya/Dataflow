import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from datetime import datetime
import logging
logging.basicConfig(level=logging.INFO)

# Hardcoded values
PROJECT_ID = "practiceproject-454918"
REGION = "us-central1"
BUCKET_NAME = "practice_emp_bucket"
INPUT_TABLE = f"{PROJECT_ID}:employee_data_raw.employee_details_raw"
OUTPUT_TABLE = f"{PROJECT_ID}:employee_data_staging.employee_details_staging"

# Define BigQuery Schema in required Format
SCHEMA = "first_name:STRING, last_name:STRING, joining_date:DATE, monthly_salary:INTEGER, employee_id:INTEGER, RawIngestionTime:TIMESTAMP, StagingIngestionTime:TIMESTAMP"

# Function to parse CSV rows
def Trasform_Raw_Data(record):
    try:
        return {
            "first_name": record["first_name"],
            "last_name": record["last_name"],
            "joining_date": datetime.strptime(record["joining_date"], "%Y-%m-%d").date().isoformat(),
            "monthly_salary": int(record["monthly_salary"]),
            "employee_id": int(record["employee_id"]),
            "RawIngestionTime": record["RawIngestionTime"],
            "StagingIngestionTime": datetime.utcnow().isoformat()
            }
    except Exception as e:
        logging.error(f"Error processing record: {record}, Error: {e}")
        return None

# Apache Beam Pipeline
def run():
    pipeline_options = PipelineOptions(
        runner="DataflowRunner",  # Execute on GCP Dataflow
        project=PROJECT_ID,
        temp_location=f"gs://{BUCKET_NAME}/temp",
        region=REGION,
        job_name="dataflow-job-staging",
        save_main_session=True
    )

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read from BigQuery" >> beam.io.ReadFromBigQuery(table=INPUT_TABLE)
            | "Trasform Raw Data" >> beam.Map(lambda record: Trasform_Raw_Data(record))
            | "Write to BigQuery" >> WriteToBigQuery(
                table=OUTPUT_TABLE,
                schema=SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )
            

if __name__ == "__main__":
    run()