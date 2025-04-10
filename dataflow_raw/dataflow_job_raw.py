import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from datetime import datetime

# Hardcoded values
PROJECT_ID = "practiceproject-454918"
REGION = "us-central1"
BUCKET_NAME = "practice_emp_bucket"
CSV_FILE_NAME = "emp_data.csv"
INPUT_FILE = f"gs://{BUCKET_NAME}/{CSV_FILE_NAME}"
OUTPUT_TABLE = f"{PROJECT_ID}:employee_data_raw.employee_details_raw"

# Define BigQuery Schema in String Format
SCHEMA = "first_name:STRING, last_name:STRING, joining_date:STRING, monthly_salary:STRING, employee_id:STRING, RawIngestionTime:TIMESTAMP"

# Function to parse CSV rows
def parse_csv(line):
    values = line.split(",")
    return {
        "first_name": values[0],
        "last_name": values[1],
        "joining_date": values[2],
        "monthly_salary": values[3],
        "employee_id": values[4],
        "RawIngestionTime": datetime.utcnow().isoformat(),
    }

# Apache Beam Pipeline
def run():
    pipeline_options = PipelineOptions(
        runner="DataflowRunner",  # Execute on GCP Dataflow
        project=PROJECT_ID,
        temp_location=f"gs://{BUCKET_NAME}/temp",
        region=REGION,
        job_name="dataflow-job-raw",
        save_main_session=True,
    )

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read CSV File" >> beam.io.ReadFromText(INPUT_FILE, skip_header_lines=1)
            | "Parse CSV Rows" >> beam.Map(lambda line: parse_csv(line))
            | "Write to BigQuery" >> WriteToBigQuery(
                OUTPUT_TABLE,
                schema=SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

if __name__ == "__main__":
    run()