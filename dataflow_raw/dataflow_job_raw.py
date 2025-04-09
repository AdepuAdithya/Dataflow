import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class CustomPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument("--input_file", type=str, help="GCS path to the input CSV file")