
import argparse
import os
import sys
from os import environ as env

from etl.main import Main
from etl.opts import Opts
from etl.log import Log

# define argument parser
parser = argparse.ArgumentParser()

# AWS BATCH arguments
parser.add_argument(
    '--aws_batch_job_id',
    default=env.get("AWS_BATCH_JOB_ID", ""),
    help="AWS Batch Job ID",
)
parser.add_argument(
    '--aws_batch_job_array_index',
    default=env.get("AWS_BATCH_JOB_ARRAY_INDEX", "0"),
    type=int
)
parser.add_argument(
    '--aws_batch_job_attempt',
    default=env.get("AWS_BATCH_JOB_ATTEMPT", "1"),
    type=int
)

# PANASONIC DATAPLATFORM arguments
parser.add_argument(
    '--log_level',
    default=env.get("LOG_LEVEL", "WARNING"),
)
parser.add_argument(
    '--workload_input_uri',
    default=env.get("WORKLOAD_INPUT_URI", ""),
)
parser.add_argument(
    '--workload_input_type',
    default=env.get("WORKLOAD_INPUT_TYPE", "MANIFEST"), # MANIFEST,DEFAULT
)
# used for storing structured data.
parser.add_argument(
    '--workload_output_prefix_s3uri',
    default=env.get("WORKLOAD_OUTPUT_PREFIX_S3URI", ""),
)
# used for storing non-data artifacts - images, videos etc.
parser.add_argument(
    '--workload_output_artifacts_prefix_s3uri',
    default=env.get("WORKLOAD_OUTPUT_ARTIFACTS_PREFIX_S3URI", ""),
)
# can be used to storing job state.
parser.add_argument(
    '--workload_state_prefix_s3uri',
    default=env.get("WORKLOAD_STATE_PREFIX_S3URI", ""),
)
parser.add_argument(
    '--workload_temp_prefix_s3uri',
    default=env.get("WORKLOAD_TEMP_PREFIX_S3URI", ""),
)
parser.add_argument(
    '--manifest_skip_factor',
    default=env.get("MANIFEST_SKIP_FACTOR", "10"),
    type=int
)
parser.add_argument(
    '--workload_control_queue_url',
    default=env.get("WORKLOAD_CONTROL_QUEUE_URL", ""),
)

# Application arguments
parser.add_argument(
    '--input_regex',
    default=env.get("INPUT_REGEX", "(.*)daily_(.*)_(.*)_(.*)-(.*)-(.*)_(.*)_(.*).sql.gz"),
)
parser.add_argument(
    '--input_regex_fields',
    default=env.get("INPUT_REGEX_FIELDS", "_,prefix_code,airline_code,year,month,day,loadtime,daystr"),
)
parser.add_argument(
    '--output_subpath',
    default=env.get("OUTPUT_SUBPATH", "{tblname}/dt={year}{month}{day}/_airline={airline_code}")
)
parser.add_argument(
    '--output_filename',
    default=env.get("OUTPUT_FILENAME", "{prefix_code}_{loadtime}_{daystr}")
)
parser.add_argument(
    '--output_format',
    default=env.get("OUTPUT_FORMAT", "json.gz")
)
parser.add_argument(
    '--db_name',
    default=env.get("DB_NAME", "{prefix_code}_{airline_code}")
)
parser.add_argument(
    '--sql_import_method',
    default=env.get("SQL_IMPORT_METHOD", "SYSCMD") # STREAMING,FULL_CONTENT,PYCMD,SYSCMD
)
parser.add_argument(
    '--tmp_dir',
    default=env.get("TMP_DIR", "/tmp")
)
parser.add_argument(
    '--mysql_url',
    default=env.get("MYSQL_URL", "mysql://root@localhost/mysql?unix_socket=/var/run/mysqld/mysqld.sock")
)

# LOCAL Testing arguments
parser.add_argument(
    '--start_mysql',
    default=env.get("START_MYSQL", "yes")
)
parser.add_argument(
    '--dryrun',
    default=env.get("DRYRUN", "no")
)
parser.add_argument(
    '--do_data_load',
    default=env.get("DO_DATA_LOAD", "yes")
)
parser.add_argument(
    '--aws_sqs_endpoint_url',
    default=env.get("AWS_SQS_ENDPOINT_URL", None),
)

# parse the arguments
args_parsed, _ = parser.parse_known_args(sys.argv[1:])
args = vars(args_parsed)

# check for mandatory arguments
if 'workload_input_uri' not in args or args['workload_input_uri'] == "":
    raise Exception("missing mandatory arg workload_input_uri")
if 'workload_output_prefix_s3uri' not in args or args['workload_output_prefix_s3uri'] == "":
    raise Exception("missing mandatory arg workload_output_prefix_s3uri")

# ETL
opts = Opts(args)
main = Main(Log(opts), opts)
main.run()
