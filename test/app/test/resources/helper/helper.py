import sys
sys.path.append("../")
from etl.main import Main
from etl.opts import Opts
from etl.path import Path
from etl.log import Log
import datetime
import time
from decimal import Decimal
from smart_open import open

TEST_TABLE_PATH = "resources/test_data/input_data/{table_name}.txt"
TEST_JSON_DATA = "resources/test_data/expected_data/{file_name}.json"

class Helper(object):

    def set_params(self, log_level: str = "WARNING", tmp_dir: str = "/tmp", workload_input_uri: str = "../data/manifest/manifest.txt",
                    workload_input_type: str = "MANIFEST", workload_output_prefix_s3uri: str = "../data/output/", 
                    output_subpath: str = "{tblname}/year={year}/month={month}/day={day}/airline={airline_code}", 
                    output_filename: str = "{prefix_code}_{loadtime}_{daystr}", output_format:str = "json.gz", 
                    aws_sqs_endpoint_url=None,workload_output_artifacts_prefix_s3uri: str = "", workload_state_prefix_s3uri: str = "",
                    start_mysql: str = "no", input_regex_fields: str = "_,prefix_code,airline_code,year,month,day,loadtime,daystr", 
                    input_regex: str = "(.*)daily_(.*)_(.*)_(.*)-(.*)-(.*)_(.*)_(.*).sql.gz", aws_batch_job_id: str = "", 
                    workload_temp_prefix_s3uri: str = "",aws_batch_job_attempt: int = 1, workload_control_queue_url: str = "", 
                    db_name="{prefix_code}_{airline_code}",aws_batch_job_array_index: int = 0,manifest_skip_factor: int = 10,
                    mysql_url="mysql://root@localhost/mysql?unix_socket=/var/run/mysqld/mysqld.sock",dryrun="no",
                    do_data_load="yes"):
        
        params = {
            'aws_batch_job_id': aws_batch_job_id,
            'aws_batch_job_array_index': aws_batch_job_array_index,
            'aws_batch_job_attempt': aws_batch_job_attempt,
            'log_level': log_level,
            'workload_input_uri': workload_input_uri,
            'workload_input_type': workload_input_type,
            'workload_output_prefix_s3uri': workload_output_prefix_s3uri,
            'workload_output_artifacts_prefix_s3uri': workload_output_artifacts_prefix_s3uri,
            'workload_state_prefix_s3uri': workload_state_prefix_s3uri,
            'workload_temp_prefix_s3uri': workload_temp_prefix_s3uri,
            'manifest_skip_factor': manifest_skip_factor,
            'workload_control_queue_url': workload_control_queue_url,
            'input_regex': input_regex,
            'input_regex_fields': input_regex_fields,
            'output_subpath': output_subpath,
            'output_filename': output_filename,
            'output_format': output_format,
            'db_name': db_name,
            'tmp_dir': tmp_dir,
            'mysql_url': mysql_url,
            'start_mysql': start_mysql,
            'dryrun': dryrun,
            'do_data_load': do_data_load,
            'aws_sqs_endpoint_url': aws_sqs_endpoint_url,
        }
        return params

    def setup_opts(self,params):

        opts = Opts(params)
        return opts
    
    def setup_path(self,opts):

        path = Path(opts.input_path)
        return path

    def setup_log(self,opts):

        log = Log(opts)
        return log


    def sql_rows_return(self,table_name):

        with open(TEST_TABLE_PATH.format(table_name=table_name), encoding='utf-8') as sql_rows_file:
            sql_rows = sql_rows_file.readlines()
            sql_desc = eval(sql_rows[0].strip().replace("\n", ""))
            sql_rows = [tuple(eval(row.strip().replace("\n", ""))) for row in sql_rows[1:]]
            sql_rows_count = len(sql_rows)
        return sql_desc, sql_rows, sql_rows_count


    def json_rows_return(self,file_name):

        with open(TEST_JSON_DATA.format(file_name=file_name), encoding='utf-8') as jsonfile:
            json_rows = jsonfile.readlines()
            json_rows = [jr.strip().replace("\n", "") for jr in json_rows]
        return json_rows


    def get_test_tables(self):

        test_table_list = ['cyc_category', 'cyc_cycle', 'cyc_group_content', 'cyc_preference', 'cyc_source', 'cyc_status_sheet',                                'lib_adcontainer_content',
                        'lib_adgroup_content', 'lib_adrunner_content', 'lib_image', 'lib_image_ref', 'lib_media', 'log_export', 
                        'log_exported_mids','sys_extension', 'sys_image_category_thumbnail', 'sys_image_spec', 'sys_media_config']

        return test_table_list
