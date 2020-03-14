
class Opts(object):
    def __init__(self, args: dict):
        super().__init__()
        self.args = args

    def get(self, name, d=None):
        return self.args.get(name, d)

    @property
    def tmp_dir(self) -> str:
        return self.args['tmp_dir']

    @property
    def job_id(self) -> str:
        return self.args['aws_batch_job_id']

    @property
    def job_attempt(self) -> str:
        return self.args['aws_batch_job_attempt']

    @property
    def job_index(self) -> int:
        return self.args.get('aws_batch_job_array_index',  0)

    @property
    def sqs_queue_url(self) -> str:
        return self.args['workload_control_queue_url']

    @property
    def input_path(self) -> str:
        return self.args['workload_input_uri']

    @property
    def input_type(self) -> str:
        return self.args['workload_input_type']

    @property
    def input_batch_len(self) -> int:
        return self.args.get('manifest_skip_factor', 1)

    @property
    def input_batch_start_index(self) -> int:
        return self.job_index * self.input_batch_len

    @property
    def input_batch_end_index(self) -> int:
        return self.input_batch_start_index + self.input_batch_len

    @property
    def input_regex(self) -> str:
        return self.args['input_regex']

    @property
    def input_regex_fields(self) -> list:
        return self.args['input_regex_fields'].split(",")

    @property
    def output_basepath(self) -> str:
        return self.args['workload_output_prefix_s3uri']

    @property
    def output_subpath(self) -> str:
        return self.args['output_subpath']

    @property
    def output_filename(self) -> str:
        return self.args['output_filename']

    @property
    def output_format(self) -> str:
        return self.args['output_format']

    @property
    def log_level(self) -> str:
        return self.args['log_level']

    @property
    def sqs_endpoint_url(self) -> str:
        return self.args['aws_sqs_endpoint_url']

    @property
    def start_mysql(self) -> str:
        return self.args['start_mysql']
