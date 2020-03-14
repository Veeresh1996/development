
# system imports
import json
import os
import uuid

# lib imports
from smart_open import open, s3_iter_bucket

# local imports
from .base import Base
from .etl import ETL
from .log import Log
from .opts import Opts
from .path import Path
from .sqs import SQS

#
#  ETL main
#


class Main(Base):
    def __init__(self, log: Log, opts: Opts):
        super().__init__(log.new('main'), opts)
        self.opts = opts
        self.input_path = Path(opts.input_path)
        self.is_manifest = (opts.input_type == "MANIFEST")
        self.sqs = SQS(log, opts)

    # get paths from manifest file
    def get_paths_from_manifest_file(self, path: Path) -> list:
        manifest_line_start = self.opts.input_batch_start_index
        manifest_line_end = self.opts.input_batch_end_index
        count = 0
        paths = []
        with open(path.get(), 'r') as fin:
            for line in fin:
                line = line.strip()
                if line == "" or line.startswith("#"): # ignore emtpy line or commented line
                    continue
                if count >= manifest_line_start and count < manifest_line_end:
                    paths.append(line)
                count += 1
        return paths

    # get input paths list
    def get_input_paths(self):
        paths = []
        # if manifest enabled, get from manifest file (panasonic data platform framework for AWS BATCH)
        if self.is_manifest:
            return self.get_paths_from_manifest_file(self.input_path)
        # if manifest not enabled and the input path is direct s3 path (prefix/object)
        elif self.input_path.is_s3():  # TODO: returns content as well.
            s3key = self.input_path.get_s3_key()
            if s3key.endswith("/"):
                # TODO: use different method, this returns content as well.
                for key, _ in s3_iter_bucket(self.input_path.get_s3_bucket(), prefix=s3key):
                    paths.append("s3://{}/{}".format(self.input_path.get_s3_bucket(), key))
                return paths
            else:
                return [self.input_path.get()]
        # if input path is local --> testing purpose.
        elif self.input_path.is_local():
            if os.path.isdir(self.input_path.get_path()):
                for root, _, files in os.walk(self.input_path.get_path()):
                    for f in files:
                        paths.append(os.path.join(root, f))
                return paths
            else:
                return [self.input_path.get()]
        else:
            raise Exception("invalid input path")

    def run(self):
        for fpath in self.get_input_paths():
            self.log.info("Starting ETL on {}".format(fpath))
            resp = ETL(self.log, self.opts, fpath).run()
            self.log.info("Sending ETL STATUS to SQS")
            self.sqs.send_status(resp)
