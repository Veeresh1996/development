
# system imports
import os
import uuid
from enum import Enum

# lib imports
from smart_open import open

# local imports
from .log import Log
from .opts import Opts


class Status(Enum):
    SUCCEEDED = "SUCCEEDED"
    INPUT_INCOMPLETE = "INPUT_INCOMPLETE"
    INPUT_UNSUPPORTED = "INPUT_UNSUPPORTED"
    INPUT_DUPLICATE = "INPUT_DUPLICATE"
    FAILED = "FAILED"

#
# ETL Base.
#


class Base(object):
    def __init__(self, log: Log, opts: Opts):
        super().__init__()
        self.log = log
        self.tmp_dir = opts.tmp_dir

    # downloads the file to tmp_dir/{uuid}.{format}
    def download_file(self, path: str, extn: str):
        downloadfile = os.path.join(self.tmp_dir, "{}.{}".format(str(uuid.uuid4()), extn))
        with open(path, 'rb') as fin:
            with open(downloadfile, "wb") as fout:
                for data in fin:
                    fout.write(data)
        return downloadfile
