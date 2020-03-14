
import json
import os
import re
import uuid
import traceback

from smart_open import open

from .base import Base, Status
from .json import CustomJSONEncoder
from .log import Log
from .mysql import MySQL
from .opts import Opts
from .path import Path

#
# ETL process
#


class ETL(Base):
    def __init__(self, log: Log, opts: Opts, input_path: str):
        super().__init__(log.new('etl'), opts)
        self.input_path = Path(input_path)
        self.input_regex = opts.input_regex
        self.input_regex_fields = opts.input_regex_fields
        self.output_basepath = Path(opts.output_basepath)
        self.output_subpath = opts.output_subpath
        self.output_filename = opts.output_filename
        self.output_format = opts.output_format
        self.do_data_load = opts.get('do_data_load', 'yes') == "yes"
        self.fields = self.extract_fields()
        self.sql_import_method = opts.get('sql_import_method', 'PYCMD')
        self.dbname = opts.get('db_name').format(**self.fields)
        self.output_paths = []
        self.mysql = MySQL(log, opts, self.dbname)

    def extract_fields(self):
        fields = {}
        matches = re.finditer(
            self.input_regex, self.input_path.get(), re.MULTILINE)
        count = 0
        for matchNum, match in enumerate(matches, start=1):
            count += 1
            gl = len(match.groups())
            if len(self.input_regex_fields) == gl:
                for gn in range(0, gl):
                    gf = self.input_regex_fields[gn]
                    if gf != "_":
                        fields[gf] = match.group(gn+1)
            else:
                count = 0
        if count == 0:
            raise Exception("Invalid key pattern. key={}, pattern={}".format(self.input_path.get(), self.input_regex))
        return fields

    def get_output_file(self, **kwargs):
        uid = str(uuid.uuid4())
        params = {**self.fields, **{'uuid': uid}, **kwargs}
        # output dir/prefix
        output_dir = Path(self.output_basepath.get_with_suffix(self.output_subpath.format(**params)))
        output_dir.local_mkdirs()  # create local dirs if the path is local.
        # output file name
        output_filename = self.output_filename
        if output_filename == "":
            output_filename = uid
        output_filename = output_filename.format(**params) + "." + self.output_format
        # output file - full path.
        return output_dir.get_with_suffix(output_filename)

    def extract_rows_and_save(self, tblname):
        rcount = self.mysql.get_rows_count(tblname)
        if rcount <= 0:
            self.log.info("  No rows found!")
            return
        output_file = self.get_output_file(tblname=tblname)
        count = 0
        meta, rows = self.mysql.get_all_rows(tblname)
        with open(output_file, 'w') as fout:
            for row in rows:
                count += 1
                record = {}
                for i, value in enumerate(row):
                    record[meta[i][0]] = value
                fout.write(json.dumps(record, cls=CustomJSONEncoder))
                fout.write("\n")
        self.log.info("  Rows={}".format(count))
        self.output_paths.append(output_file)
        self.log.info("  OutputFile={}".format(output_file))
        return output_file

    def run(self) -> dict:
        input_path = self.input_path.get()
        resp = {'input_path': input_path}
        try:
            if self.do_data_load:
                self.log.info("Importing sql...")
                if self.sql_import_method == "FULL_CONTENT":
                    with open(input_path, 'r') as fin:
                        self.mysql.import_sql_content(fin.read())
                elif self.sql_import_method == "PYCMD":
                    with open(input_path, 'rb') as fin:
                        self.mysql.import_sql_using_pycmd(fin)
                elif self.sql_import_method == "SYSCMD":
                    sqlfile = self.download_file(input_path, "sql")
                    self.mysql.import_sql_using_system(sqlfile)
                else: # STREAMING / DEFAULT
                    with open(input_path, 'r') as fin:
                        self.mysql.import_sql(fin)
                self.log.info("Imported...")
            self.log.info("Extract from DB and save...")
            for tblname in self.mysql.get_tables():
                self.extract_rows_and_save(tblname)
            resp['status'] = Status.SUCCEEDED
            resp['output_paths'] = self.output_paths
        except Exception as err:
            resp['status'] = Status.FAILED
            resp['message'] = str(err)
            self.log.error(err)
            traceback.print_exc()
        finally:
            self.mysql.close(True)
        return resp
