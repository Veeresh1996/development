import sys
sys.path.append("../")
from unittest import TestCase,main,skip
from etl.etl import ETL
from resources.helper.helper import Helper
from etl.mysql import MySQL
import datetime,time
from decimal import Decimal
from smart_open import open
import warnings

class TestETL(TestCase):

    def expected_actual_return(self,tables,etl):
    
        for table in tables:
            warnings.simplefilter("ignore", ResourceWarning)
            output_path = etl.extract_rows_and_save(table)
            expected = self.helper.json_rows_return(table)
            if output_path!=None:
                with open(output_path,'r') as jsonfile:
                    json_rows = jsonfile.readlines()
                    actual = [jr.strip().replace("\n","") for jr in json_rows]
                    self.assertListEqual(expected,actual)


    def executer(self,input_file):
        self.helper = Helper()
        params = self.helper.set_params()
        opts = self.helper.setup_opts(params)
        log = self.helper.setup_log(opts)
        self.etl = ETL(log,opts,input_file)
        with open(input_file,'rb') as fin:
            self.etl.mysql.import_sql_using_pycmd(fin)
        tables = self.etl.mysql.get_tables() 
        sys_tables = [tb for tb in tables if tb.startswith("sys_")]
        cyc_tables = [tb for tb in tables if tb.startswith("cyc_")]
        lib_tables = [tb for tb in tables if tb.startswith("lib_")]
        log_tables = [tb for tb in tables if tb.startswith("log_")]
        self.expected_actual_return(sys_tables,self.etl)
        self.expected_actual_return(cyc_tables,self.etl)
        self.expected_actual_return(log_tables,self.etl)
        self.expected_actual_return(lib_tables,self.etl)
            
    def test_extract_rows_and_save_m25_4u(self):

        self.executer("../data/input/daily_m25_4u_2020-02-16_06h30m_Sunday.sql.gz")
    
    # @skip("SQL Syntax Error")
    def test_extract_rows_and_save_m25_ua(self):

        self.executer("../data/input/daily_m25_ua_2020-03-11_06h30m_Wednesday.sql.gz")


