import sys
sys.path.append("../")
from unittest import TestCase,main
from unittest.mock import patch
from etl.etl import ETL
from resources.helper.helper import Helper
from etl.mysql import MySQL
import datetime,time
from decimal import Decimal
from etl.main import Main
from smart_open import open
import warnings

class TestETL(TestCase):
    
    def setUp(self):

        self.helper = Helper()
        self.tables = self.helper.get_test_tables()
        self.sys_tables = [tb for tb in self.tables if tb.startswith("sys_")]
        self.cyc_tables = [tb for tb in self.tables if tb.startswith("cyc_")]
        self.lib_tables = [tb for tb in self.tables if tb.startswith("lib_")]
        self.log_tables = [tb for tb in self.tables if tb.startswith("log_")]
    
    def expected_actual_return(self,tables):

        for table in tables:
            description,rows,rows_count = self.helper.sql_rows_return(table)
            with patch("etl.mysql.MySQL.__init__",return_value =None):
                    with patch("etl.mysql.MySQL.get_all_rows",return_value = [description,rows]):
                        with patch("etl.mysql.MySQL.get_rows_count",return_value = rows_count):
                                params = self.helper.set_params()
                                opts = self.helper.setup_opts(params)
                                log = self.helper.setup_log(opts)
                                input_file = Main(log,opts).get_input_paths()
                                warnings.simplefilter("ignore", ResourceWarning)
                                output_path = ETL(log,opts,input_file[0]).extract_rows_and_save(table)
                                expected = self.helper.json_rows_return(table)
                                if output_path!=None:
                                    with open(output_path,'r') as jsonfile:
                                        json_rows = jsonfile.readlines()
                                        actual = [jr.strip().replace("\n","") for jr in json_rows]
                                        self.assertListEqual(expected,actual)

    def test_extract_rows_and_save_sys(self):
        
        self.expected_actual_return(self.sys_tables)
        
       
    def test_extract_rows_and_save_cyc(self):
    
        self.expected_actual_return(self.cyc_tables)
        
        
    def test_extract_rows_and_save_lib(self):
        
        self.expected_actual_return(self.lib_tables)
       

    def test_extract_rows_and_save_log(self):
             
        self.expected_actual_return(self.log_tables)

