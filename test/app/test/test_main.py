import sys
sys.path.append("../")
from unittest import TestCase,main,skipIf,expectedFailure
from unittest.mock import MagicMock,patch
from resources.helper.helper import Helper
from etl.main import Main
from smart_open import  s3_iter_bucket

class TestMain(TestCase):

    def setUp(self):

        self.helper = Helper()
    
    def test_get_input_files_manifest(self):

        params = self.helper.set_params(workload_input_type="MANIFEST")
        opts = self.helper.setup_opts(params)
        log = self.helper.setup_log(opts)

        actual = Main(log,opts).get_input_paths()
        expected = ["../data/input/daily_m25_4u_2019-12-14_05h30m_Saturday.sql.gz"]
        
        self.assertListEqual(actual,expected)
        
    def test_get_input_files_local(self):

        params = self.helper.set_params(workload_input_type="",workload_input_uri="../data/input")
        opts = self.helper.setup_opts(params)
        log = self.helper.setup_log(opts)

        actual = Main(log,opts).get_input_paths()
        actual.sort()
        expected = ["../data/input\daily_m25_4u_2019-12-14_05h30m_Saturday.sql.gz",
                    "../data/input\daily_m25_4u_2020-02-16_06h30m_Sunday.sql.gz",
                    "../data/input\daily_m25_4u_2020-02-17_06h30m_Monday.sql.gz",
                    "../data/input\daily_m25_sn_2020-03-09_06h30m_Monday.sql.gz",
                    "../data/input\daily_m25_ua_2020-03-11_06h30m_Wednesday.sql.gz"]
        expected.sort()
        
        self.assertListEqual(actual,expected)


