import sys
sys.path.append("../")
import os
from unittest import TestCase,main
from etl.path import Path

class TestPath(TestCase):
    
    def test_get(self):

        expected = "../data/manifest/manifest.txt"
        actual = Path(expected).get()

        self.assertEqual(expected,actual)
    
    def test_get_with_suffix(self):

        input_path = '../data/input'
        suffix = 'daily_m25_4u_2019-12-14_05h30m_Saturday.sql.gz'
        expected = "../data/input\daily_m25_4u_2019-12-14_05h30m_Saturday.sql.gz"
        actual = Path(input_path).get_with_suffix(suffix)

        self.assertEqual(expected,actual)
    
    def test_is_local_True(self):

        input_path = '../data/input'
        self.assertTrue(Path(input_path).is_local())
        
    def test_is_local_False(self):

        input_path = 's3://test/testfile.file'
        self.assertFalse(Path(input_path).is_local())

    def test_local_mkdirs(self):

        output_path = '../data/output/test_1'
        Path(output_path).local_mkdirs()
        self.assertTrue(os.path.exists(output_path)==True)

    def test_get_path(self):

        input_path = "../data/input"
        expected = "../data/input"
        actual = Path(input_path).get_path()
        self.assertEqual(expected,actual)

    def test_get_path_with_base_path_removed(self):

        base_path = '../data/output'
        output_path = "../data/output/test"
        expected = "/test"
        actual = Path(output_path).get_path_with_base_path_removed(base_path)
        self.assertEqual(expected,actual)

    def test_is_s3_True(self):

        input_path = "s3://test/testfile.file"
        self.assertTrue(Path(input_path).is_s3())
    
    def test_is_s3_False(self):

        input_path = "../data/input"
        self.assertFalse(Path(input_path).is_s3())

    def test_get_s3_bucket(self):

        input_path = "s3://test/testfile.file"
        expected = "test"
        actual = Path(input_path).get_s3_bucket()
        self.assertEqual(expected,actual)

    def test_get_s3_key(self):

        input_path = "s3://test/testfile.file"
        expected = "testfile.file"
        actual = Path(input_path).get_s3_key()
        self.assertEqual(expected,actual)

    def test_is_mysql_True(self):

        mysql_path = "mysql://root@localhost/mysql?unix_socket=/var/run/mysqld/mysqld.sock"
        self.assertTrue(Path(mysql_path).is_mysql())
        
    def test_is_mysql_False(self):

        mysql_path = "s3://test"
        self.assertFalse(Path(mysql_path).is_mysql())

    def test_get_mysql_cfg_without_pass_and_port(self):

        mysql_path = "mysql://root@localhost/mysql?unix_socket=/var/run/mysqld/mysqld.sock"
        expected = {'unix_socket': '/var/run/mysqld/mysqld.sock', 'user': 'root', 'host': 'localhost', 'database': 'mysql'}
        actual = Path(mysql_path).get_mysql_cfg()
        self.assertDictEqual(expected,actual)    
        
    def test_get_mysql_cfg_with_pass_and_port(self):

        mysql_path = "mysql://root:123@localhost:3306/mysql?unix_socket=/var/run/mysqld/mysqld.sock"
        expected = {'unix_socket': '/var/run/mysqld/mysqld.sock', 'user': 'root','password': '123', 'host': 'localhost','port':'3306', 'database': 'mysql'}
        actual = Path(mysql_path).get_mysql_cfg()
        self.assertDictEqual(expected,actual)
