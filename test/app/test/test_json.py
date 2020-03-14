
import sys
sys.path.append("../")
import unittest
import decimal
import json
from datetime import timedelta,datetime,time,date
from time import localtime
from etl.json import CustomJSONEncoder

class TestCustomJSONEncoder(unittest.TestCase):
	
		
		
	def test_decimal(self):
		
		expected = '0.7'
		actual = json.dumps(decimal.Decimal(0.7), cls=CustomJSONEncoder)
		self.assertEqual(expected,actual)
	
	def test_decimal_2(self):
		expected = '7.0'
		actual = json.dumps(decimal.Decimal(7), cls=CustomJSONEncoder)
		self.assertEqual(expected,actual)
	
	def test_timedelta_to_str(self):
	
		'''This function will test the _timedelta_to_str method in the etl.json module'''
		expected = '"24:00:00"'
		actual = json.dumps(timedelta(1),cls=CustomJSONEncoder)
		self.assertEqual(expected,actual)



	def test_time_to_str(self):
	
		'''This function will test the _time_to_str method in the etl.json module'''
		expected = '"11:16:35.030947"'
		actual = json.dumps(time(hour = 11, minute = 16, second = 35,microsecond=30947),cls=CustomJSONEncoder)
		self.assertEqual(expected,actual)


	def test_time_to_str_2(self):
	
		'''This function will test the _time_to_str method in the etl.json module'''
		expected = '"11:26:11"'
		actual = json.dumps(time(hour = 11, minute = 26, second = 11),cls=CustomJSONEncoder)
		self.assertEqual(expected,actual)


	def test_date_to_str(self):
		'''This function will test the _date_to_str method in the etl.json module'''
		expected = '"2020-03-13"'
		tm = datetime(2020,3,13)
		actual = json.dumps(date(year = tm.year, month = tm.month, day = tm.day),cls=CustomJSONEncoder)
		self.assertEqual(expected,actual)

	def test_datetime_to_str(self):
		'''This function will test the _datetime_to_str method in the etl.json module'''
		expected = '"2020-03-13 12:09:02.844535"'
		tm = datetime(2020,3,13,12,9,2,844535)
		actual = json.dumps(tm,cls=CustomJSONEncoder)
		self.assertEqual(expected,actual)
		
	def test_datetime_to_st_2(self):
		'''This function will test the _datetime_to_str_2 method in the etl.json module'''
		expected = '"'+datetime(2020,3,9,5,15,30).strftime('%Y-%m-%d %H:%M:%S')+'"'
		tm = datetime(2020,3,9,5,15,30)
		actual = json.dumps(tm,cls=CustomJSONEncoder)
		self.assertEqual(expected,actual)

	def test_byte(self):
		expected = '"Hello"'
		actual = json.dumps(b'Hello',cls=CustomJSONEncoder)
		self.assertEqual(expected,actual)


	def test_default(self):
		'''This function will test the default method in the etl.json module'''
		expected="123"
		actual = json.dumps(123,cls=CustomJSONEncoder)
		self.assertEqual(expected,actual)
