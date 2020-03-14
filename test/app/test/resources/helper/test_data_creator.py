from mysql.connector import connect
import os
import json
import sys
import decimal
from datetime import date, datetime, timedelta, time
import argparse

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        if isinstance(o, datetime):
            return self._datetime_to_str(o)
        if isinstance(o, date):
            return self._date_to_str(o)
        if isinstance(o, timedelta):
            return self._timedelta_to_str(o)
        if isinstance(o, time):
            return self._time_to_str(o)
        if isinstance(o, (bytes, bytearray)):
            return o.decode('utf8')
        return json.JSONEncoder.default(self, o)

    def _timedelta_to_str(self, value):
        """
        Converts a timedelta instance to a string suitable for json.
        The returned string has format: %H:%M:%S
        Returns a bytes.
        """
        seconds = abs(value.days * 86400 + value.seconds)
        if value.microseconds:
            fmt = '{0:02d}:{1:02d}:{2:02d}.{3:06d}'
            if value.days < 0:
                mcs = 1000000 - value.microseconds
                seconds -= 1
            else:
                mcs = value.microseconds
        else:
            fmt = '{0:02d}:{1:02d}:{2:02d}'
        if value.days < 0:
            fmt = '-' + fmt
        (hours, remainder) = divmod(seconds, 3600)
        (mins, secs) = divmod(remainder, 60)
        if value.microseconds:
            result = fmt.format(hours, mins, secs, mcs)
        else:
            result = fmt.format(hours, mins, secs)
        return result.encode('ascii')

    def _time_to_str(self, value):
        """
        Converts a time instance to a string suitable for json.
        The returned string has format: %H:%M:%S[.%f]
        If the instance isn't a datetime.time type, it return None.
        Returns a bytes.
        """
        if value.microsecond:
            return value.strftime('%H:%M:%S.%f').encode('ascii')
        return value.strftime('%H:%M:%S').encode('ascii')

    def _date_to_str(self, value):
        """
        Converts a date instance to a string suitable for json.
        The returned string has format: %Y-%m-%d
        If the instance isn't a datetime.date type, it return None.
        Returns a bytes.
        """
        return '{0:d}-{1:02d}-{2:02d}'.format(value.year, value.month, value.day).encode('ascii')

    def _datetime_to_str(self, value):
        """
        Converts a datetime instance to a string suitable for json.
        The returned string has format: %Y-%m-%d %H:%M:%S[.%f]
        If the instance isn't a datetime.datetime type, it return None.
        Returns a bytes.
        """
        if value.microsecond:
            fmt = '{0:d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}.{6:06d}'
            return fmt.format(
                value.year, value.month, value.day,
                value.hour, value.minute, value.second,
                value.microsecond).encode('ascii')

        fmt = '{0:d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}'
        return fmt.format(
            value.year, value.month, value.day,
            value.hour, value.minute, value.second).encode('ascii')

    def _struct_time_to_str(self, value):
        """
        Converts a time.struct_time sequence to a string suitable
        for json.
        The returned string has format: %Y-%m-%d %H:%M:%S
        Returns a bytes or None when not valid.
        """
        return time.strftime('%Y-%m-%d %H:%M:%S', value).encode('ascii')

class TestDataCreator(object):
    def __init__(self):

        self.conn = connect(host="localhost",user="root",passwd="",database=DATABASE_NAME)
        self.mycursor = self.conn.cursor()   

    def test_json_creator(self):
        
        self.mycursor.execute("SHOW TABLES")
        tables = self.mycursor.fetchall()
        table_list = []
        for ind,t in enumerate(tables):
            dict_list = []
            self.mycursor.execute("SELECt * FROM {}".format(t[0]))
            data = self.mycursor.fetchall()
            for d in data:
                json_dict = {}
                for column_index,column_name in enumerate(self.mycursor.description):
                    json_dict.update({column_name[0]:d[column_index]})
                dict_list.append(json_dict)
            table_list.append({t[0]:dict_list})
        return table_list

    def write_test_json(self):
        
        table_data = self.test_json_creator()
        path = "../test_data/expected_data/{}".format(DATABASE_NAME)
        print('writing sql rows to "{path}"')
        os.makedirs(path,exist_ok=True)
        for td in table_data:
            for tname,tdata in td.items():
                file_name = "{}.json".format(tname)
                with open(os.path.join(path,file_name),"w",encoding="utf8") as jf:
                    for data in tdata:
                        jf.write(json.dumps(data,cls=CustomJSONEncoder)+"\n")

    def sql_row_creator(self):
        self.mycursor.execute("SHOW TABLES")
        tables = self.mycursor.fetchall()
        path = "../test_data/input_data/{0}".format(DATABASE_NAME)
        print('writing test json to "{path}"')
        for t in tables:
            file_name = "{}.txt".format(t[0])
            self.mycursor.execute("SELECt * FROM {}".format(t[0]))
            data = self.mycursor.fetchall()
            os.makedirs(path,exist_ok=True)
            with open(os.path.join(path,file_name),'w',encoding="utf8") as tfile:
                tfile.write(str(self.mycursor.description).strip()+"\n")
                for d in data:
                    tfile.write(str(d).strip()+"\n")

    def main(self):
        self.write_test_json()
        self.sql_row_creator()


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-d','--dbname',help="DBName to create test data for testing")
    args = parser.parse_args()
    DATABASE_NAME = args.dbname
    testdatacreator = TestDataCreator()
    testdatacreator.main()
