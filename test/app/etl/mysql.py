
import os
import io
import socket
import subprocess

import mysql.connector
from mysql.connector import errorcode

from .path import Path
from .log import Log
from .opts import Opts


class MySQL(object):
    def __init__(self, log: Log, opts: Opts, dbname: str):
        super().__init__()
        self.log = log.new('mysql')
        self.opts = opts
        self.dbname = dbname
        self.should_start_mysql = opts.start_mysql == "yes"
        self.mysql_cfg = Path(opts.get('mysql_url')).get_mysql_cfg()
        self.connect()

    def connect(self):
        if self.should_start_mysql and not self.is_port_open(
                self.mysql_cfg.get('host', 'localhost'),
                self.mysql_cfg.get('port', 3306)):
            self.start_mysql()
        self.cnx = mysql.connector.connect(**self.mysql_cfg)
        self.cursor = self.cnx.cursor()

    def close(self, drop=False):
        self.drop()
        if self.cursor is not None:
            self.cursor.close()
        if self.cnx is not None:
            self.cnx.close()

    def is_port_open(self, host, port) -> bool:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((host, int(port)))
            s.shutdown(2)
            return True
        except:
            return False

    def start_mysql(self):
        self.log.info("Starting MYSQL")
        os.system("service mysql start")

    # uses mysql connector execute to load/import
    # method: by parsing sql file then ignoring comments and running single statement at a time
    def import_sql(self, fin):
        sql = ''
        for line in fin:
            tempLine = line.strip()
            # Skip empty lines or comments.
            if len(tempLine) == 0 or tempLine[0] == '#' or tempLine.startswith("--"):
                continue
            sql += line
            if not ';' in line:
                continue
            # self.log.debug("[sql] {sql} [/sql]", sql=sql)
            self.cursor.execute(sql)
            sql = ''

    # uses mysql connector execute to load/import
    # method: by loading whole sql file in one go.
    # NOTE: hangs for bigger file e.g. 10mb sql file.
    def import_sql_content(self, content: str):
        for res in self.cursor.execute(content, multi=True):
            if res.with_rows:
                self.log.debug("Rows produced by statement '{}'".format(res.statement))
                # self.log.debug(result.fetchall())
            else:
                lmsg = "Number of rows affected by statement '{s}': {r}".format(s=res.statement, r=res.rowcount)
                self.log.debug(lmsg)
                # self.log.debug(result)

    # creates mysql cmd
    def _mysql_cmd(self) -> list:
        cmd = ['mysql']
        if 'host' in self.mysql_cfg:
            cmd.append('-h')
            cmd.append(self.mysql_cfg['host'])
        if 'port' in self.mysql_cfg:
            cmd.append('-P')
            cmd.append(self.mysql_cfg['port'])
        if 'user' in self.mysql_cfg:
            cmd.append('-u')
            cmd.append(self.mysql_cfg['user'])
        if 'password' in self.mysql_cfg:
            cmd.append("-p'{}'".format(self.mysql_cfg['password']))
        return cmd

    # uses python process to import sql file to mysql
    def import_sql_using_pycmd(self, fin):
        cmd = self._mysql_cmd()
        self.log.debug("  CMD: {}".format(' '.join(cmd)))
        p = subprocess.Popen(cmd, stdin=subprocess.PIPE)
        with p.stdin as pin:
            for data in fin:
                pin.write(data)
        exitcode = p.wait()
        self.log.debug("  EXITCODE: {}".format(exitcode))

    # uses os system func to import sql file to mysql
    def import_sql_using_system(self, sqlfile: str):
        cmd = self._mysql_cmd()
        cmd.append('<')
        cmd.append(sqlfile)
        self.log.debug("  CMD: {}".format(' '.join(cmd)))
        exitcode = os.system(' '.join(cmd))
        self.log.debug("  EXITCODE: {}".format(exitcode))

    def exec_sql(self, sql):
        self.log.info("  Query={}".format(sql))
        self.cursor.execute(sql)

    def get_tables(self) -> list:
        self.log.info("Extracting DB {}".format(self.dbname))
        self.exec_sql("USE {}".format(self.dbname))
        self.exec_sql("SHOW TABLES")
        tablenames = []
        for (tblname,) in self.cursor.fetchall():
            tablenames.append(tblname)
        return tablenames

    def get_rows_count(self, tblname) -> int:
        self.log.info("Find Table {}.{} Rows Count.".format(self.dbname, tblname))
        self.exec_sql("SELECT count(*) as count FROM {}.{}".format(self.dbname, tblname))
        for row in self.cursor.fetchall():
            for value in row:
                    return value
        return 0

    def get_all_rows(self, tblname) -> (list, list):
        self.log.info("Extracting Table {}.{}".format(self.dbname, tblname))
        self.exec_sql("SELECT * FROM {}.{}".format(self.dbname, tblname))
        rows = self.cursor.fetchall()
        return self.cursor.description, rows

    def drop(self):
        self.exec_sql("UNLOCK TABLES")
        self.exec_sql("DROP DATABASE IF EXISTS {}".format(self.dbname))
