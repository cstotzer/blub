#!/usr/bin/env python
#
#    Copyright (C) 2015 Christoph Stotzer (christoph.stotzer@gmail.com)
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Blub
====

  Allows loading arbitrary files into Oracle BLOB (Binary Large Object) columns.

Usage:
  blobloader.py load  [-vb] [--cfgfile <file>] [--cfgkey <key>] <id> <filename>
  blobloader.py dump  [-vb] [--cfgfile <file>] [--cfgkey <key>] <id> <filename>
  blobloader.py print [-v] [--cfgfile <file>] [--cfgkey <key>]
  blobloader.py --help
  blobloader.py --version

Options:
  -b, --batch             Batch mode
  -c, --cfgfile <file>    Name of a configuration file [default: ~/.blobloader]
  -k, --cfgkey <key>      Name of a configuration file [default: default]
  -h, --help              Show this screen and exit.
  -v, --verbose           Verbose mode
  --version               Show version and exit.

Description:

  Configuration File format:

  {
      "default": {
          "description": "This is my favorite configuration.",
          "user": "cstotzer",
          "table_name": "my_blob_table",
          "id_column": "id",
          "blob_column": "data"
      },
      "titi": {
          "user": "cstotzer",
          "schema": "cstotzer",
          "table_name": "my_blob_table",
          "id_column": "id",
          "blob_column": "data",
          "dsn": "ORCL"
      }
  }

"""
from __future__ import print_function
from docopt import docopt
import cx_Oracle
from os import path
import getpass
import json
import sys


class ExceptionParset(object):
    messages = {
        904: "Column name is invalid.",
        1747: "Table name is invalid.",
        932: "Datatype did not match.",
        1017: "Invalid credentials."
    }

    @staticmethod
    def parse(exception):

        if isinstance(exception, cx_Oracle.DatabaseError):
            if exception.message.code in ExceptionParset.messages:
                print("ERROR: {}".format(ExceptionParset.messages[exception.message.code]))
            else:
                print("ERROR: {}".format(exception.message.message))
        elif isinstance(exception, IOError):
            print("ERROR: IOException ({})".format(exception.strerror))
        else:
            raise exception


class Configuration(object):
    config = {}

    def checkProperty(self, property):
        rc = 0
        if property not in self.config:
            print(
                'ERROR: Mandatory property \'{}\' is missing in named configuration \'{}\' of configuration file {}'.format(
                    property, self.key, self.filename))
            rc = 3
        return rc

    def __init__(self, filename, key):
        rc = 0
        self.key = key if not key == "" else "default"
        self.filename = path.abspath(path.expanduser(filename))
        if path.isfile(self.filename):
            with open(self.filename, 'r') as file:
                data = json.load(file)
                if key in data:
                    self.config = data[self.key]
                    rc += self.checkProperty('user')
                    rc += self.checkProperty('table_name')
                    rc += self.checkProperty('id_column')
                    rc += self.checkProperty('blob_column')
                else:
                    print("ERROR: Named configuration \'{}\' not found in configuration file \'{}\'.".format(self.key,
                                                                                                             self.filename))
                    rc = 3
        else:
            print("ERROR: Configuration file \'{}\' not found.".format(self.filename))
            rc = 3

        if rc > 0:
            exit(rc)

    def get_user(self):
        return self.config['user']

    def get_dsn(self):
        if 'dsn' in self.config:
            return self.config['dsn']
        else:
            return None

    def get_table_name(self):
        return self.config['table_name']

    def get_id_column_name(self):
        return self.config['id_column']

    def get_blob_column_name(self):
        return self.config['blob_column']

    def get_schema(self):
        if 'schema' in self.config:
            return self.config['schema'] + '.'
        else:
            return ""


class BlobLoader(object):
    def __init__(self, config):
        self.config = config
        self.pwdfilepath = path.expanduser('~/.blobloader_passwd')
        self.verbose = False

    def printConfig(self):
        print(self.config.config)

    def createSqlInsertString(self):
        sqlStr = "update {schema}{table_name} set {blob_column_name}=:blobData where {id_column_name} = :id"
        sqlStr = sqlStr.format(table_name=self.config.get_table_name(),
                               blob_column_name=self.config.get_blob_column_name(),
                               id_column_name=self.config.get_id_column_name(),
                               schema=self.config.get_schema())
        if self.verbose: print(sqlStr)
        return sqlStr

    def createSqlSelectString(self):
        sqlStr = "SELECT {blob_column_name} FROM {schema}{table_name} where {id_column_name} = :id"
        sqlStr = sqlStr.format(table_name=self.config.get_table_name(),
                               blob_column_name=self.config.get_blob_column_name(),
                               id_column_name=self.config.get_id_column_name(),
                               schema=self.config.get_schema())
        if self.verbose: print(sqlStr)
        return sqlStr

    # TODO: Put in one class
    def savePassword(self, user, dsn):
        if dsn == None or dsn == '':
            dsn = 'LOCAL'

        key = user + '@' + dsn

        passwd = getpass.getpass()

        pwds = {}

        if path.isfile(path.expanduser(self.pwdfilepath)):
            # TODO: Load password when file present
            pwds = {key: passwd}
        else:
            pwds = {key: passwd}

        with open(self.pwdfilepath, 'wb') as file:
            json.dump(pwds, file)

    def readPassword(self, user, dsn):
        if dsn == None or dsn == '':
            dsn = 'LOCAL'

        key = user + '@' + dsn

        if path.isfile(path.expanduser(self.pwdfilepath)):
            with open(self.pwdfilepath, 'r') as file:
                pwds = json.load(file)
                return pwds[key]

    def connect(self, password):
        user = self.config.get_user()
        dsn = self.config.get_dsn()

        try:
            if dsn == None:
                self.conn = cx_Oracle.connect(user, password)
            else:
                self.conn = cx_Oracle.connect(user, password, dsn)
        except cx_Oracle.DatabaseError as e:
            ExceptionParset.parse(e)
            exit(8)

        if self.verbose: print("INFO: Connected to Oracle.")

    def load(self, id, filename):
        rc = 0
        filename = path.abspath(path.expanduser(filename))
        if not path.isfile(filename):
            print("ERROR: File \'{}\' does not exist.".format(filename))
            rc = 2
        file = open(filename, 'rb')

        content = file.read()
        file.close()

        curs = self.conn.cursor()
        idvar = curs.var(cx_Oracle.NUMBER)
        idvar.setvalue(0, int(id))
        blobvar = curs.var(cx_Oracle.BLOB)
        blobvar.setvalue(0, content)

        curs.setinputsizes(blobData=cx_Oracle.BLOB)

        try:
            curs.execute(self.createSqlInsertString(), {'id': idvar, 'blobData': blobvar})
            curs.execute('commit')
        except cx_Oracle.DatabaseError as e:
            ExceptionParset.parse(e)
            rc = 9
        finally:
            curs.close()

    def dump(self, id, filename):
        rc = 0
        filename = path.abspath(path.expanduser(filename))
        if not path.isdir(path.dirname(filename)):
            print("ERROR: Directory \'{}\' does not exist.".format(path.dirname(filename)))
            rc = 2
        else:
            curs = self.conn.cursor()
            idvar = curs.var(cx_Oracle.NUMBER)
            idvar.setvalue(0, int(id))

            try:
                curs.execute(self.createSqlSelectString(), {'id': idvar})

                row = curs.fetchone()
                if curs.rowcount > 0:
                    blob = row[0]
                    file = open(filename, 'wb')
                    file.write(blob.read())
                    file.close()
                else:
                    print("ERROR: Record with \'{}\' = \'{}\' not found.".format(self.config.get_id_column_name(), id))
                    rc = 9

            except Exception as e:
                # except cx_Oracle.DatabaseError as e:
                ExceptionParset.parse(e)
                rc = 9

            finally:
                curs.close()

        return rc

    def disconnect(self):
        try:
            self.conn.close()
        except:
            pass
        if self.verbose: print("INFO: Disconnected from Oracle.")

    def setVerbose(self, verbose):
        self.verbose = verbose


class Main(object):
    def __init__(self):
        self.arguments = docopt(__doc__, version='blobloader 0.1.0')
        self.verbose = self.arguments['--verbose']
        self.batch = self.arguments['--batch']
        if self.verbose: print(self.arguments)
        self.loader = BlobLoader(Configuration(self.arguments['--cfgfile'], self.arguments['--cfgkey']))
        self.loader.setVerbose(self.arguments['--verbose'])

    def load(self):
        self.loader.connect(self.readPassword())
        rc = self.loader.load(self.arguments['<id>'], self.arguments['<filename>'])
        self.loader.disconnect()
        exit(rc)

    def dump(self):
        self.loader.connect(self.readPassword())
        rc = self.loader.dump(self.arguments['<id>'], self.arguments['<filename>'])
        self.loader.disconnect()
        exit(rc)

    def print(self):
        self.loader.printConfig()
        print(self.loader.createSqlInsertString())

    def readPassword(self):
        pwd = None
        if self.batch:
            pwd = sys.stdin.readline()
        else:
            pwd = getpass.getpass("Enter password: ")
        return pwd.rstrip()

    def run(self):
        if self.arguments['load']:
            if self.verbose: print('load')
            self.load()

        if self.arguments['print']:
            if self.verbose: print('print')
            self.print()

        if self.arguments['dump']:
            if self.verbose: print('dump')
            self.dump()


if __name__ == '__main__':
    main = Main()
    main.run()
