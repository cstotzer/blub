#!/usr/bin/env python

"""BlobLoader

Utility allows to load arbitrary files into Oracle as Blobs.

Usage:
  blobloader.py load [--cfgfile <file>] [--cfgkey <name>] [-H <host>] <key> <user> <dsn> <id> <filename>
  blobloader.py dump [--cfgfile <file>] [--cfgkey <name>] [-H <host>] <key> <user> <dsn> <id> <filename>
  blobloader.py savepasswd <user> <dsn>
  blobloader.py echopasswd <user> <dsn>
  blobloader.py print [--cfgfile <file>] [--cfgkey <name>]
  blobloader.py --help
  blobloader.py --version

Options:
  -H <host>               Hostname
  -c, --cfgfile <file>    Name of a configuration file [default: ~/.blobloader]
  -k, --cfgkey <name>     Name of a configuration key [default: default]
  -h, --help              Show this screen and exit.
  -v, --version           Show version and exit.

"""
from __future__ import print_function
from docopt import docopt
import cx_Oracle
from os import path
import getpass
import json


class Configuration(object):
    config = {}

    def __init__(self, filename, key):
        if path.isfile(filename):
            with open(filename, 'r') as file:
                data = json.load(file)
                self.config = data[key]


class BlobLoader(object):
    config = None

    def __init__(self, configuration_file, configuration_key):
        self.config = Configuration(path.expanduser(configuration_file), configuration_key)

    pwdfilepath = path.expanduser('~/.blobloader_passwd')

    def printConfig(self):
        print(self.config.config)

    def createSqlString(self):
        sqlStr = "update my_blob_table set data=:blobData where id = :id"
        return

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

    def connect(self, user, dsn):
        password = self.readPassword(user, dsn)
        if dsn == None:
            self.conn = cx_Oracle.connect(user, password)
        else:
            self.conn = cx_Oracle.connect(user, password, dsn)

    def load(self, id, filename):
        file = open(filename, 'rb')
        ext = 'jpg'
        content = file.read()
        file.close()

        curs = self.conn.cursor()
        idvar = curs.var(cx_Oracle.NUMBER)
        idvar.setvalue(0, int(id))
        blobvar = curs.var(cx_Oracle.BLOB)
        blobvar.setvalue(0, content)

        curs.setinputsizes(blobData=cx_Oracle.BLOB)
        # TODO: Error handling
        curs.execute(sqlStr, {'id': idvar, 'blobData': blobvar})
        curs.execute('commit')
        curs.close()

    def dump(self, id, filename):
        pass

    def disconnect(self):
        self.conn.close();


if __name__ == '__main__':
    arguments = docopt(__doc__, version='blobloader 0.1.0')
    print(arguments)
    loader = BlobLoader(arguments['--cfgfile'], arguments['--cfgkey'])
    if arguments['load']:
        print('load')
        loader.connect(arguments['<user>'], arguments['<dsn>'])
        loader.load(arguments['<id>'], arguments['<filename>'])
        loader.disconnect()

    if arguments['print']:
        print('print')
        loader.printConfig()

    if arguments['dump']:
        print('dump')

    if arguments['savepasswd']:
        print('save')
        loader.savePassword(arguments['<user>'], arguments['<dsn>'])

    if arguments['echopasswd']:
        print('echo')
        print(loader.readPassword(arguments['<user>'], arguments['<dsn>']))
