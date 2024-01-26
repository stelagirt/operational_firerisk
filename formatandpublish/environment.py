'''
Created on 11 Oct 2017

@author: alex
'''

import sys
import traceback
import os
from backports import configparser
import logging
import json
import psycopg2
import select
from datetime import datetime
from distutils.util import strtobool


class Environment(object):

    def __init__(self, openconn=True):

        self.DEBUG = True if "FRISKDEBUG" in os.environ else False
        self.inipath = os.getcwd() if not "FRISKROOT" in os.environ else os.path.join(os.environ['FRISKROOT'],
                                                                                             "config")
        config = configparser.ConfigParser()
        self.mglog = None
        try:
            config.read(os.path.join(self.inipath,"config.ini"), encoding='utf-8')

            Paths = config['Paths']
            self.rootpath = os.environ['FRISKROOT'] if "GEOHUBROOT" in os.environ else Paths['rootpath']
            self.rootpath = self.rootpath + os.sep if self.rootpath[-1] != os.sep else self.rootpath
            self.mapspath = os.path.join(self.rootpath,Paths['mappath'])
            self.lstpath = os.path.join(self.rootpath,Paths['lstpath'])
            self.ndvi1path = os.path.join(self.rootpath,Paths['ndvi1path'])
            self.ndvi2path = os.path.join(self.rootpath,Paths['ndvi2path'])
            self.meteopath = os.path.join(self.rootpath,Paths['meteopath'])
            self.publishedpath = os.path.join(self.rootpath, Paths['publishedpath'])

            self.configpath = os.path.join(self.rootpath,Paths['configpath'])
            self.datapath = os.path.join(self.rootpath, Paths['datapath'])
            self.logs = os.path.join(self.rootpath,Paths['logs'])
            self.processlog = Paths['processlog']

            Filenames = config["Filenames"]
            self.mapfilesuffix = Filenames['mapfilesuffix']
            self.cellfilesuffix = Filenames['cellfilesuffix']
            self.reclfilesuffix = Filenames['reclfilesuffix']

            if openconn:
                self.conn = Connections(config['dbconnection'])

            self.logger = None
            notifications = config['Notifications']
            self.smtphost = notifications['smtphost']
            self.smtpuser = notifications['smtpuser']
            self.smtppass = config.get('Notifications', 'smtppass', raw=True)
            self.smtpport = notifications['smtpport']

        except:
            print('Configuration Missing. Exiting...')
            traceback.print_exc()
            sys.exit()

class Connections(object):
    def __init__(self, connargs):
        self.pg_dbname = os.environ['FRISKDB'] if "FRISKDB" in os.environ else connargs['pg_dbname']
        self.pg_user = connargs['pg_user']
        self.pg_password = connargs['pg_password']
        self.pg_host = connargs['pg_host']
        self.pg_port = connargs['pg_port']

        self.postgr = psycopg2.connect(dbname=self.pg_dbname,
                                       user=self.pg_user,
                                       password=self.pg_password,
                                       host=self.pg_host,
                                       port=self.pg_port)