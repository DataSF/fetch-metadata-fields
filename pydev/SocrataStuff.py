
from __future__ import division
import shutil
import inflection
import pandas as pd
import re
import csv
import inflection
import re
import datetime
import os
import requests
from sodapy import Socrata
import yaml
import base64
import itertools
import datetime
import json
import time
import logging
from retry import retry
import urllib2
import zipfile
from retry import retry
from socket import error as SocketError
import errno
from ConfigUtils import *
from Utils import *


class SocrataClient:
    def __init__(self, inputdir, configItems):
        self.inputdir = inputdir
        self.configItems = configItems

    def connectToSocrata(self):
        clientConfigFile = self.inputdir + self.configItems['socrata_client_config_fname']
        with open(clientConfigFile,  'r') as stream:
            try:
                client_items = yaml.load(stream)
                client = Socrata(client_items['url'],  client_items['app_token'], username=client_items['username'], password=base64.b64decode(client_items['password']))
                return client
            except yaml.YAMLError as exc:
                print(exc)
        return 0

    def connectToSocrataConfigItems(self):
        clientConfigFile = self.inputdir + self.configItems['socrata_client_config_fname']
        with open(clientConfigFile,  'r') as stream:
            try:
                clientItems = yaml.load(stream)
                return clientItems
            except yaml.YAMLError as exc:
                print(exc)
        return 0


class SocrataCRUD:

    def __init__(self, client, clientItems, configItems, logger):
        self.client = client
        self.chunkSize = 1000
        self.row_id = configItems['row_id_field']
        self.name = configItems['dataset_name_field']
        self.fourXFour = configItems['fourXFour']
        self.rowsInserted = configItems['dataset_records_cnt_field']
        self.clientItems = clientItems
        self.isLoaded = configItems['isLoaded']
        self.logger = logger
        self.src_records_cnt_field = configItems['src_records_cnt_field']

    @retry( tries=5, delay=1, backoff=2)
    def insertGeodataSet(self, dataset, insertDataSet):
        insertChunks = self.makeChunks(insertDataSet)
        #overwrite the dataset on the first insert chunk[0] if there is no row id
        if dataset[self.rowsInserted] == 0 and dataset[self.row_id ] == '':
            print "replacing and inserting"
            rejectedChunk = self.replaceDataSet(dataset, insertChunks[0])
            if len(insertChunks) > 1:
                for chunk in insertChunks[1:]:
                    rejectedChunk = self.insertData(dataset, chunk)
        else:
            print "upserting..."

            for chunk in insertChunks:
                rejectedChunk = self.insertData(dataset, chunk)
        return dataset


    @retry( tries=10, delay=1, backoff=2)
    def replaceDataSet(self, dataset, chunk):
        result = self.client.replace( dataset[self.fourXFour], chunk )
        dataset[self.rowsInserted] = dataset[self.rowsInserted] + int(result['Rows Created'])
        time.sleep(0.25)


    @retry( tries=10, delay=1, backoff=2)
    def insertData(self, dataset, chunk):
        result = self.client.upsert(dataset[self.fourXFour], chunk)
        dataset[self.rowsInserted] = dataset[self.rowsInserted] + int(result['Rows Created'])
        time.sleep(0.25)


    def makeChunks(self, insertDataSet):
        return [insertDataSet[x:x+ self.chunkSize] for x in xrange(0, len(insertDataSet), self.chunkSize)]


    def postDataToSocrata(self, dataset, insertDataSet ):
        if dataset[self.fourXFour]!= 0:
            dataset = self.insertGeodataSet(dataset, insertDataSet)
            dataset = self.checkCompleted(dataset)
        else:
            print "dataset does not exist"
        return dataset

    def checkCompleted(self, dataset):
        if dataset[self.rowsInserted] == dataset[self.src_records_cnt_field]:
            dataset[self.isLoaded] = 'success'
        else:
            dataset[self.isLoaded] = 'failed'
        if dataset[self.isLoaded] == 'failed':
            dataset[self.rowsInserted] = self.getRowCnt(dataset)
            if dataset[self.rowsInserted] == dataset[self.src_records_cnt_field]:
                dataset[self.isLoaded] = 'success'
        if dataset[self.isLoaded] == 'success':
            print "data insert success!" + " Loaded " + str(dataset[self.rowsInserted]) + "rows!"
        return dataset

    def getRowCnt(self, dataset):
        time.sleep(1)
        qry = '?$select=count(*)'
        qry = "https://"+ self.clientItems['url']+"/resource/" +dataset[self.fourXFour]+ ".json" + qry
        r = requests.get( qry , auth=( self.clientItems['username'],  base64.b64decode(self.clientItems['password'])))
        cnt =  r.json()
        print cnt
        return int(cnt[0]['count'])

class SocrataLoadUtils:
    def __init__(self, configItems):
        self.datasets_to_load_fn = configItems['datasets_to_load_fn']
        self.dataset_name_field = configItems['dataset_name_field']
        self.dataset_src_dir_field = configItems['dataset_src_dir_field']
        self.dataset_src_fn_field= configItems['dataset_src_fn_field']
        self.inputConfigDir = configItems['inputConfigDir']
        self.rowsInserted = configItems['dataset_records_cnt_field']
        self.src_records_cnt_field = configItems['src_records_cnt_field']
        self.row_id = configItems['row_id_field']

    def make_datasets(self):
        datasets = PandasUtils.loadCsv(self.inputConfigDir+self.datasets_to_load_fn)
        datasets = PandasUtils.convertDfToDictrows(datasets)
        for dataset in datasets:
            dataset = self.setDatasetDicts(dataset)
        return datasets

    def setDatasetDicts(self, dataset):
        dataset[ self.rowsInserted] = 0
        dataset[self.src_records_cnt_field] = 0
        print dataset[self.row_id]
        if dataset[self.row_id] == 'None':
            dataset[self.row_id] = ''
        return dataset

    def makeInsertDataSet(self, dataset):
        insertDataSet = PandasUtils.loadCsv(dataset[self.dataset_src_dir_field]+ dataset[self.dataset_src_fn_field])
        insertDataSet = PandasUtils.convertDfToDictrows(insertDataSet)
        dataset[self.src_records_cnt_field] = len(insertDataSet)
        return insertDataSet, dataset

if __name__ == "__main__":
    main()
