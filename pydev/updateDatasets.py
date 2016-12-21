# coding: utf-8
#!/usr/bin/env python

#updates datadict dataset
from optparse import OptionParser
from ConfigUtils import *
from SocrataStuff import *
from Utils import *
from UpdateSocrataFields import *

helpmsgUpdateSchedule = 'Use the -d option for updating the data dictionary attachment dataset'
parser = OptionParser(usage='usage: %prog [options] ')
parser.add_option('-a', '--attachmentsForDataDict',
                      action='store',
                      dest='updt_attachments_dataset',
                      default=None,
                      help=helpmsgUpdateSchedule ,)

helpmsgConfigFile = 'Use the -c to add a config yaml file. EX: fieldConfig.yaml'
parser.add_option('-c', '--configfile',
                      action='store',
                      dest='configFn',
                      default=None,
                      help=helpmsgConfigFile ,)

helpmsgConfigDir = 'Use the -d to add directory path for the config files. EX: /home/ubuntu/configs'
parser.add_option('-d', '--configdir',
                      action='store',
                      dest='configDir',
                      default=None,
                      help=helpmsgConfigDir ,)

(options, args) = parser.parse_args()

if  options.configFn is None:
    print "ERROR: You must specify a config yaml file!"
    print helpmsgConfigFile
    exit(1)
elif options.configDir is None:
    print "ERROR: You must specify a directory path for the config files!"
    print helpmsgConfigDir
    exit(1)

updt_attachments_dataset = options.updt_attachments_dataset
fieldConfigFile = options.configFn
config_inputdir = options.configDir


cI =  ConfigItems(config_inputdir ,fieldConfigFile  )
configItems = cI.getConfigs()
sc = SocrataClient(config_inputdir, configItems)
client = sc.connectToSocrata()
clientItems = sc.connectToSocrataConfigItems()
lg = pyLogger(configItems)
logger = lg.setConfig()
socrataLoadUtils = SocrataLoadUtils(configItems)
scrud = SocrataCRUD(client, clientItems, configItems, logger)

datasets = socrataLoadUtils.make_datasets()
for dataset in datasets:
  print dataset
  insertDataSet, dataset = socrataLoadUtils.makeInsertDataSet(dataset)
  if 'Data Dictionary Attachments' in dataset.values():
    insertDataSet = UpdateSocrataFields.addIsDataDictBool(insertDataSet)
  dataset = scrud.postDataToSocrata(dataset, insertDataSet )
  print dataset
