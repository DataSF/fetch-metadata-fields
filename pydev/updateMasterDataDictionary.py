# coding: utf-8
#!/usr/bin/env python

#updates datadict dataset
from optparse import OptionParser
from ConfigUtils import *
from SocrataStuff import *
from Utils import *
from UpdateSocrataFields import *


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
sqry = SocrataQueries(clientItems, configItems)


tables = configItems['tables']
dataset_inventory = tables['dataset_inventory']
dataset_inventory_cols = tables['dataset_inventory']['field_list']
dataset_inventory_cols_qry = ",".join(dataset_inventory_cols)
print dataset_inventory_cols_qry
rowCount = sqry.getRowCnt(dataset_inventory['fbf'])
qry = '?$select='+dataset_inventory_cols_qry

results = sqry.getQry(dataset_inventory['fbf'], qry)
print results
print rowCount
