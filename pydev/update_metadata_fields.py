# coding: utf-8
#!/usr/bin/env python

#updates datadict dataset
from optparse import OptionParser
from ConfigUtils import *
from SocrataStuff import *
from Utils import *
from UpdateSocrataFields import *
from JobStatusEmailerComposer import *


def parse_opts():
  helpmsgConfigFile = 'Use the -c to add a config yaml file. EX: fieldConfig.yaml'
  parser = OptionParser(usage='usage: %prog [options] ')
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

  fieldConfigFile = None
  config_inputdir = None
  fieldConfigFile = options.configFn
  config_inputdir = options.configDir
  return config_inputdir, fieldConfigFile

def main():
  config_inputdir, fieldConfigFile = parse_opts()
  cI =  ConfigItems(config_inputdir ,fieldConfigFile  )
  configItems = cI.getConfigs()
  lg = pyLogger(configItems)
  logger = lg.setConfig()
  logger.info("****************JOB START******************")
  sc = SocrataClient(config_inputdir, configItems, logger)
  client = sc.connectToSocrata()
  clientItems = sc.connectToSocrataConfigItems()
  socrataLoadUtils = SocrataLoadUtils(configItems)
  scrud = SocrataCRUD(client, clientItems, configItems, logger)
  sqry = SocrataQueries(clientItems, configItems, logger)
  datasets = socrataLoadUtils.make_datasets()
  finshed_datasets = []
  for dataset in datasets:
    insertDataSet, dataset = socrataLoadUtils.makeInsertDataSet(dataset)
    dataset = scrud.postDataToSocrata(dataset, insertDataSet )
    finshed_datasets.append(dataset)
  print finshed_datasets
  logger.info(finshed_datasets)
  dsse = JobStatusEmailerComposer(configItems, logger)
  dsse.sendJobStatusEmail(finshed_datasets)

if __name__ == "__main__":
    main()
