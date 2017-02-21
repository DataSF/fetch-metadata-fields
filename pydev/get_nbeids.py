
# coding: utf-8
#!/usr/bin/env python

from optparse import OptionParser
from ConfigUtils import *
from SocrataStuff import *
from Utils import *
from PandasUtils import *
from MasterDataset import *
from JobStatusEmailerComposer import *
from PyLogger import *
from MetaDatasets import *

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

  config_inputdir = None
  fieldConfigFile = None
  fieldConfigFile = options.configFn
  config_inputdir = options.configDir
  return fieldConfigFile, config_inputdir


def main():
  fieldConfigFile, config_inputdir = parse_opts()
  cI =  ConfigUtils(config_inputdir ,fieldConfigFile  )
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
  tables = configItems['tables']
  metadatasets = MetaDatasets(configItems, sqry, logger)
  dfs_dict = BuildDatasets.getDatasets(tables, sqry)
  job_success = False
  master_dd_json = metadatasets.get_master_metadataset_as_json()
  #print master_dd_json
  #master_dd_json = True

  asset_fields, asset_inventory, data_dictionary_attachments, dataset_inventory, coordinators = MasterDataDictionary.filter_base_datasets(dfs_dict)
  asset_inventory_json = NbeIds.get_nbe_id_for_df(asset_inventory, sqry, configItems )
  #asset_inventory_json = True
  if asset_inventory_json and master_dd_json:
    nbe_asset_inventory_json = WkbkJson.loadJsonFile(configItems['inputDataDir'], configItems['nbe_migration_fn'])
    master_dd_json_obj = WkbkJson.loadJsonFile(configItems['inputDataDir'], configItems['master_dd_json_fn'])
    nbeid_list = NbeIds.get_nbeid_final_df(asset_fields, nbe_asset_inventory_json, master_dd_json_obj)
    nbeid_list_rows =  PandasUtils.convertDfToDictrows(nbeid_list)
    dataset_info = MasterDataDictionary.set_dataset_info(configItems, socrataLoadUtils, nbeid_list_rows)
    dataset_info = scrud.postDataToSocrata(dataset_info, nbeid_list_rows )
    print dataset_info
    dsse = JobStatusEmailerComposer(configItems, logger)
    dsse.sendJobStatusEmail([dataset_info])
    job_success = True
  if(not(job_success)):
    dataset_info = {'Socrata Dataset Name': "NbeIds", 'SrcRecordsCnt':0, 'DatasetRecordsCnt':0, 'fourXFour': "Job Failed"}
    dataset_info['isLoaded'] = 'failed'
    dsse.sendJobStatusEmail([dataset_info])


if __name__ == "__main__":
    main()



