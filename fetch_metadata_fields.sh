
#!/bin/bash
#grab the data
npm run output_csvs
#update the metadata fields
python pydev/update_metadata_fields.py -c fieldConfig.yaml -d /Users/j9/Desktop/fetch-socrata-fields/configs/
#update the master datadictionary
python pydev/update_master_data_dictionary.py -c fieldConfigMasterDD.yaml -d /Users/j9/Desktop/fetch-socrata-fields/configs/
