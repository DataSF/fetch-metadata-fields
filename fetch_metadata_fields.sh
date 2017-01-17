
#!/bin/bash
#
#bash script to update the asset fields, datadictionary attachments and the master data dictionary


OPTIND=1         # Reset in case getopts has been used previously in the shell.

display_help() {
    echo "Usage: $0 [option...] {d}" >&2
    echo
    echo "   -d, --main_dir   -- main path to package files"
    echo
    echo "   -a, --config_file_for_asset_fields   ---   run the script to update asset fields using this config file"
    echo
    echo "   -m, --config_file_for_master_data_dictionary   --  run the script to update master data dictionary using this config file"
    echo
    echo "   -p, --python path  -- path to python- ie run which python to find out"
    echo
    echo "   -n, --npm path  -- path to npm- ie run: npm bin -g to find out"
    echo
    echo " ***example usage: ./fetch_metadata_fields.sh -d ~/Desktop/fetch-socrata-fields/configs/ -a fieldConfig_desktop.yaml -p /usr/local/bin/python -m fieldConfigMasterDD_desktop.yaml -n /usr/local/bin/npm "
    echo " ***example usage: ./fetch_metadata_fields.sh -d /home/ubuntu/fetch-metadata-fields/configs/ -a fieldConfig_server.yaml -p /home/ubuntu/miniconda2/bin/python -m fieldConfigMasterDD_server.yaml -n /usr/local/bin/npm"
    exit 1
}
# Initialize our own variables:
path_to_main_dir=""
asset_fields_config=""
master_dd_config=""
python_path=""
npm_path=""

while getopts "h?:d:a:m:p:n:j:" opt; do
    case "$opt" in
    h|\?)
        display_help
        exit 0
        ;;
    d)  path_to_main_dir=$OPTARG
        ;;
    a)  config_fn_asset_fields=$OPTARG
        ;;
    m)  config_fn_master_dd=$OPTARG
        ;;
    p)  python_path=$OPTARG
        ;;
    n)  npm_path=$OPTARG
        ;;
    esac
done

shift $((OPTIND-1))


#[ "$1" = "--" ] && shift
if [ -z "$path_to_main_dir" ]; then
    echo "*****You must enter a path to main package directory****"
    display_help
    exit 1
fi
if [ -z "$config_fn_asset_fields" ]; then
    echo "*****You must enter a config file to load the asset fields****"
    display_help
    exit 1
fi
if [ -z "$python_path" ]; then
    echo "*****You must enter a path for python****"
    display_help
    exit 1
fi
if [ -z "$config_fn_master_dd" ]; then
    echo "*****You must enter a config file to load the master datadictionary****"
    display_help
    exit 1
fi
if [ -z "$npm_path" ]; then
    echo "*****You must enter a path for npm: try- npm bin -g****"
    display_help
    exit 1
fi

npm_path_to_package_json=$path_to_main_dir
config1="config/"
config_dir=$path_to_main_dir$config1
pydev="pydev/"
pydev_dir=
update_mm_fields="update_metadata_fields.py"
pydev_mm_fields=$path_to_main_dir$pydev$update_mm_fields
update_master_dd="update_master_data_dictionary.py"
pydev_update_master_dd=$path_to_main_dir$pydev$update_master_dd


#grab the data
$npm_path run --prefix $npm_path_to_package_json output_csvs
#update the metadata fields
$python_path $pydev_mm_fields -c $config_fn_asset_fields -d $config_dir
#update the master datadictionary
$python_path $pydev_update_master_dd -c $config_fn_master_dd -d $config_dir
