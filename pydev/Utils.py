# coding: utf-8
#!/usr/bin/env python

#util classes
import pandas as pd
import logging

class pyLogger:
    def __init__(self, configItems):
        self.logfn = configItems['exception_logfile']
        self.log_dir = configItems['log_dir']
        self.logfile_fullpath = self.log_dir+self.logfn

    def setConfig(self):
        #open a file to clear log
        fo = open(self.logfile_fullpath, "w")
        fo.close
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logger = logging.getLogger("thisApp")
        logger.setLevel(logging.WARN)
        # create the logging file handler
        fh = logging.FileHandler("self.logfile_fullpath")
        #formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        # add handler to logger object
        logger.addHandler(fh)
        return logger


class PandasUtils:
  @staticmethod
  def loadCsv(fullpath):
    df = None
    try:
      df = pd.read_csv(fullpath)
    except Exception, e:
      print str(e)
    return df

  @staticmethod
  def convertDfToDictrows(df):
    return df.to_dict(orient='records')

  @staticmethod
  def mapFieldNames(df, field_mapping_dict):
    return df.rename(columns=field_mapping_dict)

  @staticmethod
  def groupbyCountStar(df, group_by_list):
    return df.groupby(group_by_list).size().reset_index(name='count')


if __name__ == "__main__":
    main()

