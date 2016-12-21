
# coding: utf-8
#!/usr/bin/env python

#classes to do specifics transforms on the insert dataset

import re

class UpdateSocrataFields:

  @staticmethod
  def addIsDataDictBool(insertDataSet):
    '''adds a boolean field to flag attachments that are data dictionarys'''
    dataDictRegex = re.compile('datadict', re.IGNORECASE)
    newInsertDataSet = []
    for row in insertDataSet:
      if dataDictRegex.findall(row['attachment_name']):
        row['data_dictionary_attached'] = True
      else:
        row['data_dictionary_attached'] = False
      newInsertDataSet.append(row)
    return newInsertDataSet

if __name__ == "__main__":
    main()
