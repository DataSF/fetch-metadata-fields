# coding: utf-8
#!/usr/bin/env python

#util classes
import csv
import time
import datetime
import logging
import os
import csv, codecs, cStringIO
import math
from io import BytesIO
import shutil
from csv import DictWriter
from cStringIO import StringIO
import json
import os.path
import collections
import shutil


class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """
    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


class WkbkJson:

    @staticmethod
    def loadJsonFile(path_to_file, json_fn):
        json_obj = {}
        if os.path.isfile( path_to_file + json_fn):
            json_data = open(path_to_file + json_fn).read()
            json_obj =  json.loads(json_data)
        return json_obj


    @staticmethod
    def write_json_object(json_object, output_dir, json_fn):
        wroteFile = False
        try:
            json_object = EncodeObjects.convertToUTF8(json_object)
            with open(output_dir + json_fn, 'w') as f:
                json.dump(json_object, f, ensure_ascii=False)
                wroteFile = True
        except Exception, e:
            print str(e)
        return wroteFile


class EncodeObjects:

    @staticmethod
    def convertToString(data):
        '''converts unicode to string'''
        if isinstance(data, basestring):
            return str(data)
        elif isinstance(data, collections.Mapping):
            return dict(map(EncodeObjects.convertToString, data.iteritems()))
        elif isinstance(data, collections.Iterable):
            return type(data)(map(EncodeObjects.convertToString, data))
        else:
            return data

    @staticmethod
    def convertToUTF8(data):
        '''converts unicode to string'''
        if isinstance(data, basestring):
            return data.encode('utf-8')
        elif isinstance(data, collections.Mapping):
            return dict(map(EncodeObjects.convertToUTF8, data.iteritems()))
        elif isinstance(data, collections.Iterable):
            return type(data)(map(EncodeObjects.convertToUTF8, data))
        else:
            return data





if __name__ == "__main__":
    main()

