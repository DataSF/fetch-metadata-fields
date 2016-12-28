# coding: utf-8
#!/usr/bin/env python


import yaml


class ConfigItems:
    def __init__(self, inputdir, fieldConfigFile):
        self.inputdir = inputdir
        self.fieldConfigFile = fieldConfigFile

    def getConfigs(self):
        configItems = 0
        with open(self.inputdir + self.fieldConfigFile ,  'r') as stream:
            try:
                configItems = yaml.load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        return configItems

    @staticmethod
    def getConfigsStatic(inputdir, fieldConfigFile):
        configItems = 0
        with open(inputdir + fieldConfigFile ,  'r') as stream:
            try:
                configItems = yaml.load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        return configItems

if __name__ == "__main__":
    main()
