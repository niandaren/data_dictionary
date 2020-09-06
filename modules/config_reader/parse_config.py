#!/usr/bin/env python3
# coding: utf-8

from configparser import ConfigParser
import os



class ConfigReader(object):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "instance") or cls.instance is None:
            cls.instance = super().__new__(cls)

        return cls.instance

    def __init__(self):
        super(ConfigReader, self).__init__()
        file_name = "config"
        file_path = self.get_file_path(file_name)
        self.parser = ConfigParser()
        self.parser.read(file_path)
        self.sections = self.parser.sections()

    def get_file_path(self, file_name):
        p = os.path.realpath(__file__)
        d = os.path.dirname(p)
        file_path = os.path.dirname(os.path.dirname(d)) + '/configs/' + file_name + ".ini"

        return file_path

    def get_section_config(self, section):
        if section not in self.sections:
            return None

        config_info = dict(self.parser.items(section))

        return config_info



if __name__ == "__main__":
    config_reader_obj = ConfigReader()
    config_info = config_reader_obj.get_section_config("cassandra")
    print(config_info)