# -*- coding: utf-8 -*-
#  Copyright 2013 Takeshi KOMIYA
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os
import re
import json
import yaml
from glob import glob
from shutil import copyfile, copytree

from testing.common.database import (
    Database, SkipIfNotInstalledDecorator
)

try:
    from urllib.request import urlopen
except ImportError:
    from urllib import urlopen


__all__ = ['Elasticsearch', 'skipIfNotFound']

SEARCH_PATHS = ['/usr/share/elasticsearch']


class Elasticsearch(Database):
    DEFAULT_SETTINGS = dict(auto_start=2,
                            base_dir=None,
                            elasticsearch_home=None,
                            pid=None,
                            port=None,
                            copy_data_from=None,
                            boot_timeout=20)
    subdirectories = ['data', 'logs']

    def initialize(self):
        self.elasticsearch_home = self.settings.get('elasticsearch_home')
        if self.elasticsearch_home is None:
            self.elasticsearch_home = find_elasticsearch_home()

        user_config = self.settings.get('elasticsearch_yaml')
        elasticsearch_yaml_path = find_elasticsearch_yaml_path(self.elasticsearch_home)
        with open(os.path.realpath(elasticsearch_yaml_path)) as fd:
            self.elasticsearch_yaml = yaml.load(fd.read()) or {}
            self.elasticsearch_yaml['network.host'] = '127.0.0.1'
            self.elasticsearch_yaml['http.port'] = self.settings['port']
            self.elasticsearch_yaml['path.data'] = os.path.join(self.base_dir, 'data')
            self.elasticsearch_yaml['path.logs'] = os.path.join(self.base_dir, 'logs')
            self.elasticsearch_yaml['cluster.name'] = generate_cluster_name()
            self.elasticsearch_yaml['discovery.zen.ping.multicast.enabled'] = False

            if user_config:
                for key, value in user_config.items():
                    self.elasticsearch_yaml[key] = value

    def dsn(self, **kwargs):
        return {'hosts': ['127.0.0.1:%d' % self.elasticsearch_yaml['http.port']]}

    def get_data_directory(self):
        return os.path.join(self.base_dir, 'data')

    def initialize_database(self):
        # copy data files
        if self.settings['copy_data_from'] and self.elasticsearch_yaml['cluster.name']:
            indexdir = os.listdir(self.settings['copy_data_from'])[0]
            os.rename(os.path.join(self.base_dir, 'data', indexdir),
                      os.path.join(self.base_dir, 'data', self.elasticsearch_yaml['cluster.name']))

        # conf directory
        for filename in os.listdir(self.elasticsearch_home):
            srcpath = os.path.join(self.elasticsearch_home, filename)
            destpath = os.path.join(self.base_dir, filename)
            if not os.path.exists(destpath):
                if filename in ['lib', 'plugins']:
                    os.symlink(srcpath, destpath)
                elif filename == 'conf':
                    destpath = os.path.join(self.base_dir, 'config')
                    copytree(srcpath, destpath)
                elif os.path.isdir(srcpath):
                    copytree(srcpath, destpath)
                else:
                    copyfile(srcpath, destpath)

        elasticsearch_yaml_path = find_elasticsearch_yaml_path(self.elasticsearch_home)
        if not elasticsearch_yaml_path.startswith(self.elasticsearch_home):
            destpath = os.path.join(self.base_dir, 'config')
            copytree(os.path.dirname(elasticsearch_yaml_path), destpath)

        # rewrite elasticsearch.in.sh (for homebrew)
        with open(os.path.join(self.base_dir, 'bin', 'elasticsearch.in.sh'), 'r+t') as fd:
            body = re.sub('ES_HOME=.*', '', fd.read())
            fd.seek(0)
            fd.write(body)

    def prestart(self):
        super(Elasticsearch, self).prestart()

        # assign port to elasticsearch
        self.elasticsearch_yaml['http.port'] = self.settings['port']

        # generate cassandra.yaml
        with open(os.path.join(self.base_dir, 'config', 'elasticsearch.yml'), 'wt') as fd:
            fd.write(yaml.dump(self.elasticsearch_yaml, default_flow_style=False))

    def get_server_commandline(self):
        return [os.path.join(self.base_dir, 'bin', 'elasticsearch')]

    def is_server_available(self):
        try:
            url = 'http://127.0.0.1:%d/_cluster/health' % self.elasticsearch_yaml['http.port']
            ret = json.loads(urlopen(url).read().decode('utf-8'))
            if ret['status'] in ('green', 'yellow'):
                return True
            else:
                return False
        except Exception:
            return False


class ElasticsearchSkipIfNotInstalledDecorator(SkipIfNotInstalledDecorator):
    name = 'Elasticsearch'

    def search_server(self):
        find_elasticsearch_home()  # raise exception if not found


skipIfNotFound = skipIfNotInstalled = ElasticsearchSkipIfNotInstalledDecorator()


def strip_version(dir):
    m = re.search('(\d+)\.(\d+)\.(\d+)', dir)
    if m is None:
        return None
    else:
        return tuple([int(ver) for ver in m.groups()])


def find_elasticsearch_home():
    elasticsearch_home = os.environ.get('ES_HOME')
    if elasticsearch_home:
        elasticsearch_home = os.path.abspath(elasticsearch_home)
        if os.path.exists(os.path.join(elasticsearch_home, 'bin', 'elasticsearch')):
            return elasticsearch_home

    for path in SEARCH_PATHS:
        if os.path.exists(os.path.join(path, 'bin', 'elasticsearch')):
            return path

    # search newest elasticsearch-x.x.x directory
    globbed = (glob("/usr/local/*elasticsearch*") +
               glob("*elasticsearch*") +
               glob("/usr/local/Cellar/elasticsearch/*/libexec"))
    elasticsearch_dirs = [os.path.abspath(dir) for dir in globbed if os.path.isdir(dir)]
    if elasticsearch_dirs:
        return sorted(elasticsearch_dirs, key=strip_version)[-1]

    raise RuntimeError("could not find ES_HOME")


def find_elasticsearch_yaml_path(es_home):
    for path in (os.path.join(es_home, 'conf', 'elasticsearch.yml'),  # ubuntu
                 os.path.join(es_home, 'config', 'elasticsearch.yml'),  # official package
                 '/etc/elasticsearch/elasticsearch.yml'):  # travis
        if os.path.exists(path):
            return path

    raise RuntimeError("could not find elasticsearch.yml")


def generate_cluster_name():
    import string
    import random

    return ''.join([random.choice(string.ascii_letters) for i in range(6)])
