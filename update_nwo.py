#!/usr/bin/env python3


import argparse
import contextlib
import copy
import csv
import itertools
import glob
import json
import os
import pickle
import re
import requests
import shutil
import subprocess

import yaml
from logzero import logger
import ruamel.yaml
import git as pygit
from sh import git
from sh import find

from pprint import pprint

import requests_cache
requests_cache.install_cache('.cache/requests_cache')

ghrepos = [
    # network
    'https://github.com/ansible-network/ansible_collections.ansible.netcommon',
    'https://github.com/ansible-network/ansible_collections.cisco.iosxr',
    'https://github.com/ansible-network/ansible_collections.junipernetworks.junos',
    'https://github.com/ansible-network/ansible_collections.arista.eos',
    'https://github.com/ansible-network/ansible_collections.cisco.ios',
    'https://github.com/ansible-network/ansible_collections.vyos.vyos',
    'https://github.com/ansible-network/ansible_collections.network.netconf',
    'https://github.com/ansible-network/ansible_collections.network.cli',
    'https://github.com/ansible-network/ansible_collections.cisco.nxos',
    # community
    'https://github.com/ansible-collections/ansible_collections_netapp',
    'https://github.com/ansible-collections/grafana',
    'https://github.com/ansible-collections/ansible_collections_google',
    'https://github.com/ansible-collections/ansible_collections_azure',
    'https://github.com/ansible-collections/ibm_zos_ims',
    'https://github.com/ansible-collections/ibm_zos_core',
    # partners
    'https://github.com/Azure/AnsibleCollection',
    'https://github.com/ansible/ansible_collections_azure',
    #'https://github.com/ansible/ansible/tree/devel/lib/ansible/modules/network/aci',
    'https://github.com/F5Networks/f5-ansible',
    'https://github.com/ansible-network/ansible_collections.cisco.ios',
    'https://github.com/ansible-network/ansible_collections.cisco.iosxr',
    'https://github.com/ansible-network/ansible_collections.cisco.nxos',
    'https://github.com/aristanetworks/ansible-cvp',
    'https://github.com/ansible-security/ibm_qradar',
    'https://github.com/cyberark/ansible-security-automation-collection',
    'https://github.com/Paloaltonetworks/ansible-pan',
    'https://github.com/ansible/ansible_collections_netapp',
    'https://github.com/ansible/ansible_collections_google',
    'https://github.com/ansible-network/ansible_collections.juniper.junos',
    #'https://github.com/ansible/ansible/tree/devel/lib/ansible/modules/network/nso',
    'https://github.com/aruba/aruba-switch-ansible',
    'https://github.com/CiscoDevNet/ansible-dnac',
    'https://github.com/CiscoDevNet/ansible-viptela',
    'https://github.com/dynatrace-innovationlab/ansible-collection',
    'https://github.com/sensu/sensu-go-ansible',
    #'https://github.com/CheckPointSW/cpAnsibleModule',
    #'https://galaxy.ansible.com/frankshen01/testfortios',
    'https://github.com/ansible-security/SplunkEnterpriseSecurity',
    'https://github.com/ansible/ansible_collections_netapp',
    'https://github.com/ansible/ansible_collections_netapp',
    'https://github.com/dell/ansible-powermax',
    'https://github.com/ansible/ansible_collections_netapp',
    'https://github.com/rubrikinc/rubrik-modules-for-ansible',
    'https://github.com/HewlettPackard/oneview-ansible',
    'https://github.com/dell/dellemc-openmanage-ansible-modules',
    'https://github.com/dell/redfish-ansible-module',
    'https://github.com/nokia/sros-ansible',
    #'https://github.com/ansible/ansible/tree/devel/lib/ansible/modules/network/frr',
    'https://github.com/ansible-network/ansible_collections.vyos.vyos',
    'https://github.com/wtinetworkgear/wti-collection',
    'https://github.com/Tirasa/SyncopeAnsible',
    # redhat
    #   foreman/candlepin/etc
    # tower
    'https://opendev.org/openstack/ansible-collections-openstack'
]

partners = [
    ('ansible', 'netcommon'),
    ('awx', 'awx'),
    'azure',
    ('azure', 'azcollection'),
    ('gavinfish', 'azuretest'),
    'cisco',
    ('cyberark', 'bizdev'),
    'f5networks',
    'fortinet',
    ('frankshen01', 'testfortios'),
    'google',
    'netapp',
    ('netapp', 'ontap'),
    'netbox_community',
    ('openstack', 'cloud'),
    ('sensu', 'sensu_go'),
    'servicenow'
]

non_partners = [
    'chillancezen',
    'debops',
    'engineerakki',
    'jacklotusho',
    'kbreit',
    'lhoang2',
    'mattclay',
    'mnecas',
    'nttmcp',
    'rrey',
    'schmots1',
    'sh4d1',
    'testing'
]

def captured_return(result, **kwargs):
    #if 'filename' in kwargs and 'sumo' in kwargs['filename']:
    #    import epdb; epdb.st()
    return result



class NWO:

    SCENARIO = 'nwo'
    DUMPING_GROUND = ('community', 'general')

    collections = None
    plugins = None
    pluginfiles = None
    orphaned = None
    topics = None

    def __init__(self):

        self.scenario_output_dir = os.path.join('scenarios', self.SCENARIO + '.test')

        self.galaxyindexer = None
        self.cachefile = '.cache/nwo_status_quo.pickle'
        self.pluginfiles = []
        self.collections = {}
        self.url = 'https://github.com/ansible/ansible'
        self.checkouts_dir = '.cache/checkouts'
        self.checkout_dir = os.path.join(self.checkouts_dir, 'ansible')
        self.community_general_topics = None

        self.scenario_cache = {}

        self.rules = []

    def run(self, usecache=False, galaxy_indexer=None, base_scenario_file=None):

        if os.path.exists(self.scenario_output_dir):
            shutil.rmtree(self.scenario_output_dir)
        os.makedirs(self.scenario_output_dir)

        if galaxy_indexer:
            self.galaxy_indexer = galaxy_indexer

        self.map_existing_files_to_rules()
        self.manage_checkout()
        self.get_plugins()
        self.map_plugins_to_collections()

        self.make_compiled_csv()
        self.make_spec()


    def map_existing_files_to_rules(self):
        sfiles = glob.glob('scenarios/%s/*.yml' % self.SCENARIO) 
        sfiles = sorted(set(sfiles))

        for sfile in sfiles:
            with open(sfile, 'r') as f:
                ydata = yaml.load(f.read())
            namespace = os.path.basename(sfile).replace('.yml', '')
            self.scenario_cache[namespace] = copy.deepcopy(ydata)
            for name,plugins in ydata.items():

                if namespace == 'community' and name == 'general':
                    continue

                for ptype,pfiles in plugins.items():
                    for pfile in pfiles:
                        self.rules.append({
                            'plugin_type': ptype,
                            'matcher': pfile,
                            'namespace': namespace,
                            'name': name,
                            'source': sfile
                        })

    def manage_checkout(self):
        logger.info('manage ansible checkout for NWO updates')

        if not os.path.exists(self.checkouts_dir):
            os.makedirs(self.checkouts_dir)

        if not os.path.exists(self.checkout_dir):
            logger.info('git clone %s %s' % (self.url, self.checkout_dir))
            git.clone(self.url, self.checkout_dir)
        else:
            logger.info('git fetch -a')
            git.fetch('-a', _cwd=self.checkout_dir)
            logger.info('git pull --rebase')
            git.pull('--rebase', _cwd=self.checkout_dir)

    def _guess_collection(self, plugin_type=None, plugin_basename=None, plugin_relpath=None, plugin_filepath=None):

        ''' use rules to match a file to a namespace.name '''

        logger.debug(plugin_filepath)

        ppaths = plugin_relpath.split('/')
        matched_rules = []
        for rule in self.rules:

            # no need to process different plugin type
            if rule['plugin_type'] != plugin_type:
                continue

            # simplest match
            if rule['matcher'] == plugin_relpath:
                logger.debug('1')
                matched_rules.append(rule)
                continue

            # globs are FUN ...
            if '*' in rule['matcher']:
                mpaths = rule['matcher'].split('/')
                zipped = list(itertools.zip_longest(mpaths, ppaths))
                bad = [False for x in zipped if x[0] != x[1] and x[0] != '*']
                if not bad:
                    matched_rules.append(rule)
                    continue

        if len(matched_rules) == 1:
            return (matched_rules[0]['namespace'], matched_rules[0]['name'], matched_rules[0])        
        elif len(matched_rules) > 1:
            # use most specific match?
            for mr in matched_rules:
                mpaths = mr['matcher'].split('/')
                if len(mpaths) == len(ppaths):
                    return (mr['namespace'], mr['name'], mr)        

        return (
            self.DUMPING_GROUND[0],
            self.DUMPING_GROUND[1],
            {
                'plugin_type': plugin_type,
                'matcher': 'unclaimed!',
                'namespace': self.DUMPING_GROUND[0],
                'name': self.DUMPING_GROUND[1]
            }
        )

    def get_plugins(self):

        # enumerate the modules
        logger.info('iterating through modules')
        root = os.path.join(self.checkout_dir, 'lib', 'ansible', 'modules')
        for dirName, subdirList, fileList in os.walk(root):

            for fn in set(fileList) - {'__init__.py', 'loader.py'}:
                fp = os.path.join(dirName, fn)
                topic = None
                self.pluginfiles.append(['modules', fn, topic, fp])

        # enumerate the module utils
        logger.info('iterating through module utils')
        root = os.path.join(self.checkout_dir, 'lib', 'ansible', 'module_utils')
        for dirName, subdirList, fileList in os.walk(root):

            for fn in set(fileList) - {'__init__.py', 'loader.py'}:
                fp = os.path.join(dirName, fn)
                #topic = self._guess_topic(fp)
                topic = None
                self.pluginfiles.append(['module_utils', fn, topic, fp])

        # enumerate all the other plugins
        logger.info('examining other plugins')
        root = os.path.join(self.checkout_dir, 'lib', 'ansible', 'plugins')
        for dirName, subdirList, fileList in os.walk(root):

            for fn in set(fileList) - {'__init__.py', 'loader.py'}:
                ptype = os.path.basename(dirName)
                fp = os.path.join(dirName, fn)
                self.pluginfiles.append([ptype, fn, None, fp])

        '''
        # let's get rid of contrib too
        logger.info('looking at contrib scripts')
        root = os.path.join(self.checkout_dir, 'contrib', 'inventory')
        for dirName, subdirList, fileList in os.walk(root):
            ptype = 'scripts'
            for fn in fileList:
                fp = os.path.join(dirName, fn)
                bn = os.path.basename(fn).replace('.py', '').replace('.ini', '')
                #topic = self._guess_topic(fp)
                topic = None
                self.pluginfiles.append([ptype, fn, topic, fp])
        '''


    def map_plugins_to_collections(self):

        self.community_general_topics = set()
        self.topics = set()

        # enumerate the modules
        logger.info('iterating through modules')
        root = os.path.join(self.checkout_dir, 'lib', 'ansible', 'modules')
        for dirName, subdirList, fileList in os.walk(root):

            for fn in set(fileList) - {'__init__.py', 'loader.py'}:
                fp = os.path.join(dirName, fn)
                topic = os.path.relpath(fp, root)
                topic = os.path.dirname(topic)
                topic = topic.replace('/', '.')
                self.topics.add(topic)
                self.community_general_topics.add(topic)

        logger.info('matching %s files' % len(self.pluginfiles))
        for idp,plugin in enumerate(self.pluginfiles):
            filepath = plugin[3].replace(self.checkout_dir + '/', '')
            if '/plugins/' in plugin[3]:
                relpath = plugin[3].replace(self.checkout_dir + '/lib/ansible/plugins/%s/' % plugin[0], '')
            else:
                relpath = plugin[3].replace(self.checkout_dir + '/lib/ansible/%s/' % plugin[0], '')

            this_namespace, this_name, this_rule = self._guess_collection(
                plugin_type=plugin[0],
                plugin_basename=plugin[1],
                plugin_filepath=filepath,
                plugin_relpath=relpath,
            )
            self.pluginfiles[idp][2] = (this_namespace, this_name)
            self.pluginfiles[idp].append(this_rule)
            logger.debug('%s.%s:%s:%s > %s' % (this_namespace, this_name, this_rule['plugin_type'], this_rule['matcher'], plugin[3]))
        logger.info('files matching done')
        self.pluginfiles = sorted(self.pluginfiles, key=lambda x: x[3])

    def make_compiled_csv(self):

        fn = os.path.join(self.scenario_output_dir, 'compiled.csv')        
        with open(fn, 'w') as csvfile:
            spamwriter = csv.writer(csvfile)
            spamwriter.writerow([
                'filename',
                'fqn',
                'namespace',
                'name',
                'scenario_file',
                'scenario_plugin_type',
                'matched_line']
            )

            for pf in self.pluginfiles:
                row = [
                    pf[3].replace(self.checkout_dir+'/', ''),
                    '%s.%s' % (pf[2][0], pf[2][1]),
                    pf[2][0],
                    pf[2][1],
                    'scenarios/nwo/%s.yml' % pf[2][0],
                    pf[4]['plugin_type'],
                    pf[4]['matcher']
                ]
                if pf[2][0] == 'community' and pf[2][1] == 'general':
                    row[4] = 'unclaimed!'
                spamwriter.writerow(row)

    def make_spec(self):

        # make specfile ready dicts for each collection
        for idx,x in enumerate(self.pluginfiles):
            ns = x[-1]['namespace']
            name = x[-1]['name']
            ckey = (ns, name)
            ptype = x[0]
            matcher = x[-1]['matcher']            

            if ckey not in self.collections:
                self.collections[ckey] = {}

            if ptype not in self.collections[ckey]:
                self.collections[ckey][ptype] = []
            if matcher not in self.collections[ckey][ptype]:
                self.collections[ckey][ptype].append(matcher)

        # sort the filepaths in each plugin type
        for k,v in self.collections.items():
            for ptype, pfiles in v.items():
                self.collections[k][ptype] = sorted(pfiles)

        # squash all collections into their namespaces
        namespaces = {}
        for ckey, collection in self.collections.items():
            if ckey[0] not in namespaces:
                namespaces[ckey[0]] = {}
            namespaces[ckey[0]][ckey[1]] = copy.deepcopy(collection)

        # write each namespace as a separate file
        for namespace,collections in namespaces.items():
            fn = os.path.join(self.scenario_output_dir, namespace + '.yml')
            logger.info('write %s' % fn)
            with open(fn, 'w') as f:

                if namespace != 'community':
                    ruamel.yaml.dump(self.scenario_cache[namespace], f, Dumper=ruamel.yaml.RoundTripDumper)
                else:
                    this_data = copy.deepcopy(self.scenario_cache['community'])
                    this_data['general'] = collections['general']
                    ruamel.yaml.dump(this_data, f, Dumper=ruamel.yaml.RoundTripDumper)




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--usecache', action='store_true')
    args = parser.parse_args()

    nwo = NWO()
    nwo.run(usecache=args.usecache, galaxy_indexer=None)

