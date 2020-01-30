#!/usr/bin/env python3

# generate_status_quo.py - directly map current module folders to collections
#
# examples:
#   cloud/amazon -> cloud.amazon
#   packaging/os -> packaging.os
#
# Any plugins which can not be easily mapped will end up in an _orphaned file.
# Ideally, -you- munge this script until no files are orphaned.


import argparse
import contextlib
import copy
import csv
import glob
import json
import os
import pickle
import re
import requests
import shutil
import subprocess
import tarfile
import zipfile

import yaml
from logzero import logger
import ruamel.yaml
import git as pygit
from sh import git
from sh import find

from pprint import pprint

import requests_cache
requests_cache.install_cache('.cache/requests_cache')


def captured_return(result, **kwargs):
    #if 'filename' in kwargs and 'sumo' in kwargs['filename']:
    #    import epdb; epdb.st()
    return result



class StatusQuo:

    SCENARIO = 'nwo'
    DUMPING_GROUND = 'community.general'

    collections = None
    plugins = None
    pluginfiles = None
    orphaned = None
    topics = None

    static_namespaces = [
        #'community.extras',
        #'monitoring.logstash',
        #'monitoring.logdna',
        #'monitoring.nagios',
        #'monitoring.sumologic',
    ]

    '''
    static_mappings = {
        'logdna': 'monitoring.misc',
        'logstash': 'monitoring.misc',
        'sumologic': 'monitoring.misc',
        'nrdp': 'monitoring.misc',
    }
    '''

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

    synonyms = {
        'alicloud_ecs': 'alicloud',
        'apache-libcloud': 'cloud.misc',
        'aws': 'amazon',
        #'azure_rm': 'azure',
        'bigip': 'f5',
        'bigiq': 'f5',
        'buildah': 'podman',
        #'ce': 'cloudengine',
        'checkpoint': 'check_point',
        'cloudforms': 'cloud',
        #'cloudstack': 'cloud.cloudstack',
        #'consul': 'clustering.consul',
        'conjur': 'cloud',
        'credstash': 'cloud',
        'csharp': 'windows',
        'cyberarkpassword': 'cyberark',
        'dig': 'net_tools',
        'dnstext': 'net_tools',
        'ec2': 'amazon',
        #'ecs': 'crypto.entrust',
        'etcd': 'cloud',
        #'kube': 'k8s',
        #'foreman': 'remote_management.foreman',
        'fortios': 'fortinet',
        'spacewalk': 'remote_management',
        'f5_utils': 'f5',
        'gcdns': 'google',
        'gce': 'google',
        'gcp_compute': 'google',
        'gcp': 'google',
        'hashi_vault': 'identity',
        'chef_databag': 'identity',
        'lastpass': 'identity',
        #'logdna': 'monitoring.foo',
        'logstash': 'monitoring.logstash',
        #'sumologic': 'monitoring.foo',
        'nrdp': 'nagios',
        #'logdna.py': 'monitoring.misc',
        'hetzner': 'hcloud',
        'hiera': 'puppet',
        'hwc': 'huawei',
        'httpapi': 'network',
        #'jabber': 'web_infrastructure',
        'infinibox': 'infinidat',
        #'infoblox': 'net_tools.nios',
        'infoblox': 'nios',
        #'ipa': 'identity.ipa',
        'ipaddr': 'network',
        'kubectl': 'k8s',
        'libcloud': 'cloud.misc',
        'libvirt': 'cloud.misc',
        #'linode': 'cloud.linode',
        'logstash': 'monitoring.logstash',
        #'lxc': 'cloud.lxc',
        #'lxd': 'cloud.lxc',

        #'mso': 'network.aci',
        'mso': 'aci',
        #'nagios': 'monitoring.nagios',
        #'nagios': 'web_infrastructure',
        'nrdp': 'monitoring.nagios',
        'napalm': 'network',
        'oc': 'k8s',
        #'openstack': 'cloud.openstack',
        #'openshift': 'clustering.openshift',
        'openswitch': 'opx',
        #'openvz': 'cloud.misc',
        #'ovirt': 'cloud.ovirt',
        'onepassword': 'identity',

        #'package': 'packaging',
        'package': 'packaging.os',
        'paramiko': 'network',
        'passwordstore': 'identity',

        'postgres': 'postgresql',
        'powershell': 'windows',
        #'proxmox': 'cloud.misc',
        'psrp': 'windows',
        'qubes': 'cloud',
        'rax': 'cloud.rackspace',
        #'redis': 'database.misc',
        'rhv': 'ovirt',
        'sumologic': 'monitoring.sumologic',
        'tower': 'ansible_tower',
        #'utm': 'sophos_utm',
        'vagrant': 'cloud',
        'vca': 'vmware',
        'virtualbox': 'cloud',
        #'vbox': 'cloud.misc',

        'win': 'windows',
        'win': 'windows',

        'Vmware': 'vmware',
        'vmware': 'cloud.vmware',
        #'yum': 'packaging.os',
        #'zabbix': 'monitoring.zabbix'

        'yumdnf': 'packaging.os',
    }

    extras_synonyms = {
        # connection
        'chroot': 'community.general',
        'funcd': 'community.general',
        'jail': 'community.general',
        'saltstack': 'community.general',
        'zone': 'community.general',
        'json_query': 'community.general',
        # callback
        'actionable': 'community.general',
        'cgroup_memory_recap': 'community.general',
        'cgroup_perf_recap': 'community.general',
        'context_demo': 'community.general',
        'counter_enabled': 'community.general',
        'dense': 'community.general',
        'full_skip': 'community.general',
        'json': 'community.general',
        'junit': 'community.general',
        'log_plays': 'community.general',
        'minimal': 'community.general',
        'null': 'community.general',
        'oneline': 'community.general',
        'profile_roles': 'community.general',
        'profile_tasks': 'community.general',
        'selective': 'community.general',
        'skippy': 'community.general',
        'stderr': 'community.general',
        'syslog_json': 'community.general',
        'timer': 'community.general',
        'tree': 'community.general',
        'unixy': 'community.general',
        # become
        'doas': 'community.general',
        'dzdo': 'community.general',
        'enable': 'community.general',
        'ksu': 'community.general',
        'machinectl': 'community.general',
        'pbrun': 'community.general',
        'pfexec': 'community.general',
        'pmrun': 'community.general',
        'runas': 'community.general',
        'sesu': 'community.general',
        # strategy
        'host_pinned': 'community.general',
        # lookup
        'filetree': 'community.general',
        'laps_password': 'community.general',
        'lmdb_kv': 'community.general',
        'manifold': 'community.general',
        'password': 'community.general',
        'shelvefile': 'community.general',
        # shell
        'csh': 'community.general',
        'fish': 'community.general',
    }

    def __init__(self):

        self.galaxyindexer = None
        self.cachefile = '.cache/nwo_status_quo.pickle'
        self.pluginfiles = []
        self.collections = {}
        self.url = 'https://github.com/ansible/ansible'
        self.checkouts_dir = '.cache/checkouts'
        self.checkout_dir = os.path.join(self.checkouts_dir, 'ansible')

        self.base_scenario_file = None
        self.base_scenario = None
        self.community_general_topics = None

        self.synonyms.update(self.extras_synonyms)

    def run(self, usecache=False, galaxy_indexer=None, base_scenario_file=None):

        if galaxy_indexer:
            self.galaxy_indexer = galaxy_indexer

        if base_scenario_file:
            self.base_scenario_file = base_scenario_file

        if usecache and self.cache_exists:
            self.load_cache()
        else:
            self.manage_checkout()
            self.get_plugins()
            self.map_base_scenario()
            self.map_plugins_topics()
            if usecache:
                self.save_cache()

        self.make_spec()

    @property
    def cache_exists(self):
        if os.path.exists(self.cachefile):
            return True
        return False

    def load_cache(self):
        logger.info('loading cache from %s' % self.cachefile)
        with open(self.cachefile, 'rb') as f:
            pdata = pickle.load(f)

        self.pluginfiles = pdata['orphaned']
        self.pluginfiles = pdata['pluginfiles']
        self.collections = pdata['collections']

    def save_cache(self):
        logger.info('saving cache to %s' % self.cachefile)
        pdata = {
            'orphaned': self.orphaned,
            'pluginfiles': self.pluginfiles,
            'collections': self.collections
        }
        with open(self.cachefile, 'wb') as f:
            pickle.dump(pdata, f, pickle.HIGHEST_PROTOCOL)

    def manage_checkout(self):
        logger.info('manage ansible checkout for statusquo')

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

    def _guess_topic(self, filename):

        if self.in_base(filename):
            return 'ansible._core'

        return None

        # hardcode nxos
        if 'nxos' in filename:
            return 'network.nxos'

        bn = os.path.basename(filename)
        bn = bn.replace('.py', '').replace('.ini', '')

        # workaround for everything depending on aws
        if bn == 'core' and 'aws' not in filename:
            #return 'utilities.misc'
            return captured_return('utilities.misc', filename=filename)

        # workaround for mu/database.py ending up in aerospike
        if bn == 'database':
            return 'database.misc'

        # don't lump all facts into network
        if '/facts/' in filename and 'module_utils/network' not in filename:
            #return None
            return captured_return(None, filename=filename)

        # does the filepath contain a topic?
        paths = filename.replace(self.checkout_dir + '/', '')
        paths = paths.replace('lib/ansible/', '')
        paths = paths.split('/')
        paths = paths[1:]

        for ltup in zip(paths, paths[1:]):
            thistopic = '.'.join(ltup)
            if thistopic in self.topics:
                logger.debug('A. %s --> %s' % (filename, thistopic))
                #return thistopic
                return captured_return(thistopic, filename=filename)

        # fill in topics via synonyms
        for idx,x in enumerate(self.pluginfiles):

            if not x[2]:
                continue

            for a,b in self.synonyms.items():
                if a in bn:

                    if not '.' in x[2] and b == x[2]:
                        logger.debug('I. %s --> %s' % (filename, x[2]))
                        #return x[2]
                        return captured_return(x[2], filename=filename)
                    elif x[2].endswith(bn):
                        logger.debug('J. %s --> %s' % (filename, x[2]))
                        #return x[2]
                        return captured_return(x[2], filename=filename)
                    elif os.path.dirname(x[-1]).endswith(b):
                        logger.debug('K. %s --> %s' % (filename, x[2]))
                        #return x[2]
                        return captured_return(x[2], filename=filename)

        # match on similar filenames
        if bn not in ['common']:
            for pf in self.pluginfiles:
                if not pf[2]:
                    continue
                if os.path.basename(pf[-1]).replace('.py', '') == bn:
                    logger.debug('B. %s --> %s' % (filename, pf[2]))
                    #if 'fortios' in filename:
                    #    import epdb; epdb.st()
                    #return pf[2]
                    return captured_return(pf[2], filename=filename)

        # match basename to similar dirname
        for pf in self.pluginfiles:
            if not pf[2]:
                continue
            xdn = os.path.basename(os.path.dirname(pf[-1]))
            if xdn == bn:
                logger.debug('C. %s --> %s' % (filename, pf[2]))
                #if 'fortios' in filename:
                #    import epdb; epdb.st()
                #return pf[2]
                return captured_return(pf[2], filename=filename)

        # use path segments to match
        fparts = filename.split('/')
        fparts[-1] = fparts[-1].split('.')[0]
        for part in fparts[::-1]:
            if part in self.topics:
                logger.debug('D. %s --> %s' % (filename, pf[2]))
                #return part
                return captured_return(part, filename=filename)

        for part in fparts[::-1]:
            for topic in self.topics:
                if not '.' in topic:
                    continue
                if topic.startswith(part + '.') or topic.endswith('.' + part):
                    logger.debug('E. %s --> %s' % (filename, pf[2]))
                    #return topic
                    return captured_return(topic, filename=filename)

        for part in fparts[::-1]:
            if part in self.synonyms:
                syn = self.synonyms[part]
                if syn in self.topics:
                    logger.debug('F. %s --> %s' % (filename, pf[2]))
                    #return syn
                    return captured_return(syn, filename=filename)

        for part in fparts[::-1]:
            if part in self.synonyms:
                syn = self.synonyms[part]
                for topic in self.topics:
                    if not '.' in topic:
                        continue
                    if topic.startswith(syn + '.') or topic.endswith('.' + syn):
                        logger.debug('G. %s --> %s' % (filename, pf[2]))
                        #return topic
                        return captured_return(syn, filename=filename)

        # is this a _ delimited name?
        if '_' in bn:
            _bn = bn.split('_')[0]
            for pf in self.pluginfiles:
                if not pf[2]:
                    continue
                xbn = os.path.basename(pf[-1]).replace('.py', '').replace('.ini', '').replace('.yml', '')
                if '_' in xbn:
                    xbn = xbn.split('_')[0]
                if xbn == _bn:
                    logger.debug('H. %s --> %s' % (filename, pf[2]))
                    #return pf[2]
                    return captured_return(pf[2], filename=filename)

        # fill in topics via synonyms
        for idx,x in enumerate(self.pluginfiles):

            if not x[2]:
                continue

            for a,b in self.synonyms.items():
                if a in bn:

                    if not '.' in x[2] and b == x[2]:
                        logger.debug('I. %s --> %s' % (filename, x[2]))
                        #return x[2]
                        return captured_return(x[2], filename=filename)
                    elif x[2].endswith(bn):
                        logger.debug('J. %s --> %s' % (filename, x[2]))
                        #return x[2]
                        return captured_return(x[2], filename=filename)
                    elif os.path.dirname(x[-1]).endswith(b):
                        logger.debug('K. %s --> %s' % (filename, x[2]))
                        #return x[2]
                        return captured_return(x[2], filename=filename)

                    xdn = x[-1].replace(self.checkout_dir + '/', '')
                    if b in xdn:
                        logger.debug('L. %s --> %s' % (filename, x[2]))
                        if 'sumo' in filename:
                            import epdb; epdb.st()
                        #return x[2]
                        return captured_return(x[2], filename=filename)

                    #import epdb; epdb.st()

        return None


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

    def map_base_scenario(self):
        if not self.base_scenario_file:
            return

        with open(self.base_scenario_file, 'r') as f:
            self.base_scenario = yaml.load(f.read())

    def in_base(self, filename, plugin_type=None):

        if '/contrib/' in filename:
            return False

        trimmed = None
        try:
            if plugin_type:
                #trimmed = filename.index('lib/ansible/%s/' % plugin_type)
                #trimmed = filename[trimmed+12+len(plugin_type)+1:]
                trimmed = filename.index(plugin_type + '/')
                trimmed = filename[trimmed+len(plugin_type)+1:]
            else:
                trimmed = filename.index('lib/ansible/')
                trimmed = filename[trimmed+12:]
        except ValueError as e:
            print(e)
            print(plugin_type)
            print(filename)
            #import epdb; epdb.st()
            pass

        if trimmed and plugin_type and trimmed in self.base_scenario['_core'].get(plugin_type, []):
            return True

        #import epdb; epdb.st()
        #if 'add_host' in filename:
        #    import epdb; epdb.st()

        if plugin_type:
            if os.path.basename(filename) in self.base_scenario['_core'].get(plugin_type, []):
                return True
            for pfile in self.base_scenario['_core'].get(plugin_type, []):
                if pfile.endswith('/*') and trimmed:
                    checkme = pfile.replace('*', '')
                    if trimmed.startswith(checkme):
                        return True
            return False
        else:
            for ptype,plugins in self.base_scenario['_core'].items():
                for plugin in plugins:
                    if os.path.basename(filename) == plugin:
                        return True

        return False


    def map_plugins_topics(self):

        self.community_general_topics = set()

        self.topics = set()
        for ns in self.static_namespaces:
            self.topics.add(ns)

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

        # check base
        for idp,plugin in enumerate(self.pluginfiles):
            if self.in_base(plugin[-1], plugin_type=plugin[0]):
                self.pluginfiles[idp][2] = 'ansible._core'

        # check galaxy collections
        if self.galaxy_indexer:
            logger.info('mapping topics from galaxyindexer')
            for idp,plugin in enumerate(self.pluginfiles):
                if plugin[2] == 'ansible._core':
                    continue
                if plugin[1] in ['ping.py', 'user.py', 'base.py']:
                    continue
                res = self.galaxy_indexer.provides_plugin(plugin[1], plugin_type=plugin[0], exact=True)

                '''
                if 'na_ontap_svm' in plugin[-1]:
                    print(plugin)
                    pprint(res)
                    import epdb; epdb.st()
                '''

                if res:

                    candidates = list(res.keys())
                    candidates = [x for x in candidates if x not in self.non_partners and x[0] not in self.non_partners]
                    if not candidates:
                        continue

                    for ckey in candidates:
                        topic = '%s.%s' % ckey
                        self.topics.add(ckey)

                    if len(candidates) == 1:
                        ckey = candidates[0]
                        topic = '%s.%s' % (ckey[0], ckey[1])
                        self.pluginfiles[idp][2] = topic
                    else:
                        candidates = [x for x in candidates if x in self.partners or x[0] in self.partners]
                        if len(candidates) == 1:
                            ckey = candidates[0]
                            topic = '%s.%s' % (ckey[0], ckey[1])
                            self.pluginfiles[idp][2] = topic
                        else:
                            ckey = sorted(candidates)[0]
                            topic = '%s.%s' % (ckey[0], ckey[1])
                            self.pluginfiles[idp][2] = topic
                            #pprint(res)
                            #import epdb; epdb.st()

        logger.info('creating *.misc namespaces')
        for idx,x in enumerate(self.pluginfiles):
            if x[2]:
                continue
            topic = self._guess_topic(x[-1])
            if topic and topic.endswith('.misc'):
                self.pluginfiles[idx][2] = topic

        # guess the rest 
        logger.info('guessing all the lefovers')
        for idx,x in enumerate(self.pluginfiles):
            if x[2]:
                continue
            
            # this screws everything up ...
            if x[-1] == 'common.py':
                continue

            topic = self._guess_topic(x[-1])
            self.pluginfiles[idx][2] = topic

            #if 'ec2.ini' in x:
            #    import epdb; epdb.st()

        # find which modules use orphaned doc fragments
        logger.info('hashing doc fragments')
        for idx,x in enumerate(self.pluginfiles):
            if x[2]:
                continue
            if x[0] != 'doc_fragments':
                continue
            df = os.path.basename(x[-1])
            df = df.replace('.py', '')
            cmd = 'find %s -type f | xargs fgrep -iH %s' % \
                (os.path.join(self.checkout_dir, 'lib', 'ansible', 'modules'), df)
            with contextlib.suppress(subprocess.CalledProcessError):
                filenames = subprocess.check_output(cmd, shell=True, text=True).split('\n')
                filenames = [x.split(':')[0] for x in filenames if x]
                filenames = sorted(set(filenames))
                dirnames = [os.path.dirname(x) for x in filenames]
                dirnames = [x.replace(os.path.join(self.checkout_dir, 'lib', 'ansible', 'modules') + '/', '') for x in dirnames]
                dirnames = sorted(set(dirnames))
                logger.info('%s dirs' % len(dirnames))

        self.orphaned = [x for x in self.pluginfiles if not x[-2]]

    def make_spec(self):
        '''
        topics = list(self.topics)[:]
        for idx,x in enumerate(topics):
            if not '.' in x:
                topics[idx] = x + '.misc'
        '''

        topics = sorted(set([x[2] for x in self.pluginfiles if x[2]]))
        self.collections['_core'] = self.base_scenario['_core']
        self.collections[self.DUMPING_GROUND] = {}
        for topic in topics:
            if topic in self.community_general_topics:
                continue
            self.collections[topic] = {}

        for idx,x in enumerate(self.pluginfiles):
            if self.in_base(x[-1]):
                topic = 'ansible._core'
            else:
                topic = x[2]
                if topic is None or topic in self.community_general_topics:
                    topic = self.DUMPING_GROUND
            ptype = x[0]
            if not '.' in topic:
                topic = topic + '.misc'
            if topic not in self.collections:
                self.collections[topic] = {}
            if ptype not in self.collections[topic]:
                self.collections[topic][ptype] = []

            #if 'yum.py' in x[-1]:
            #    import epdb; epdb.st()

            spec_path = x[3].replace(os.path.join(self.checkout_dir, 'lib', 'ansible'), '')
            spec_path = spec_path.lstrip('/')
            if spec_path.startswith('modules/'):
                spec_path = spec_path.replace('modules/', '', 1)
            elif spec_path.startswith('module_utils/'):
                spec_path = spec_path.replace('module_utils/', '', 1)
            elif spec_path.startswith('plugins/'):
                spec_path = spec_path.replace('plugins/', '', 1)
                pos = spec_path.index('/') + 1
                spec_path = spec_path[pos:]
            elif 'contrib/inventory' in spec_path:
                spec_path = os.path.join('contrib', 'inventory', os.path.basename(spec_path))
            else:
                #import epdb; epdb.st()
                continue
            
            self.collections[topic][ptype].append(spec_path)

        #self.collections['_orphaned'] = sorted(self.collections['_orphaned'])
        for k,v in self.collections.items():
            #if k == '_orphaned':
            #    continue
            for ptype, pfiles in v.items():
                self.collections[k][ptype] = sorted(pfiles)

        sdir = os.path.join('scenarios', self.SCENARIO)
        if os.path.exists(sdir):
            shutil.rmtree(sdir)
        os.makedirs(sdir)

        namespaces = {}
        keys = self.collections.keys()
        keys = sorted(keys)
        #for k,v in self.collections.items():
        for k in keys:
            v = copy.deepcopy(self.collections[k])
            if '.' not in k:
                continue
            npaths = k.split('.')

            # network.nxos.storage
            if len(npaths) > 2:
                namespace = k.split('.')[0]
                name = k.split('.')[1]
                if namespace not in namespaces:
                    namespaces[namespace] = {}
                nkeys = list(v.keys())
                for nkey in nkeys:
                    if nkey not in namespaces[namespace][name]:
                        namespaces[namespace][name][nkey] = copy.deepcopy(v[nkey])
                    else:
                        namespaces[namespace][name][nkey] = sorted(set( namespaces[namespace][name][nkey] +  copy.deepcopy(v[nkey])))
            else:
                namespace = k.split('.')[0]
                name = k.split('.')[1]
                if namespace not in namespaces:
                    namespaces[namespace] = {}
                namespaces[namespace][name] = copy.deepcopy(v)

        for namespace,names in namespaces.items():

            if namespace == 'nttmcp':
                import epdb; epdb.st()

            fn = os.path.join(sdir, namespace + '.yml')
            with open(fn, 'w') as f:
                ruamel.yaml.dump(names, f, Dumper=ruamel.yaml.RoundTripDumper)

            # comment out the contrib scripts until migrate.py can support them
            has_scripts = [x.get('scripts') for x in names.values()]
            has_scripts = [x for x in has_scripts if x]
            if has_scripts:
                logger.info('commenting out scripts in %s' % namespace)
                with open(fn, 'r') as f:
                    fdata = f.read()
                fdata = fdata.replace('scripts:', '#scripts:')
                fdata = fdata.replace('- contrib/inventory', '#- contrib/inventory')
                os.remove(fn)
                with open(fn, 'w') as f:
                    f.write(fdata)

            # do the same for the stupid openshift files
            if namespace == 'clustering':
                logger.info('commenting out openshift dead links in %s' % namespace)
                with open(fn, 'r') as f:
                    lines = f.readlines()
                for idx,x in enumerate(lines):
                    if 'modules:' in x and ('/_oc' in lines[idx+1] or '/_openshift' in lines[idx+1]):
                        lines[idx] = x.replace('modules:', '#modules:')
                        continue
                    if '/_oc' in x or '/_openshift' in x:
                        lines[idx] = x.replace('-', '#-')
                        continue
                os.remove(fn)
                with open(fn, 'w') as f:
                    f.write(''.join(lines))

        # write out a csv for human readability
        rows = [['filename', 'namespace', 'name', 'source_of_truth']]
        for namespace,collections in namespaces.items():
            for name,files in collections.items():
                for plugin_type,plugins in files.items():
                    for plugin in plugins:
                        src = None
                        ckey = (namespace, name)
                        if ckey in self.galaxy_indexer.collections:
                            src = self.galaxy_indexer.collections[ckey].get('href')
                        if namespace == 'ansible' and name == '_core':
                            src = 'scenarios/bcs'
                        if namespace == 'community' and name == 'general':
                            src = 'unclaimed'
                        rows.append([os.path.join(plugin_type, plugin), namespace, name, src])
        rows = sorted(rows, key=lambda x: x[0])
        rows = [['filename', 'namespace', 'name', 'source_of_truth']] + rows
        with open(os.path.join(sdir, 'compiled.csv'), 'w') as csvfile:
            spamwriter = csv.writer(csvfile)
            for row in rows:
                spamwriter.writerow(row)
        #import epdb; epdb.st()


class GalaxyIndexer:
    def __init__(self):
        self.cachedir = '.cache/galaxy'
        self.tars_path = os.path.join(self.cachedir, 'tars')
        self.checkouts_path = os.path.join(self.cachedir, 'checkouts')
        self.collections_path = os.path.join(self.cachedir, 'collections')
        self.collections = {}

    def run(self, usecache=False):
        if not os.path.exists(self.cachedir):
            os.makedirs(self.cachedir)
        if not os.path.exists(self.tars_path):
            os.makedirs(self.tars_path)
        if not os.path.exists(self.checkouts_path):
            os.makedirs(self.checkouts_path)
        if not os.path.exists(self.collections_path):
            os.makedirs(self.collections_path)
        self.get_remote_collections_info(usecache=usecache)
        self.fetch_remote_collections(usecache=usecache)

    def get_remote_collections_info(self, usecache=False):
        baseurl = 'https://galaxy.ansible.com'
        starturl = baseurl + '/api/v2/collections/'
        logger.debug(starturl)
        rr = requests.get(starturl)
        jdata = rr.json()
        for collection in jdata['results']:
            fqn = (collection['namespace']['name'], collection['name'])
            self.collections[fqn] = collection

        next_page = jdata.get('next')
        while next_page:
            this_url = baseurl + next_page
            logger.debug(this_url)
            rr = requests.get(this_url)
            jdata = rr.json()
            next_page = jdata.get('next')
            for collection in jdata['results']:
                fqn = (collection['namespace']['name'], collection['name'])
                self.collections[fqn] = collection

    def fetch_remote_collections(self, usecache=False):

        ckeys = sorted(list(self.collections.keys()))
        for ckey in ckeys:
            latest = self.collections[ckey]['latest_version']['version']
            tarurl = 'https://galaxy.ansible.com/download/' + \
                ckey[0] + '-' + ckey[1] + '-' + latest + '.tar.gz'
            tarbn = os.path.basename(tarurl)
            tarfn = os.path.join(self.tars_path, tarbn)

            # download
            if not os.path.exists(tarfn):
                logger.debug(tarurl)
                rr = requests.get(tarurl, stream=True)
                with open(tarfn, 'wb') as f:
                    f.write(rr.raw.read())
            
            # extract
            efp = os.path.join(self.collections_path, ckey[0], ckey[1])
            if not os.path.exists(efp):
                logger.debug('unzip %s' % efp)
                namespace_path = os.path.dirname(efp)
                if not os.path.exists(namespace_path):
                    os.makedirs(namespace_path)                
                with tarfile.open(tarfn, 'r:gz') as f:
                    f.extractall(path=efp)
            self.collections[ckey]['filepath'] = efp

            self.index_plugins_in_collection(efp, ckey)

    def index_plugins_in_collection(self, path, fqn):
        efp = os.path.join(path)
        logger.debug('index %s' % efp)
        self.collections[fqn]['plugins'] = {}
        pluginfiles = glob.glob('%s/plugins/*/*' % efp)
        for pf in pluginfiles:
            ptype = pf.replace(efp+'/plugins/', '')
            ptype = ptype.split('/')[0]
            if ptype not in self.collections[fqn]['plugins']:
                self.collections[fqn]['plugins'][ptype] = set()
            bn = os.path.basename(pf)
            if bn == '__init__.py':
                continue
            if os.path.isdir(pf):
                pglob = glob.glob('%s/*' % pf)
                for pfg in pglob:
                    if os.path.basename(pfg) == '__init__.py':
                        continue
                    pfg = pfg.replace(os.path.dirname(pf) + '/', '')
                    self.collections[fqn]['plugins'][ptype].add(pfg)
            else:
                self.collections[fqn]['plugins'][ptype].add(os.path.basename(pf))

        logger.debug('%s modules in %s.%s' % (len(self.collections[fqn]['plugins'].get('modules', [])), fqn[0], fqn[1]))

        #if 'netapp/ontap' in path.lower():
        #    print(path)
        #    import epdb; epdb.st()

    def fetch_git_repo(self, repo):
        cp = os.path.join(self.checkouts_path, os.path.basename(repo))
        if not os.path.exists(cp):
            logger.debug('clone %s' % repo)
            pygit.Repo.clone_from(repo, cp)

        # find galaxy.yml file(s)
        res = find(cp, '-name', 'galaxy.yml')
        galaxy_files = res.stdout.decode('utf-8').split('\n')
        galaxy_files = [x.strip() for x in galaxy_files if x.strip()]

        for gf in galaxy_files:
            logger.debug(gf)
            with open(gf, 'r') as f:
                gdata = yaml.load(f.read())
            ckey = (gdata['namespace'], gdata['name'])
            logger.debug('found fqn: %s.%s' % ckey)
            if ckey not in self.collections:
                self.collections[ckey] = {
                    'name': ckey[1],
                    'namespace': {'name': ckey[0]},
                    'href': repo,
                    'plugins': {},
                    'filepath': os.path.dirname(gf)
                }
            self.index_plugins_in_collection(os.path.dirname(gf), ckey)

        res = find(cp, '-name', 'MANIFEST.json')
        manifest_files = res.stdout.decode('utf-8').split('\n')
        manifest_files = [x.strip() for x in manifest_files if x.strip()]
        for mf in manifest_files:
            logger.debug(mf)
            with open(mf, 'r') as f:
                mdata = json.loads(f.read())
            ckey = (mdata['collection_info']['namespace'], mdata['collection_info']['name'])
            logger.debug('found fqn: %s.%s' % ckey)
            if ckey not in self.collections:
                self.collections[ckey] = {
                    'name': ckey[1],
                    'namespace': {'name': ckey[0]},
                    'href': repo,
                    'plugins': {},
                    'filepath': os.path.dirname(mf)
                }
            self.index_plugins_in_collection(os.path.dirname(mf), ckey)

            #if 'netapp/ontap' in mf:
            #    import epdb; epdb.st()

    def provides_plugin(self, bit, plugin_type=None, exact=False):
        candidates = {}
        for fqn,collection in self.collections.items():
            for ptype,pfiles in collection['plugins'].items():
                if plugin_type and ptype != plugin_type:
                    continue
                for pfile in pfiles:
                    if exact:
                        ismatch = bit == pfile
                    else:
                        ismatch = re.search(r'%s' % bit, pfile)
                    if ismatch:
                        if fqn not in candidates:
                            candidates[fqn] = {}
                        if ptype not in candidates[fqn]:
                            candidates[fqn][ptype] = [] 
                        candidates[fqn][ptype].append(pfile)

        #if 'na_ontap_svm.py' in bit:
        #    import epdb; epdb.st()

        return candidates




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--usecache', action='store_true')
    args = parser.parse_args()


    gi = GalaxyIndexer()
    gi.run()
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
        # redhat
        #   foreman/candlepin/etc
        # tower
        'https://opendev.org/openstack/ansible-collections-openstack'
    ]
    for ghr in ghrepos:
        gi.fetch_git_repo(ghr)
    pprint(gi.provides_plugin('na_ontap_svm.py', plugin_type='module'))
    #import epdb; epdb.st()

    sq = StatusQuo()
    sq.run(usecache=args.usecache, galaxy_indexer=gi, base_scenario_file='scenarios/bcs/ansible.yml')

