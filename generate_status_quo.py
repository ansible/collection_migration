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
import copy
import os
import shutil
import subprocess

from collections import OrderedDict
from logzero import logger
import ruamel.yaml
from sh import git
from sh import find



def run_command(cmd):
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (so, se) = p.communicate()
    return (p.returncode, so, se)


class StatusQuo:

    collections = None
    plugins = None
    pluginfiles = None
    orphaned = None
    topics = None

    synonyms = {
        'apache-libcloud': 'cloud.misc',
        'aws': 'amazon',
        #'azure_rm': 'azure',
        'bigip': 'f5',
        'bigiq': 'f5',
        'ce': 'cloudengine',
        'checkpoint': 'check_point',
        'cloudforms': 'cloud',
        #'cloudstack': 'cloud.cloudstack',
        #'consul': 'clustering.consul',
        #'dns': 'net_tools',
        'ec2': 'amazon',
        #'ecs': 'crypto.entrust',
        #'kube': 'k8s',
        #'foreman': 'remote_management.foreman',
        'spacewalk': 'remote_management',
        'gce': 'google',
        'gcp': 'google',
        #'hashi_vault': 'crypto',
        'hetzner': 'hcloud',
        'hwc': 'huawei',
        #'jabber': 'web_infrastructure',
        'infinibox': 'infinidat',
        #'infoblox': 'net_tools.nios',
        'infoblox': 'nios',
        #'ipa': 'identity.ipa',
        'kubectl': 'k8s',
        'libcloud': 'cloud.misc',
        #'linode': 'cloud.linode',
        #'lxc': 'cloud.lxc',
        #'lxd': 'cloud.lxc',

        #'mso': 'network.aci',
        'mso': 'aci',
        #'nagios': 'monitoring.nagios',
        #'nagios': 'web_infrastructure',
        'oc': 'k8s',
        #'openstack': 'cloud.openstack',
        #'openshift': 'clustering.openshift',
        #'openvz': 'cloud.misc',
        #'ovirt': 'cloud.ovirt',
        #'onepassword': 'identity',

        #'package': 'packaging',
        'package': 'packaging.os',
        'passwordstore': 'identity',

        'powershell': 'windows',
        #'proxmox': 'cloud.misc',
        'psrp': 'windows',
        'rax': 'cloud.rackspace',
        #'redis': 'database.misc',
        'rhv': 'ovirt',
        'tower': 'ansible_tower',
        #'utm': 'sophos_utm',
        'vagrant': 'cloud',
        'vca': 'vmware',
        #'virtualbox': 'cloud.misc',
        #'vbox': 'cloud.misc',

        'win': 'windows',
        'win': 'windows',

        'Vmware': 'vmware',
        'vmware': 'cloud.vmware',
        #'yum': 'packaging.os',
        #'zabbix': 'monitoring.zabbix'
    }

    def __init__(self):
        self.collections = OrderedDict()
        self.url = 'https://github.com/ansible/ansible'
        self.checkouts_dir = '.cache/checkouts'
        self.checkout_dir = os.path.join(self.checkouts_dir, 'ansible')

    def run(self):
        self.manage_checkout()
        self.get_plugins()
        self.make_spec()

    def manage_checkout(self):
        logger.info('manage checkout')

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

    def get_plugins(self):

        pluginfiles = []

        # enumerate the modules
        root = os.path.join(self.checkout_dir, 'lib', 'ansible', 'modules')
        for dirName, subdirList, fileList in os.walk(root):

            for fn in fileList:
                if fn == '__init__.py':
                    continue
                if fn == 'loader.py':
                    continue
                fp = os.path.join(dirName, fn)

                topic = fp.replace(root + '/', '')
                topic = os.path.dirname(topic)
                topic = topic.replace('/', '.')

                pluginfiles.append(['modules', fn, topic, fp])

        # make a list of unique topics
        topics = sorted(set([x[2] for x in pluginfiles]))
        self.topics = topics[:]

        # enumerate all the other plugins
        root = os.path.join(self.checkout_dir, 'lib', 'ansible', 'plugins')
        for dirName, subdirList, fileList in os.walk(root):

            for fn in fileList:
                if fn == '__init__.py':
                    continue
                if fn == 'loader.py':
                    continue

                ptype = os.path.basename(dirName)
                fp = os.path.join(dirName, fn)

                # hacky topic matching
                ptopic = None
                for topic in topics:
                    if topic in fn:
                        ptopic = topic
                        break
                    if '.' in topic:
                        tparts = topic.split('.')
                        if tparts[-1] in fn:
                            ptopic = topic
                            break

                pluginfiles.append([ptype, fn, ptopic, fp])

        # let's get rid of contrib too
        root = os.path.join(self.checkout_dir, 'contrib', 'inventory')
        for dirName, subdirList, fileList in os.walk(root):
            ptype = 'scripts/inventory'
            for fn in fileList:
                fp = os.path.join(dirName, fn)
                bn = os.path.basename(fn).replace('.py', '').replace('.ini', '')

                ptopic = None
                for topic in topics:
                    if topic in bn or bn in topic:
                        ptopic = topic
                        break

                if ptopic is None:
                    #print(fn)
                    for idx,x in enumerate(pluginfiles):
                        if not x[2]:
                            continue
                        xbn = os.path.basename(x[-1]).replace('.py', '')
                        if xbn in bn or bn in xbn:
                            ptopic = x[2]
                            break

                #if ptopic is None:
                #    import epdb; epdb.st()

                pluginfiles.append([ptype, fn, ptopic, fp])

        # match action plugins to modules
        for idx,x in enumerate(pluginfiles):
            if x[2]:
                continue
            if x[0] != 'action':
                continue
            for pf in pluginfiles:
                if not pf[2]:
                    continue
                if os.path.basename(pf[-1]).replace('.py', '') == os.path.basename(x[-1]).replace('.py', '').replace('.ini', ''):
                    pluginfiles[idx][2] = pf[2]
                    break

        # fill in topics via synonyms
        for idx,x in enumerate(pluginfiles):
            if x[2]:
                continue

            #print(x)
            ptopic = None
            for k,v in self.synonyms.items():
                #print('k: %s' % k)
                if k in x[1]:
                    # hacky topic matching
                    for topic in topics:
                        #print('topic: %s' % topic)
                        if topic in v:
                            ptopic = topic
                            break
                        if '.' in topic:
                            tparts = topic.split('.')
                            if tparts[-1] in v:
                                ptopic = topic
                                break
                if ptopic:
                    pluginfiles[idx][2] = ptopic
                    break

        # find which modules use orphaned doc fragments
        for idx,x in enumerate(pluginfiles):
            if x[2]:
                continue
            if x[0] != 'doc_fragments':
                continue
            df = os.path.basename(x[-1])
            df = df.replace('.py', '')
            cmd = 'find %s -type f | xargs fgrep -iH %s' % (os.path.join(self.checkout_dir, 'lib', 'ansible', 'modules'), df)
            logger.info(cmd)
            (rc, so, se) = run_command(cmd)
            if rc == 0:
                filenames = so.decode('utf-8').split('\n')
                filenames = [x.split(':')[0] for x in filenames if x]
                filenames = sorted(set(filenames))
                dirnames = [os.path.dirname(x) for x in filenames]
                dirnames = [x.replace(os.path.join(self.checkout_dir, 'lib', 'ansible', 'modules') + '/', '') for x in dirnames]
                dirnames = sorted(set(dirnames))
                logger.info('%s dirs' % len(dirnames))
                #import epdb; epdb.st()

        self.orphaned = [x for x in pluginfiles if not x[-2]]
        self.pluginfiles = pluginfiles[:]

    def make_spec(self):
        topics = self.topics[:]
        for idx,x in enumerate(topics):
            if not '.' in x:
                topics[idx] = x + '.misc'
        self.collections['_core'] = {}
        self.collections['_orphaned'] = []
        for topic in topics:
            self.collections[topic] = {}

        for idx,x in enumerate(self.pluginfiles):
            topic = x[2]
            if topic is None:
                self.collections['_orphaned'].append(x[-1])
                continue
            ptype = x[0]
            if not '.' in topic:
                topic = topic + '.misc'
            if ptype not in self.collections[topic]:
                self.collections[topic][ptype] = []
            self.collections[topic][ptype].append(x[1])

        self.collections['_orphaned'] = sorted(self.collections['_orphaned'])
        for k,v in self.collections.items():
            if k == '_orphaned':
                continue
            for ptype, pfiles in v.items():
                self.collections[k][ptype] = sorted(pfiles)

        if os.path.exists('status_quo'):
            shutil.rmtree('status_quo')
        os.makedirs('status_quo')

        namespaces = OrderedDict()
        for k,v in self.collections.items():
            if '.' not in k:
                continue
            namespace = k.split('.')[0]
            name = k.split('.')[1]
            if namespace not in namespaces:
                namespaces[namespace] = OrderedDict()
            namespaces[namespace][name] = copy.deepcopy(v)

        for namespace,names in namespaces.items():
            fn = os.path.join('status_quo', namespace + '.yaml')
            with open(fn, 'w') as f:
                ruamel.yaml.dump(names, f, Dumper=ruamel.yaml.RoundTripDumper)

        with open(os.path.join('status_quo', '_orphaned.yaml'), 'w') as f:
            ruamel.yaml.dump(self.collections['_orphaned'], f, Dumper=ruamel.yaml.RoundTripDumper)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--filter')
    parser.add_argument('--namespace_prefix', default=None, help='prefix each collection namespace with this string')
    args = parser.parse_args()

    sq = StatusQuo()
    sq.run()



if __name__ == "__main__":
    main()
