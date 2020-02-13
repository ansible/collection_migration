#!/usr/bin/env python3

#################################################################
# update_nwo.py - recreates scenarios/nwo with current plugins
# 
# instructions:
#   1) virtualenv --python=$(which python3) venv
#   2) source venv/bin/activate
#   3) pip install -r requirements_nwo.txt
#   4) ./update_nwo.py
#   5) rm -rf scenarios/nwo
#   6) mv scenarios/nwo.new scenarios/nwo
#
#################################################################


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
import shutil
import subprocess

import yaml
from logzero import logger
import ruamel.yaml
import git as pygit
from sh import git
from sh import find

from pprint import pprint

from ansibullbot.utils.component_tools import AnsibleComponentMatcher
from ansibullbot.utils.git_tools import GitRepoWrapper


def collection_diff(a, b):
    collections = list(a.keys())
    collections += list(b.keys())
    collections = sorted(set(collections))

    delta = {}
    for cn in collections:
        if cn not in a or cn not in b:
            if 'missing_collections' not in delta:
                delta['missing_collections'] = []
            if cn not in a:
                delta['missing_collections'].append(['a', cn])
            else:
                delta['missing_collections'].append(['b', cn])
            continue

        plugin_types = list(a[cn].keys())
        plugin_types += list(b[cn].keys())
        plugin_types = sorted(set(plugin_types))

        for pt in plugin_types:
            if pt not in a[cn] or pt not in b[cn]:
                if 'missing_plugin_types' not in delta:
                    delta['missing_plugin_types'] = []
                if pt not in a[cn]:
                    delta['missing_plugin_types'].append(['a', cn, pt])
                else:
                    delta['missing_plugin_types'].append(['b', cn, pt])
                continue

            files = a[cn][pt][:]
            files += b[cn][pt][:]
            files = sorted(set(files))

            for filen in files:
                if filen not in a[cn][pt] or filen not in b[cn][pt]:
                    dkey = 'missing_%s' % pt
                    if dkey not in delta:
                        delta[dkey] = []
                    if filen not in a[cn][pt]:
                        delta[dkey].append(['a', cn, filen])
                    else:
                        delta[dkey].append(['b', cn, filen])

    #import epdb; epdb.st()
    return delta


class UpdateNWO:

    SCENARIO = 'nwo'
    DUMPING_GROUND = ('community', 'general')

    collections = None
    plugins = None
    pluginfiles = None
    orphaned = None
    topics = None

    def __init__(self):

        self.scenario_output_dir = os.path.join('scenarios', self.SCENARIO + '.new')

        self.component_matcher = None
        self.galaxyindexer = None
        self.cachedir = '.cache'
        self.pluginfiles = []
        self.collections = {}
        self.url = 'https://github.com/ansible/ansible'
        self.checkouts_dir = '.cache/checkouts'
        self.checkout_dir = os.path.join(self.checkouts_dir, 'ansible')
        self.community_general_topics = None

        self.scenario_cache = {}

        self.rules = []

    def run(self, usecache=False, galaxy_indexer=None, base_scenario_file=None, writeall=False, use_botmeta=True, inplace=False, writecsv=True):

        if os.path.exists(self.scenario_output_dir):
            shutil.rmtree(self.scenario_output_dir)
        os.makedirs(self.scenario_output_dir)

        if galaxy_indexer:
            self.galaxy_indexer = galaxy_indexer

        self.manage_checkout()

        # ansibot magic
        gitrepo = GitRepoWrapper(
            cachedir=self.cachedir,
            repo=self.url
        )
        self.component_matcher = AnsibleComponentMatcher(
            gitrepo=gitrepo,
            email_cache={}
        )

        self.get_plugins()
        self.map_existing_files_to_rules()
        if use_botmeta:
            self.map_botmeta_migrations_to_rules()
        self.map_plugins_to_collections()

        self.make_spec(writeall=writeall, inplace=inplace)
        if writecsv:
            self.make_compiled_csv(inplace=inplace)

    def map_existing_files_to_rules(self):

        ''' Make a set of matching rules based on current scenario files '''

        sfiles = glob.glob('scenarios/%s/*.yml' % self.SCENARIO) 
        sfiles = sorted(set(sfiles))

        for sfile in sfiles:
            with open(sfile, 'r') as f:
                ydata = yaml.load(f.read())
            namespace = os.path.basename(sfile).replace('.yml', '')
            self.scenario_cache[namespace] = copy.deepcopy(ydata)
            for name,plugins in ydata.items():

                if namespace == self.DUMPING_GROUND[0] and name == self.DUMPING_GROUND[1]:
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

    def map_botmeta_migrations_to_rules(self):

        ''' botmeta is also a source of truth for migrations '''

        logger.info("map out BOTMETA's view of the world")

        rules_added = 0
        for pf in self.pluginfiles:
            libix = pf[3].index('/lib/')
            libpath = pf[3][libix+1:]
            meta = self.component_matcher.get_meta_for_file(libpath)
            if meta.get('migrated_to'):
                mt = meta['migrated_to'][0]
                namespace = mt.split('.')[0]
                name = mt.split('.')[1]
                if '/modules/' in libpath:
                    plugin_type = libpath.split('/')[2]
                else:
                    plugin_type = libpath.split('/')[3]

                # resembles what needs to go on each line in the specfile
                matcher = self._make_relpath(libpath, plugintype=plugin_type)

                # create the rule
                rule = {
                    'matcher': matcher,
                    'name': name,
                    'namespace': namespace,
                    'plugin_type': plugin_type,
                    'source': 'BOTMETA.yml'
                }
                self.rules.insert(0, rule)
                rules_added += 1

        logger.info('%s rules added from BOTMETA' % rules_added)

    def manage_checkout(self):

        ''' Could probably be replaced with pygit '''

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

        ''' use rules to match a plugin file to a namespace.name '''

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

            # globs are MORE FUN ...
            if '*' in rule['matcher']:
                if re.match(rule['matcher'], plugin_relpath):
                    matched_rules.append(rule)
                    continue

        # keep init files in base unless otherwise specified
        if plugin_basename == "__init__.py" and not matched_rules:
            iparts = plugin_relpath.split('/')
            if len(iparts) in [1] or iparts[0] in ['csharp', 'common', 'facts', 'powershell']:
                return (
                    'ansible',
                    '_core', 
                    {
                        'plugin_type': plugin_type,
                        'matcher': plugin_relpath,
                        'namespace': 'ansible',
                        'name': '_core'
                    }
                )

        # pick the "best" rule?
        if len(matched_rules) == 1:
            return (matched_rules[0]['namespace'], matched_rules[0]['name'], matched_rules[0])        
        elif len(matched_rules) > 1:
            # use most specific match?
            for mr in matched_rules:
                mpaths = mr['matcher'].split('/')
                if len(mpaths) == len(ppaths):
                    return (mr['namespace'], mr['name'], mr)        

        # default to community dumping ground
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

        ''' Find all plugins in the cached checkout and make a list '''

        # enumerate the modules
        logger.info('iterating through modules')
        root = os.path.join(self.checkout_dir, 'lib', 'ansible', 'modules')
        for dirName, subdirList, fileList in os.walk(root):

            #for fn in set(fileList) - {'__init__.py', 'loader.py'}:
            for fn in set(fileList):
                fp = os.path.join(dirName, fn)
                topic = None
                self.pluginfiles.append(['modules', fn, topic, fp])

        # enumerate the module utils
        logger.info('iterating through module utils')
        root = os.path.join(self.checkout_dir, 'lib', 'ansible', 'module_utils')
        for dirName, subdirList, fileList in os.walk(root):

            #for fn in set(fileList) - {'__init__.py', 'loader.py'}:
            for fn in set(fileList):
                fp = os.path.join(dirName, fn)
                #topic = self._guess_topic(fp)
                topic = None
                self.pluginfiles.append(['module_utils', fn, topic, fp])

        # enumerate all the other plugins
        logger.info('examining other plugins')
        root = os.path.join(self.checkout_dir, 'lib', 'ansible', 'plugins')
        for dirName, subdirList, fileList in os.walk(root):

            #for fn in set(fileList) - {'__init__.py', 'loader.py'}:
            for fn in set(fileList):
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

        ''' associate each plugin to a collection '''

        self.community_general_topics = set()
        self.topics = set()

        # enumerate the modules
        logger.info('iterating through modules')
        root = os.path.join(self.checkout_dir, 'lib', 'ansible', 'modules')
        for dirName, subdirList, fileList in os.walk(root):

            #for fn in set(fileList) - {'__init__.py', 'loader.py'}:
            for fn in set(fileList):
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

    def make_compiled_csv(self, inplace=False):
        
        ''' Make the human readable aggregated spreadsheet '''

        if inplace:
            fn = os.path.join('scenarios', self.SCENARIO, 'compiled.csv')
        else:
            fn = os.path.join(self.scenario_output_dir, 'compiled.csv')

        logger.info('compiling %s' % fn)
        with open(fn, 'w') as csvfile:
            spamwriter = csv.writer(csvfile)
            spamwriter.writerow([
                'filename',
                'fqn',
                'namespace',
                'name',
                'current_support_level',
                'new_support_level',
                'botmeta_migrated_to',
                'scenario_file',
                'scenario_plugin_type',
                'matched_line']
            )

            for pf in self.pluginfiles:

                ns = pf[2][0]
                name = pf[2][1]
                fqn = '%s.%s' % (ns, name)
                if ns == 'ansible' and name == '_core':
                    fqn = 'base'

                relpath = pf[3].replace(self.checkout_dir+'/', '')
                meta = self.component_matcher.get_meta_for_file(relpath)
                migrated_to = meta.get('migrated_to')
                if migrated_to:
                    migrated_to = migrated_to[0]

                new_support = 'community'
                if pf[2][0] == 'ansible' and pf[2][1] == '_core':
                    new_support = 'core'
                    name = 'base'

                row = [
                    relpath,
                    fqn,
                    ns,
                    name,
                    meta['support'],
                    new_support,
                    migrated_to,
                    'scenarios/nwo/%s.yml' % ns,
                    pf[4]['plugin_type'],
                    pf[4]['matcher']
                ]
                if ns == self.DUMPING_GROUND[0] and name == self.DUMPING_GROUND[1]:
                    row[7] = 'unclaimed!'
                spamwriter.writerow(row)

    def _make_relpath(self, filename, plugintype):
        ''' create relative path for a plugin, minus the plugin dir '''
        # .cache/checkouts/ansible/lib/ansible/module_utils/foo/bar/acme.py
        # foo/bar/acme.py
        pindex = filename.index('/'+plugintype)
        relpath = filename[pindex+len(plugintype)+2:]
        return relpath

    def check_spec_for_dupes(self, spec):
        ''' Duplicated files across collections is an instant fail for migrate.py '''
        seen = {}
        for ns,col in spec.items():
            for cn,plugins in col.items():
                for pt,pfs in plugins.items():
                    for pf in pfs:

                        # use real globbing to list files ...
                        if '*' in pf:
                            if pt.startswith('module'):
                                gpattern = os.path.join(self.checkout_dir, 'lib', 'ansible', pt, pf)
                                filenames = glob.glob(gpattern)
                                filenames = [x.replace(os.path.join(self.checkout_dir, 'lib', 'ansible', pt)+'/', '') for x in filenames]
                            else:
                                gpattern = os.path.join(self.checkout_dir, 'lib', 'ansible', 'plugins', pt, pf)
                                filenames = glob.glob(gpattern)
                                filenames = [x.replace(os.path.join(self.checkout_dir, 'lib', 'ansible', 'plugins', pt)+'/', '') for x in filenames]
                        else:
                            filenames = [pf]

                        for thisf in filenames:
                            thisf = os.path.join(pt, thisf)
                            if thisf in seen:
                                raise Exception('%s\'s %s %s is duplicated in %s.%s' % (seen[thisf], pt, thisf, ns, cn))
                            seen[thisf] = '%s.%s' % (ns, cn)

    def make_spec(self, writeall=False, inplace=False):

        ''' Aseemble namespaces and collections for the files and write to disk as yaml '''

        # make specfile ready dicts for each collection
        for idx,x in enumerate(self.pluginfiles):
            ns = x[-1]['namespace']
            name = x[-1]['name']
            ckey = (ns, name)
            ptype = x[0]
            matcher = x[-1]['matcher']            

            # use the relative path for unmatched community files
            if 'unclaimed' in matcher:
                matcher =  self._make_relpath(x[3], ptype)

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

        # validate there are no dupes!
        self.check_spec_for_dupes(namespaces)

        # write each namespace as a separate file
        for namespace,collections in namespaces.items():

            # overwrite existing file if inplace
            if inplace:
                fn = os.path.join('scenarios', self.SCENARIO, namespace + '.yml')
            else:
                fn = os.path.join(self.scenario_output_dir, namespace + '.yml')

            # community is the only one we really need to write since it's a catchall
            if not writeall and inplace and namespace != self.DUMPING_GROUND[0]:
                continue

            #logger.info('rewrite %s' % fn)
            if namespace == self.DUMPING_GROUND[0]:
                this_data = copy.deepcopy(self.scenario_cache[self.DUMPING_GROUND[0]])
                this_data[self.DUMPING_GROUND[1]] = collections[self.DUMPING_GROUND[1]]
            else:
                this_data = collections

            # check for diff ...
            cdiff = collection_diff(self.scenario_cache[namespace], this_data)
            if not cdiff:
                # if there's no diff and we're making a new dir, we still need to write it out
                if not inplace:
                    logger.info('duplicate %s' % fn)
                    with open(fn, 'w') as f:
                        ruamel.yaml.dump(self.scenario_cache[namespace], f, Dumper=ruamel.yaml.RoundTripDumper)
                continue
            logger.info('%s has changes ...' % namespace)
            pprint(cdiff)

            # sort all keys and make a clean yaml structure
            nd = {}
            names = sorted(list(this_data.keys()))
            for name in names:
                nd[name] = {}

                ptypes = sorted(list(this_data[name].keys()))
                ptypes += sorted(list(self.scenario_cache[namespace][name].keys()))
                ptypes = sorted(set(ptypes))
                ptypes = [x for x in ptypes if x != 'plugins']

                for ptype in ptypes:
                    if ptype in this_data[name]:
                        nd[name][ptype] = sorted(this_data[name][ptype])
                    if ptype in self.scenario_cache[namespace][name]:
                        if ptype not in nd[name]:
                            nd[name][ptype] = []
                        # readd the old entries ... ?
                        if namespace != self.DUMPING_GROUND[0] and name != self.DUMPING_GROUND[1]:
                            nd[name][ptype] += self.scenario_cache[namespace][name][ptype]
                        nd[name][ptype] = sorted(set(nd[name][ptype]))

            logger.info('rewrite %s' % fn)
            with open(fn, 'w') as f:
                ruamel.yaml.dump(nd, f, Dumper=ruamel.yaml.RoundTripDumper)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--usecache', action='store_true')
    parser.add_argument('--inplace', action='store_true')
    parser.add_argument('--nocsv', action='store_true')
    parser.add_argument('--nobotmeta', action='store_true', help="ignore botmeta processing")
    parser.add_argument('--writeall', action='store_true', help="write out all specs instead of just community")
    args = parser.parse_args()

    nwo = UpdateNWO()
    nwo.run(usecache=args.usecache, writeall=args.writeall, use_botmeta=not args.nobotmeta, inplace=args.inplace, writecsv=not args.nocsv)
