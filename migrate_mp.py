#!/usr/bin/env python

# also dynamically imports ansible in code

import argparse
import copy
import datetime
import logging
import os
import re
import shutil
import subprocess
import sys
import yaml

from collections import defaultdict
from pprint import pprint

from ansible.utils.collection_loader import AnsibleCollectionLoader

import logzero
from logzero import logger


from gh import GitHubOrgClient
from rsa_utils import RSAKey
from template_utils import render_template_into

# original script
from migrate import actually_remove
from migrate import add_deps_to_metadata
from migrate import checkout_repo
from migrate import copy_unit_tests
from migrate import create_unit_tests_copy_map
from migrate import init_galaxy_metadata
from migrate import inject_github_actions_workflow_into_collection
from migrate import inject_gitignore_into_collection
from migrate import inject_gitignore_into_tests
from migrate import inject_ignore_into_sanity_tests
from migrate import inject_init_into_tree
from migrate import inject_readme_into_collection
from migrate import inject_requirements_into_sanity_tests
from migrate import load_spec_file
from migrate import mark_moved_resources
from migrate import poor_mans_integration_tests_discovery
from migrate import process_symlink
from migrate import resolve_spec
from migrate import rewrite_py
from migrate import rewrite_integration_tests
from migrate import rewrite_unit_tests
from migrate import setup_options
from migrate import write_text_into_file
from migrate import write_yaml_into_file_as_is

from multiprocessing import Pool


# CONSTANTS/SETTINGS

# https://github.com/ansible/ansible/blob/100fe52860f45238ee8ca9e3019d1129ad043c68/hacking/fix_test_syntax.py#L62
FILTER_RE = re.compile(r'((.+?)\s*([\w \.\'"]+)(\s*)\|(\s*)(\w+))')
TEST_RE = re.compile(r'((.+?)\s*([\w \.\'"]+)(\s*)is(\s*)(\w+))')

DEVEL_URL = 'https://github.com/ansible/ansible.git'
DEVEL_BRANCH = 'devel'

ALL_THE_FILES = set()

COLLECTION_NAMESPACE = 'test_migrate_ns'
PLUGIN_EXCEPTION_PATHS = {'modules': 'lib/ansible/modules', 'module_utils': 'lib/ansible/module_utils', 'inventory_scripts': 'contrib/inventory'}
PLUGIN_DEST_EXCEPTION_PATHS = {'inventory_scripts': 'scripts/inventory'}

COLLECTION_SKIP_REWRITE = ('_core',)

RAW_STR_TMPL = "r'''{str_val}'''"
STR_TMPL = "'''{str_val}'''"

BAD_EXT = frozenset({'.pyo', '.pyc'})

VALID_SPEC_ENTRIES = frozenset({
    'action',
    'become',
    'cache',
    'callback',
    'cliconf',
    'connection',
    'doc_fragments',
    'filter',
    'httpapi',
    'inventory',
    'lookup',
    'module_utils',
    'modules',
    'netconf',
    'shell',
    'strategy',
    'terminal',
    'test',
    'vars',
    'inventory_scripts',
})

NOT_PLUGINS = frozenset(set(['inventory_scripts']))

VARNAMES_TO_PLUGIN_MAP = {
    'ansible_become_method': 'become',
    'ansible_connection': 'connection',
    'ansible_shell_type': 'shell',
}

KEYWORDS_TO_PLUGIN_MAP = {
    'become_method': 'become',
    'cache_plugin': 'cache',
    'connection': 'connection',
    'plugin': 'inventory',
    'strategy': 'strategy',
}

REWRITE_CLASS_PROPERTY_MAP = {
    'BecomeModule': 'name',
    'CallbackModule': 'CALLBACK_NAME',
    'Connection': 'transport',
    'InventoryModule': 'NAME',
}

REWRITE_CLASS_PROPERTY_PLUGINS= {
    'become',
    'callback',
    'connection',
    'inventory',
}

VARDIR = os.environ.get('GRAVITY_VAR_DIR', '.cache')
LOGFILE = os.path.join(VARDIR, 'errors.log')

REMOVE = set()

core = {}
manual_check = defaultdict(list)


### MAKE COLLECTIONS

def assemble_collections_mp(checkout_path, spec, args, target_github_org):

    worker_args = []
    for namespace, collections in spec.items():
        for collection, plugins in collections.items():

            if args.limits:
                matched = False
                for limit in args.limits:
                    if '.' in limit:
                        if limit == '%s.%s' % (namespace, collection):
                            matched = True
                            break
                    elif limit in namespace or limit in collection:
                        matched = True
                        break
                if not matched:
                    logger.info('%s.%s did not match filters, skipping' % (namespace, collection))
                    continue

            if args.excludes:
                matched = False
                for exclude in args.excludes:
                    if '.' in exclude:
                        if exclude == '%s.%s' % (namespace, collection):
                            matched = True
                            break
                    elif exclude in namespace or exclude in collection:
                        matched = True
                        break
                if matched:
                    logger.info('%s.%s was excluded, skipping' % (namespace, collection))
                    continue


            wargs = {
                'spec': spec,
                'checkout_path': checkout_path,
                'namespace': namespace,
                'collection_name': collection,
                'collection_plugins': plugins,
                'collection_spec': plugins,
                'args': args,
                'target_github_org': target_github_org
            }
            worker_args.append(wargs)

    collections_base_dir = os.path.join(args.vardir, 'collections')

    # expand globs so we deal with specific paths
    resolve_spec(spec, checkout_path, all_the_files=ALL_THE_FILES)

    # ensure we always use a clean copy
    if args.refresh and os.path.exists(collections_base_dir):
        shutil.rmtree(collections_base_dir)

    # make initial YAML transformation to minimize the diff
    mark_moved_resources(checkout_path, 'N/A', 'init', {})

    # fork the workers by namespace
    started = datetime.datetime.now()
    with Pool(4) as p:
        results = p.map(assemble_collection, worker_args)
    finished = datetime.datetime.now()

    timings = []
    combined_results = {}
    for result in results:
        if result['namespace'] not in combined_results:
            combined_results[result['namespace']] = {}
        combined_results[result['namespace']][result['name']] = copy.deepcopy(result)
        if 'finished' in result:
            timings.append([result['finished'] - result['started'], result['namespace'], result['name']])
    timings = sorted(timings, key=lambda x: x[0])
    pprint(timings)
    logger.info('assembly of %s collections took %s' % (len(worker_args), finished-started))
    import epdb; epdb.st()


def assemble_collection_test(colname):
    return {'test': colname}


def assemble_collection(wargs):
    args = wargs['args']
    spec = wargs['spec']
    checkout_path = wargs['checkout_path']
    collection_spec = wargs['collection_spec']
    collection = wargs['collection_name']
    collection_plugins = wargs['collection_plugins']
    namespace = wargs['namespace']
    target_github_org = wargs['target_github_org']
    collections_base_dir = os.path.join(args.vardir, 'collections')

    result = {
        'namespace': namespace,
        'name': collection,
        'started': datetime.datetime.now(),
        'remove': set(),
        'errors': []
    }

    import_deps = []
    docs_deps = []
    unit_deps = []
    integration_test_dirs = []
    migrated_to_collection = {}
    unit_tests_copy_map = {}

    if args.fail_on_core_rewrite:
        if collection != '_core':
            return result
    else:
        if collection.startswith('_'):
            # these are info only collections
            return result

    collection_dir = os.path.join(collections_base_dir, 'ansible_collections', namespace, collection)

    if args.refresh and os.path.exists(collection_dir):
        shutil.rmtree(collection_dir)

    if not os.path.exists(collection_dir):
        os.makedirs(collection_dir)

    # create the data for galaxy.yml
    galaxy_metadata = init_galaxy_metadata(collection, namespace, target_github_org)

    # process each plugin type
    #for plugin_type, plugins in collections_spec[collection].items():
    for plugin_type, plugins in collection_plugins.items():
        if not plugins:
            logger.error('Empty plugin_type: %s in spec for %s.%s', plugin_type, namespace, collection)
            continue

        # get src plugin path
        src_plugin_base = PLUGIN_EXCEPTION_PATHS.get(plugin_type, os.path.join('lib', 'ansible', 'plugins', plugin_type))

        # ensure destinations exist
        if plugin_type in PLUGIN_DEST_EXCEPTION_PATHS:
            relative_dest_plugin_base = PLUGIN_DEST_EXCEPTION_PATHS[plugin_type]
        else:
            relative_dest_plugin_base = os.path.join('plugins', plugin_type)
        dest_plugin_base = os.path.join(collection_dir, relative_dest_plugin_base)
        if not os.path.exists(dest_plugin_base):
            os.makedirs(dest_plugin_base)
            write_text_into_file(os.path.join(dest_plugin_base, '__init__.py'), '')

        # process each plugin
        for plugin in plugins:
            if os.path.splitext(plugin)[1] in BAD_EXT:
                raise Exception("We should not be migrating compiled files: %s" % plugin)

            # TODO: currently requires 'full name of file', but should work w/o extension?
            relative_src_plugin_path = os.path.join(src_plugin_base, plugin)
            src = os.path.join(checkout_path, relative_src_plugin_path)

            # TODO: collections are now scheduled to handle deprecations and aliases, until we get an implementation
            # we are just treating them as normal files for now (previously we were avoiding them).

            #if os.path.basename(plugin).startswith('_') and os.path.basename(plugin) != '__init__.py':
            #    if os.path.islink(src):
            #        logger.info("Removing plugin alias from checkout and skipping: %s (%s in %s.%s)",
            #                     plugin, plugin_type, namespace, collection)
            #        remove(src)
            #    else:
            #        logger.error("We should not be migrating deprecated plugins, skipping: %s (%s in %s.%s)",
            #                     plugin, plugin_type, namespace, collection)
            #    continue

            #remove(src)
            result['remove'].add(src)

            if plugin_type in ('modules',) and '/' in plugin:
                init_py_path = os.path.join(os.path.dirname(src), '__init__.py')
                if os.path.exists(init_py_path):
                    #remove(init_py_path)
                    result['remove'].add(init_py_path)

            do_preserve_subdirs = (
                (args.preserve_module_subdirs and plugin_type == 'modules')
                or plugin_type == 'module_utils'
            )
            plugin_path_chunk = plugin if do_preserve_subdirs else os.path.basename(plugin)
            relative_dest_plugin_path = os.path.join(relative_dest_plugin_base, plugin_path_chunk)

            migrated_to_collection[relative_src_plugin_path] = relative_dest_plugin_path

            dest = os.path.join(collection_dir, relative_dest_plugin_path)
            if do_preserve_subdirs:
                os.makedirs(os.path.dirname(dest), exist_ok=True)

            if os.path.islink(src):
                process_symlink(plugin_type, plugins, dest, src)
                # don't rewrite symlinks, original file should already be handled
                continue
            elif not src.endswith('.py'):
                # its not all python files, copy and go to next
                # TODO: handle powershell import rewrites
                shutil.copyfile(src, dest)
                continue

            logger.info('Processing %s -> %s', src, dest)

            deps = rewrite_py(src, dest, collection, spec, namespace, args)
            import_deps += deps[0]
            docs_deps += deps[1]

            if args.skip_tests or plugin_type in NOT_PLUGINS:
                # skip rest for 'not really plugins'
                continue

            integration_test_dirs.extend(poor_mans_integration_tests_discovery(checkout_path, plugin_type, plugin))

            # process unit tests
            plugin_unit_tests_copy_map = create_unit_tests_copy_map(
                checkout_path, plugin_type, plugin,
            )
            unit_tests_copy_map.update(plugin_unit_tests_copy_map)

    if not args.skip_tests:
        try:
            copy_unit_tests(unit_tests_copy_map, collection_dir, checkout_path)
        except Exception as e:
            logger.error(e)
            result['errors'].append(e)

        migrated_to_collection.update(unit_tests_copy_map)

        inject_init_into_tree(
            os.path.join(collection_dir, 'tests', 'unit'),
        )

        unit_deps += rewrite_unit_tests(collection_dir, collection, spec, namespace, args)

        inject_gitignore_into_tests(collection_dir)

        inject_ignore_into_sanity_tests(
            checkout_path, collection_dir, migrated_to_collection,
        )
        inject_requirements_into_sanity_tests(checkout_path, collection_dir)

        # FIXME need to hack PyYAML to preserve formatting (not how much it's possible or how much it is work)
        #   or use e.g. ruamel.yaml
        try:
            migrated_integration_test_files = rewrite_integration_tests(
                integration_test_dirs,
                checkout_path,
                collection_dir,
                namespace,
                collection,
                spec,
                args
            )
            migrated_to_collection.update(migrated_integration_test_files)
        except yaml.composer.ComposerError as e:
            logger.error(e)
            result['errors'].append(e)

        # FIXME how are these determined!? ...
        integration_tests_deps = set()

        # we only want the runtime deps in the galaxy.yml
        add_deps_to_metadata(set(import_deps).union(docs_deps), galaxy_metadata)

    # save a combined requirements file for easier human processing
    rfile = os.path.join(collection_dir, 'meta.yml')
    with open(rfile, 'w') as f:
        f.write(yaml.dump({
            'imports': sorted(set(['%s.%s' % (x[0],x[1]) for x in import_deps])),
            'doc_deps': sorted(set(['%s.%s' % (x[0],x[1]) for x in docs_deps])),
            'unit_deps': sorted(set(['%s.%s' % (x[0],x[1]) for x in unit_deps])),
            'integration_deps': sorted(set(integration_tests_deps)),
            'integration_test_provides': sorted(set([os.path.basename(x[0]) for x in integration_test_dirs if x[1]])),
            'integration_test_requires': sorted(set([os.path.basename(x[0]) for x in integration_test_dirs if not x[1]]))
        }))

    inject_gitignore_into_collection(collection_dir)
    j2_ctx = {
        'coll_ns': namespace,
        'coll_name': collection,
        'gh_org': target_github_org,
    }
    inject_readme_into_collection(
        collection_dir,
        ctx=j2_ctx,
    )
    inject_github_actions_workflow_into_collection(
        collection_dir,
        ctx=j2_ctx,
    )

    # write collection metadata
    write_yaml_into_file_as_is(
        os.path.join(collection_dir, 'galaxy.yml'),
        galaxy_metadata,
    )

    # init git repo
    subprocess.check_call(('git', 'init'), cwd=collection_dir)
    subprocess.check_call(('git', 'add', '.'), cwd=collection_dir)
    subprocess.check_call(
        ('git', 'commit', '-m', 'Initial commit', '--allow-empty'),
        cwd=collection_dir,
    )

    try:
        mark_moved_resources(
            checkout_path, namespace, collection, migrated_to_collection,
        )
    except Exception as e:
        logger.error(e)
        result['errors'].append(e)

    if args.move_plugins:
        try:
            actually_remove(checkout_path, namespace, collection)
        except Exception as e:
            logger.error(e)
            result['errors'].append(e)


    result['galaxy_metadata'] = galaxy_metadata
    result['import_deps'] = sorted(set(import_deps))
    result['docs_deps'] = sorted(set(docs_deps))
    result['unit_deps'] = sorted(set(unit_deps))
    result['integration_test_dirs'] = sorted(set(integration_test_dirs))
    result['migrated_to_collection'] = sorted(set(migrated_to_collection))
    result['unit_tests_copy_map'] = unit_tests_copy_map
    result['finished'] = datetime.datetime.now()

    return result



def main():
    parser = argparse.ArgumentParser()

    setup_options(parser)

    args = parser.parse_args()

    # required, so we should always have
    spec = {}

    for spec_file in os.listdir(args.spec_dir):
        if not spec_file.endswith('.yml'):
            logger.debug('skipping %s as it is not a yaml file', spec_file)
            continue
        try:
            spec[os.path.splitext(os.path.basename(spec_file))[0]] = load_spec_file(os.path.join(args.spec_dir, spec_file))
        except Exception as e:
            # warn we skipped spec_file for reasons: e
            raise

    devel_path = os.path.join(args.vardir, 'releases', f'{DEVEL_BRANCH}.git')

    global ALL_THE_FILES
    ALL_THE_FILES = checkout_repo(DEVEL_URL, devel_path, refresh=args.refresh)

    if args.skip_migration:
        logger.info('Skipping the migration...')
    else:

        if args.convert_symlinks:
            logger.info('Converting symlinks ...')
            script = '%s/undolinks.sh' %  os.path.dirname(os.path.realpath(__file__))

            for plugin in VALID_SPEC_ENTRIES:
                logger.info('Converting symlinks %s ...' % plugin)
                plugin_base = PLUGIN_EXCEPTION_PATHS.get(plugin, os.path.join('lib', 'ansible', 'plugins', plugin))
                subprocess.check_call((script, os.path.join(devel_path, plugin_base)))

        logger.info('Starting the migration...')

        # we need to be able to import collections when evaluating filters and tests
        loader = AnsibleCollectionLoader()
        loader._n_configured_paths = [os.path.join(args.vardir, 'collections')]
        sys.meta_path.insert(0, loader)

        # doeet
        assemble_collections_mp(devel_path, spec, args, args.target_github_org)

        global core
        print('======= Assumed stayed in core =======\n')
        print(yaml.dump(core))

        global manual_check
        print('======= Could not rewrite the following, ' 'please check manually =======\n',)
        print(yaml.dump(dict(manual_check)))

        print(f'See {LOGFILE} for any warnings/errors ' 'that were logged during migration.',)

    if args.skip_publish:
        logger.info('Skipping the publish step...')
        return

    tmp_rsa_key = None
    if args.publish_to_github or args.push_migrated_core:
        logger.info('Starting the publish step...')
        tmp_rsa_key = RSAKey()
        gh_api = GitHubOrgClient(
            args.github_app_id, args.github_app_key_path,
            args.target_github_org,
            deployment_rsa_pub_key=tmp_rsa_key.public_openssh,
        )
        logger.debug('Initialized a temporary RSA key and GitHub API client')

    if args.publish_to_github:
        logger.info('Publishing the migrated collections to GitHub...')
        publish_to_github(
            args.vardir, spec,
            gh_api, tmp_rsa_key,
        )

    if args.push_migrated_core:
        logger.info('Publishing the migrated "Core" to GitHub...')
        push_migrated_core(devel_path, gh_api, tmp_rsa_key, args.spec_dir)

### main execution

os.makedirs(VARDIR, exist_ok=True)
logzero.logfile(LOGFILE, loglevel=logging.WARNING)

if __name__ == "__main__":
    main()
