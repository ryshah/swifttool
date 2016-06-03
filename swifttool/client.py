#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2014, Blue Box Group, Inc.
# Copyright (c) 2014, Craig Tracey <craigtracey@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import argparse
import logging
import os
import subprocess
import sys
import yaml

from fabric.api import env, execute, hide, parallel, put, sudo
from swifttool.ring_defintion import SwiftRingsDefinition

LOG = logging.getLogger(__name__)


def _setup_logger(level=logging.INFO):
    logger = logging.getLogger()
    logger.setLevel(level)
    log_handler = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter(fmt='%(asctime)s %(threadName)s %(name)s '
                            '%(levelname)s: %(message)s',
                            datefmt='%F %H:%M:%S')
    log_handler.setFormatter(fmt)
    logger.addHandler(log_handler)


@parallel
def _fab_copy_swift_directory(local_files, remote_dir):
    put(local_files, remote_dir, mirror_local_mode=True)


@parallel
def _fab_start_swift_services():
    with hide('running', 'stdout', 'stderr'):
        sudo("swift-init start all", pty=False, shell=False)


def bootstrap(args):
    rc = 0
    if not os.path.exists(args.config):
        raise Exception("Could not find confguration file '%s'" % args.config)

    config = yaml.load(open(args.config, 'r'))
    ringsdef = SwiftRingsDefinition(config)

    build_script = ringsdef.generate_script(outdir=args.outdir,
                                            meta=args.meta)
    subprocess.call(build_script)

    tempfiles = os.path.join(ringsdef.workspace, "*")
    execute(_fab_copy_swift_directory, tempfiles, args.outdir,
            hosts=ringsdef.nodes)
    execute(_fab_start_swift_services, hosts=ringsdef.nodes)


def main():
    parser = argparse.ArgumentParser(description='Tool to modify swift config')
    parser.add_argument('-d', '--debug', action='store_true')
    parser.add_argument('-i', dest='keyfile')
    parser.add_argument('-u', dest='user')

    subparsers = parser.add_subparsers()
    parser_genconfig = subparsers.add_parser('bootstrap')
    parser_genconfig.add_argument('--config', required=True)
    parser_genconfig.add_argument('--outdir', required=True)
    parser_genconfig.add_argument('--meta', default=None)
    parser_genconfig.set_defaults(func=bootstrap)

    args = parser.parse_args()

    level = logging.INFO
    if args.debug:
        level = logging.DEBUG
    _setup_logger(level)

    if args.keyfile:
        env.key_filename = args.keyfile
    if args.user:
        env.user = args.user

    try:
        args.func(args)
    except Exception as e:
        LOG.error("There was an error running swifttool: '%s'", e)
        sys.exit(-1)


if __name__ == '__main__':
    main()
