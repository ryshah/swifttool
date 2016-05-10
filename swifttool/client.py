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
#

import argparse
import glob
import json
import netifaces
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import yaml

from utils import get_devices, RING_TYPES
from fabric.api import env, execute, hide, parallel, put, sudo
from netifaces import interfaces, ifaddresses, AF_INET


class SwiftRingsDefinition(object):

    def __init__(self, data=None):
        self.ring_builder_cmd = "swift-ring-builder"
        self.ports = {
            'object': 6000,
            'container': 6001,
            'account': 6002,
        }
        self.replicas = 3
        self.min_part_hours = 1
        self.zones = {}
        # Area to build new or update existing rings
        self.workspace = tempfile.mkdtemp()
        if data:
            self.__dict__.update(data)

    def __del__(self):
        shutil.rmtree(self.workspace)

    def __repr__(self):
        return str(self.__dict__)

    def _ring_create_command(self, ringtype):
        return "%s %s/%s.builder create %d %d %d" % (
            self.ring_builder_cmd, self.workspace, ringtype,
            int(self.part_power), int(self.replicas),
            int(self.min_part_hours))

    def _ring_add_command(self, ringtype, zone, host, port, disk,
                          metadata, weight):
        return "%s %s/%s.builder add %s-%s:%d/%s_%s %d" % (
            self.ring_builder_cmd, self.workspace, ringtype, zone, host,
            int(port), disk, metadata, int(weight))

    def _ring_rebalance_command(self, ringtype):
        return "%s %s/%s.builder rebalance" % (
            self.ring_builder_cmd, self.workspace, ringtype)

    def _ring_setweight_command(self, ringtype, zone, host, port,
                                disk, weight):
        return "%s %s/%s.builder set_weight %s-%s:%d/%s %d" % (
            self.ring_builder_cmd, self.workspace, ringtype, zone, host,
            int(port), disk, int(weight))

    def _ring_remove_command(self, ringtype, zone, host, port, disk):
        return "%s %s/%s.builder remove %s-%s:%d/%s" % (
            self.ring_builder_cmd, self.workspace, ringtype, zone, host,
            int(port), disk)

    def _ring_search_command(self, ringtype, zone, host, port, disk):
        return "%s %s/%s.builder search %s-%s:%d/%s" % (
            self.ring_builder_cmd, self.workspace, ringtype, zone, host,
            int(port), disk)

    @property
    def nodes(self):
        ret = set()
        if self.zones and isinstance(self.zones, dict):
            for zone, nodes in self.zones.iteritems():
                ret.update(nodes.keys())
        return ret

    def generate_commands(self, rebalance=True, meta=None):
        commands = []
        ring_disks = get_devices(self.zones, metadata=meta)

        for ringtype in RING_TYPES:
            builder_present = os.path.exists("%s/%s.builder" %
                                             (self.workspace, ringtype))
            if not builder_present:
                commands.append(self._ring_create_command(ringtype))

            for zone, devices in ring_disks[ringtype].iteritems():
                for device in devices:
                    port = self.ports[ringtype]
                    weight = device['weight']
                    disk = device['device']
                    node = device['ip']
                    metadata = device['metadata']
                    # When rings are not present or if device does not
                    # exist in ring, add it to the ring
                    # Else if the weight is to be set to 0 remove
                    # the device eor just update the weight
                    if not builder_present or \
                       not self._is_devpresent(ringtype, zone, node,
                                               port, disk):
                        cmd = self._ring_add_command(ringtype, zone,
                                                     node, port,
                                                     disk, metadata,
                                                     weight)
                    else:
                        if int(weight) == 0:
                            cmd = self._ring_remove_command(ringtype,
                                                            zone, node,
                                                            port, disk)
                        else:
                            # Always set the weight of device
                            # Verified that setting weight (to same)
                            # value doesnt cause partitions to reassign
                            cmd = self._ring_setweight_command(ringtype,
                                                               zone,
                                                               node,
                                                               port,
                                                               disk,
                                                               weight)
                    commands.append(cmd)
            if rebalance:
                commands.append(self._ring_rebalance_command(ringtype))

        return commands

    def _update_workspace(self, outdir):
        # Copy the builder files if all 3 exists, else create new
        if os.path.exists(os.path.join(outdir, "account.builder")) and \
           os.path.exists(os.path.join(outdir, "container.builder")) and \
           os.path.exists(os.path.join(outdir, "object.builder")):
            for filename in glob.glob(os.path.join(outdir, "*.builder")):
                shutil.copy(filename, self.workspace)

    def generate_script(self, outdir, name='ring_builder.sh',
                        rebalance=True, meta=None):
        self._update_workspace(outdir)
        commands = ["#!/bin/bash\n"]
        commands = commands + self.generate_commands(rebalance,
                                                     meta)

        outfile = os.path.join(self.workspace, name)
        f = open(outfile, 'w')
        for command in commands:
            f.write("%s\n" % command)
        f.close()

        st = os.stat(outfile)
        os.chmod(outfile, st.st_mode | stat.S_IEXEC)
        return outfile

    def _is_devpresent(self, ringtype, zone, node, port, disk):
        command = self._ring_search_command(ringtype, zone, node,
                                            port, disk)
        rc = subprocess.call(command, shell=True)
        return rc == 0


def ip4_addresses():
    ips = []
    for interface in interfaces():
        addresses = ifaddresses(interface)
        if addresses and AF_INET in addresses:
            for link in addresses[AF_INET]:
                ips.append(link['addr'])
    return ips


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

    try:
        config = yaml.load(open(args.config, 'r'))
        ringsdef = SwiftRingsDefinition(config)

        build_script = ringsdef.generate_script(outdir=args.outdir,
                                                meta=args.meta)
        subprocess.call(build_script)

        tempfiles = os.path.join(ringsdef.workspace, "*")
        execute(_fab_copy_swift_directory, tempfiles, args.outdir,
                hosts=ringsdef.nodes)
        execute(_fab_start_swift_services, hosts=ringsdef.nodes)
    except Exception as e:
        print >> sys.stderr, "There was an error bootrapping: '%s'" % e
        rc = -1

    sys.exit(rc)


def main():
    parser = argparse.ArgumentParser(description='Tool to modify swift config')
    subparsers = parser.add_subparsers()

    parser.add_argument('-i', dest='keyfile')
    parser.add_argument('-u', dest='user')

    parser_genconfig = subparsers.add_parser('bootstrap')
    parser_genconfig.add_argument('--config', required=True)
    parser_genconfig.add_argument('--outdir', required=True)
    parser_genconfig.add_argument('--meta', default=None)
    parser_genconfig.set_defaults(func=bootstrap)

    args = parser.parse_args()
    if args.keyfile:
        env.key_filename = args.keyfile
    if args.user:
        env.user = args.user

    args.func(args)


if __name__ == '__main__':
    main()
