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

import glob
import logging
import os
import re
import shutil
import stat
import subprocess
import tempfile

from fabric.api import execute, hide, sudo
from netifaces import interfaces, ifaddresses, AF_INET

RING_TYPES = ['account', 'container', 'object']

_host_lshw_output = {}

LOG = logging.getLogger(__name__)


def _parse_lshw_output(output, blockdev):
    disks = re.split('\s*\*', output.strip())
    alldisks = []
    for disk in disks:
        d = {}
        for line in disk.split('\n'):
            match = re.match('^-(\w+)', line)
            if match:
                d['class'] = match.group(1)
            else:
                match = re.match('^\s+([\w\s]+):\s+(.*)$', line)
                if match:
                    key = re.sub('\s', '_', match.group(1))
                    val = match.group(2)
                    d[key] = val
        if 'class' in d:
            alldisks.append(d)

    for d in alldisks:
        if d['logical_name'] == blockdev:
            serial = d['serial']
            match = re.match('\s*(\d+)[MG]iB.*', d['size'])
            if not match:
                raise Exception("Could not find size of disk %s" % disk)
            size = int(match.group(1))
            return size, serial


def _fab_get_disk_size_serial(ip, blockdev):
    with hide('running', 'stdout', 'stderr'):
        global _host_lshw_output
        output = None
        if ip in _host_lshw_output:
            output = _host_lshw_output[ip]
        else:
            output = sudo('lshw -C disk', pty=False, shell=False)
            _host_lshw_output[ip] = output
        return _parse_lshw_output(output, blockdev)


def get_disk_size_serial(ip, blockdev):
    with hide('running', 'stdout', 'stderr'):
        out = execute(_fab_get_disk_size_serial, ip, blockdev, hosts=[ip])
        return out[ip]


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

        for ringtype in RING_TYPES:
            builder_present = os.path.exists("%s/%s.builder" %
                                             (self.workspace, ringtype))
            if not builder_present:
                commands.append(self._ring_create_command(ringtype))

            for zone, nodes in self.zones.iteritems():
                for node, disks in nodes.iteritems():

                    ringdisks = []
                    # Add all disks designated for ringtype
                    if isinstance(disks['disks'], dict):
                        if ringtype in disks['disks']:
                            ringdisks += disks['disks'][ringtype]
                        else:
                            raise Exception("Malformed ring_defintion.yml. "
                                            "Unknown ringtype: %s" % ringtype)
                    elif isinstance(disks['disks'], list):
                        ringdisks = disks['disks']

                    for ringdisk in ringdisks:
                        disk = None
                        weight = None
                        serial = None
                        metadata = meta
                        if not isinstance(ringdisk, dict):
                            match = re.match('(.*)\d+$', ringdisk)
                            blockdev = '/dev/%s' % match.group(1)

                            # treat size as weight and serial as metadata
                            weight, serial = get_disk_size_serial(node,
                                                                  blockdev)
                            disk = ringdisk
                        else:
                            disk = ringdisk['blockdev']
                            weight = ringdisk['weight']
                            metadata = ringdisk.get('metadata',
                                                    'nometadata')

                        if not metadata and serial:
                            metadata = serial
                        port = self.ports[ringtype]
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
