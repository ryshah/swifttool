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
import copy
import glob
import json
import logging
import math
import netifaces
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import yaml

from fabric.api import env, execute, hide, parallel, put, sudo
from netifaces import interfaces, ifaddresses, AF_INET
from logging.handlers import SysLogHandler
from swift.common.ring import RingBuilder

RING_TYPES = ['account', 'container', 'object']
_host_lshw_output = {}


class CapacityManager(object):

    def __init__(self, iterations=10,
                 def_file='/etc/swift/ring_definition.yml'):
        self.iterations = int(iterations)
        self.def_file = def_file
        self.cluster_changes = {}
        self.cluster_config = None
        self._calculate_cluster_changes()
        self._setup_rsyslog()

    def update_cluster(self):
        self.logger.info("Starting to update the cluster")
        for iteration in range(0, self.iterations):
            changed = self._update_cluster_nodes()
            if not changed:
                break
            self.logger.info("Iteration " + str(iteration) + " complete")
            self.logger.info("Sleeping for 60 seconds")
            time.sleep(60)
        self.logger.info("Finished updating the cluster")
        # Force rebalance and wailt until there no more required

    def _update_cluster_nodes(self):
        try:
            updated_cfg = copy.deepcopy(self.cluster_config)
            # Update the cluster config to what
            # we desirei
            changed = False
            for builder in RING_TYPES:
                if builder not in self.cluster_changes:
                    continue
                for builder_changes in self.cluster_changes[builder]:
                    for zone, devices in builder_changes.items():
                        # Get the current configuration from rings
                        ring_devs = _get_ring_devices(builder, zone)
                        ip = self._get_zone_ip(devices)
                        updated_cfg['zones'].setdefault(zone, {})
                        updated_cfg['zones'][zone].setdefault(ip, {})
                        updated_cfg['zones'][zone][ip].setdefault('disks', {})
                        newdevs = []
                        for dev in devices:
                            devname = dev['device']
                            if devname not in ring_devs:
                                initial_weight = 0
                            else:
                                initial_weight = ring_devs[devname]
                            if initial_weight == dev['target']:
                                continue
                            devinfo = {}
                            devinfo['blockdev'] = devname
                            devinfo['weight'] = int(initial_weight
                                                    + dev['step'])
                            if abs(devinfo['weight'] - dev['target']) < 50:
                                devinfo['weight'] = dev['target']
                            self.logger.debug("Updating %s builder. \
                                Setting the %s device capacity to %s",
                                              builder, devname,
                                              str(devinfo['weight']))
                            newdevs.append(devinfo)
                            changed = True
                        updated_cfg['zones'][zone][ip]['disks'][builder] = \
                            newdevs
            if changed:
                _ringsdef_helper(updated_cfg, 'nometadata', '/etc/swift')
            return changed
        except Exception as e:
            print "Error updating cluster configuration", e
            sys.exit(-1)

    @staticmethod
    def _get_zone_ip(devs):
        for dev in devs:
            if 'ip' in dev:
                return dev['ip']

    def _calculate_cluster_changes(self):
        try:
            self.cluster_config = yaml.load(open(self.def_file, 'r'))
            expected_config = get_devices(self.cluster_config['zones'])
            current_config = _get_all_cluster_devices()
            # To get devices to be removed we look for devices present in
            # current_config and not in expected_config
            # To get list of devices to be added look for devices in expected
            # not in current
            additions = self._get_devices_changed(expected_config,
                                                  current_config)
            removals = self._get_devices_changed(current_config,
                                                 expected_config)
            # Reconcile the two structures in to single
            # Each ring will be list of devices to be modified
            # Each device (in ring) will have following attributes:
            # - zone
            # - ip
            # - device
            # - target (device weight when adding, 0 when removing)
            # - step (amount to change every time till target reached)
            # step is defaulted to 10% or -10% of target
            self._calculate_changes(additions, True)
            self._calculate_changes(removals, False)
        except Exception as e:
            print "Error getting device info", e
            sys.exit(-1)

    def _calculate_changes(self, source, add_operation=True):
        for builder in RING_TYPES:
            for zone, devadd in source[builder].items():
                self.cluster_changes.setdefault(builder, [])
                devices = []
                for device in devadd:
                    dev = {}
                    dev['ip'] = device['ip']
                    dev['device'] = device['device']
                    if add_operation:
                        dev['target'] = device['weight']
                        dev['step'] = int(math.ceil(device['weight'] /
                                                    self.iterations))
                    else:
                        dev['target'] = 0
                        dev['step'] = int(-1 * math.ceil(device['weight'] /
                                                         self.iterations))
                    devices.append(dev)
                self.cluster_changes[builder].append({zone: devices})

    @staticmethod
    def _get_devices_changed(devices1, devices2):
        devices = {}
        for builder in RING_TYPES:
            devices[builder] = {}
            for zone, devs in devices1[builder].iteritems():
                if zone not in devices2[builder]:
                    devices[builder].setdefault(zone, []).extend(devs)
        return devices

    def _setup_rsyslog(self):
        self.logger = logging.getLogger('capman')
        self.logger.setLevel(logging.DEBUG)
        handler = SysLogHandler(address='/dev/log',
                                facility=SysLogHandler.LOG_LOCAL0)
        fmt = logging.Formatter("%(name)s: %(levelname)s [-] %(message)s")
        handler.setFormatter(fmt)
        self.logger.addHandler(handler)


def _get_ring_devices(ring_type, zone):
    builder = RingBuilder.load(_get_builder_path(ring_type))
    search_values = {}
    search_values['zone'] = int(zone[1:])
    devs = builder.search_devs(search_values)
    list = {}
    for dev in devs:
        list[dev['device']] = dev['weight']
    return list


def _get_builder_path(ring_type):
    return os.path.join('/etc/swift', ring_type + '.builder')


def _get_all_cluster_devices():
    devices = {}
    for builder in RING_TYPES:
        ring_builder = RingBuilder.load(_get_builder_path(builder))
        devices[builder] = {}
        for dev in ring_builder.devs:
            if dev is None:
                continue
            device = {}
            device['ip'] = dev['ip']
            device['weight'] = dev['weight']
            device['device'] = dev['device']
            devices[builder].setdefault('z' + str(dev['zone']),
                                        []).append(device)
    return devices


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


def get_devices(zones, metadata=None):
    devices = {}
    for builder in RING_TYPES:
        devices[builder] = {}
        for zone, nodes in zones.iteritems():
            devices[builder][zone] = []
            for node, disks in nodes.iteritems():
                ringdisks = []
                # Add all disks designated for ringtype
                if isinstance(disks['disks'], dict):
                    if builder in disks['disks']:
                        ringdisks += disks['disks'][builder]
                elif isinstance(disks['disks'], list):
                    ringdisks = disks['disks']
            for ringdisk in ringdisks:
                device = {}
                device['weight'] = None
                device['metadata'] = metadata
                device['device'] = None
                device['ip'] = node
                if not isinstance(ringdisk, dict):
                    device['device'] = ringdisk
                    match = re.match('(.*)\d+$', ringdisk)
                    blockdev = '/dev/%s' % match.group(1)
                    # treat size as weight and serial as metadata
                    weight, serial = get_disk_size_serial(node, blockdev)
                    device['weight'] = weight
                    if not metadata:
                        device['metadata'] = serial
                else:
                    device['device'] = ringdisk['blockdev']
                    device['weight'] = ringdisk['weight']
                devices[builder][zone].append(device)
    return devices


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


def _ringsdef_helper(config, metadata, outputdir):
    ringsdef = SwiftRingsDefinition(config)

    build_script = ringsdef.generate_script(outdir=outputdir,
                                            meta=metadata)
    subprocess.call(build_script)
    tempfiles = os.path.join(ringsdef.workspace, "*")
    execute(_fab_copy_swift_directory, tempfiles, outputdir,
            hosts=ringsdef.nodes)
    return ringsdef.nodes


def manage(args):
    rc = 0
    if not os.path.exists(args.config):
        raise Exception("Could not find confguration file '%s'" % args.config)
    try:
        capman = CapacityManager(args.iterations, args.config)
        capman.update_cluster()
    except Exception as e:
        print >> sys.stderr, "There was an error updating rings: '%s'" % e
        rc = -1
    sys.exit(rc)


def bootstrap(args):
    rc = 0
    if not os.path.exists(args.config):
        raise Exception("Could not find confguration file '%s'" % args.config)
    try:
        config = yaml.load(open(args.config, 'r'))
        ringhosts = _ringsdef_helper(config, args.meta, args.outdir)
        execute(_fab_start_swift_services, hosts=ringhosts)
    except Exception as e:
        print >> sys.stderr, "There was an error bootrapping: '%s'" % e
        rc = -1
    sys.exit(rc)


def main():
    parser = argparse.ArgumentParser(description='Tool to modify swift config')
    subparsers = parser.add_subparsers()

    parser.add_argument('-i', dest='keyfile')
    parser.add_argument('-u', dest='user')
    parser.add_argument('--config', required=True)
    parser.add_argument('--outdir', required=True)

    parser_genconfig = subparsers.add_parser('bootstrap')
    parser_genconfig.add_argument('--meta', default=None)
    parser_genconfig.set_defaults(func=bootstrap)

    parser_manage = subparsers.add_parser('manage')
    parser_manage.add_argument('--iterations', default=10)
    parser_manage.set_defaults(func=manage)

    args = parser.parse_args()
    if args.keyfile:
        env.key_filename = args.keyfile
    if args.user:
        env.user = args.user

    args.func(args)


if __name__ == '__main__':
    main()
