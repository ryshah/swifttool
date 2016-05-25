import copy
import logging
import math
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import yaml
from logging.handlers import SysLogHandler
from utils import get_devices, RING_TYPES, env
from swift.common.ring import RingBuilder


class CapacityManager(object):

    def __init__(self, iterations=10,
                 def_file='/etc/swift/ring_definition.yml',
                 cluster_changes=None, cluster_config=None):
        self.iterations = iterations
        env.key_filename = '/home/swiftops/.ssh/id_rsa'
        env.user = 'swiftops'
        if cluster_changes is None or cluster_config is None:
            self.cluster_changes, self.cluster_config = \
                calculate_cluster_changes(def_file, iterations)
        else:
            self.cluster_changes = cluster_changes
            self.cluster_config = cluster_config
        self.logger = _setup_rsyslog()
        
    def update_cluster(self):
        self.logger.info("Starting to update the cluster")
        for iteration in range(0, self.iterations):
            done = self._update_cluster_nodes()
            if done:
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
                        ip = _get_zone_ip(devices)
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
                                Setting the %s device capacity to %d", 
                                builder, devname, str(devinfo['weight']))
                            newdevs.append(devinfo)
                            changed = True
                        updated_cfg['zones'][zone][ip]['disks'][builder] = \
                            newdevs
            if changed:
                fid, tmpfile = tempfile.mkstemp()
                os.fchmod(fid, stat.S_IRUSR | stat.S_IWUSR |
                          stat.S_IRGRP | stat.S_IROTH)
                self.logger.debug("Running swifttool to update rings")
                command_args = ['sudo', '-u', 'swiftops',
                                '/usr/local/bin/swifttool', '-i',
                                '/home/swiftops/.ssh/id_rsa', 'bootstrap',
                                '--outdir', '/etc/swift', '--config', tmpfile]
                with open(tmpfile, 'w') as stream:
                    yaml.dump(updated_cfg, stream, default_flow_style=False,
                              explicit_start=True)
                rc = subprocess.check_call(command_args)
                if rc == -1:
                    print "Error updating the cluster rings"
                    sys.exit(-1)
            return changed
        except Exception as e:
            print "Error updating cluster configuration", e
            sys.exit(-1)
        finally:
            os.unlink(tmpfile)


def _get_zone_ip(devs):
    for dev in devs:
        if 'ip' in dev:
            return dev['ip']


def calculate_cluster_changes(def_file, iterations):
    try:
        ring_def = yaml.load(open(def_file, 'r'))
        expected_config = get_devices(ring_def['zones'])
        current_config = _get_ring_devices()
        # To get devices to be removed we look for devices present in
        # current_config and not in expected_config
        # To get list of devices to be added look for devices in expected
        # not in current
        additions = _get_devices_changed(expected_config, current_config)
        removals = _get_devices_changed(current_config, expected_config)
        # Reconcile the two structures in to single
        # Each ring will be list of devices to be modified
        # Each device (in ring) will have following attributes:
        # - zone
        # - ip
        # - device
        # - target (device weight when adding, 0 when removing)
        # - step (amount to change every time till target reached)
        # step is defaulted to 10% or -10% of target
        reconciled = {}
        _calculate_changes(reconciled, additions, True, iterations)
        _calculate_changes(reconciled, removals, False, iterations)
        return reconciled, ring_def
    except Exception as e:
        print "Error getting device info", e
        sys.exit(-1)


def _calculate_changes(destination, source, add_operation=True, iterations=10):
    for builder in RING_TYPES:
        for zone, devadd in source[builder].items():
            destination.setdefault(builder, [])
            devices = []
            for device in devadd:
                dev = {}
                dev['ip'] = device['ip']
                dev['device'] = device['device']
                if add_operation:
                    dev['target'] = device['weight']
                    dev['step'] = int(math.ceil(device['weight'] /
                                                iterations))
                else:
                    dev['target'] = 0
                    dev['step'] = int(-1 * math.ceil(device['weight'] /
                                                     iterations))
                devices.append(dev)
            destination[builder].append({zone: devices})


def _get_devices_changed(devices1, devices2):
    devices = {}
    for builder in RING_TYPES:
        devices[builder] = {}
        for zone, devs in devices1[builder].iteritems():
            if zone not in devices2[builder]:
                devices[builder].setdefault(zone, []).extend(devs)
    return devices


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


def _setup_rsyslog(name='capman'):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = SysLogHandler(address='/dev/log',
                            facility=SysLogHandler.LOG_LOCAL0)
    fmt = logging.Formatter("%(name)s: %(levelname)s [-] %(message)s")
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    return logger


def _get_ring_devices():
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
