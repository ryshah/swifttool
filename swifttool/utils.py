import os
import re
import sys
import yaml
from fabric.api import task, execute, hide, sudo
from swift.common.ring import RingBuilder

BUILDER_TYPES = ['account', 'container', 'object']


def get_expected_cluster_changes(def_file='/etc/swift/ring_definition.yml'):
    try:
        ring_def = yaml.load(open(def_file, 'r'))
        expected_config = parse_ring_disks_info(ring_def['zones'],
                                                calculate_disks_size=False)
        current_config = _get_existing_ring_devices()
        # To get devices to be removed we look for devices present in
        # current_config and not in expected_config
        # To get list of devices to be added look for devices in expected
        # not in current
        additions = _get_device_diff(expected_config, current_config)
        removals = _get_device_diff(current_config, expected_config)
        return (additions, removals)
    except Exception as e:
        print "Error getting device info", e
        sys.exit(-1)


def _get_device_diff(devices1, devices2):
    devices = {}
    for builder in BUILDER_TYPES:
        devices[builder] = {}
        for zone, devs in devices1[builder].iteritems():
            if zone not in devices2[builder]:
                devices[builder].setdefault(zone, []).extend(devs)
    return devices


def parse_ring_disks_info(zones, metadata=None, calculate_disks_size=True):
    devices = {}
    for builder in BUILDER_TYPES:
        devices[builder] = {}
        for zone, nodes in zones.iteritems():
            devices[builder][zone] = []
            for node, disks in nodes.iteritems():
                ringdisks = []
                # Add all disks designated for ringtype
                if isinstance(disks['disks'], dict):
                    if builder in disks['disks']:
                        ringdisks += disks['disks'][builder]
                    else:
                        raise Exception("Malformed ring_defintion.yml. "
                                        "Unknown ringtype: %s" % ringtype)
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
                    if calculate_disks_size:
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


def _get_existing_ring_devices():
    devices = {}
    for builder in BUILDER_TYPES:
        builder_path = os.path.join('/etc/swift',
                                    builder + ".builder")
        ring_builder = RingBuilder.load(builder_path)
        devices[builder] = {}
        for dev in ring_builder.devs:
            device = {}
            device['ip'] = dev['ip']
            device['weight'] = dev['weight']
            device['device'] = dev['device']
            devices[builder].setdefault('z' + str(dev['zone']),
                                        []).append(device)
    return devices

_host_lshw_output = {}


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


@task
def get_disk_size_serial(ip, blockdev):
    with hide('running', 'stdout', 'stderr'):
        out = execute(_fab_get_disk_size_serial, ip, blockdev, hosts=[ip])
        return out[ip]
