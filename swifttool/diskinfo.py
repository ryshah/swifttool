from fabric.api import task, env, execute, hide, parallel, put, run, settings, sudo
import re

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
