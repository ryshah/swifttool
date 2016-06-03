swifttool
=========
a tool to help us properly boostrap and manage a [swift](https://github.com/openstack/swift) environment.

installation
------------
```
$ pip install -r requirements.txt
$ python setup.py install
```

testing
-------
```
$ tox
```

ring definition file
--------------------

Sample ring_definition.yml to use SSD for account/container rings and SATA for object ring:

```
---
part_power: 13
replicas: 3
min_part_hours: 1
zones:
  z1:
    node1:
      disks:
        container:
          - sdb1
          - sdc1
        account:
          - sdb1
          - sdc1
        object:
          - sdd1
          - sde1
          - sdf1
          - sdg1
```


Sample ring_definition.yml where disks belong to all rings:

```
---
part_power: 13
replicas: 3
min_part_hours: 1
zones:
  z1:
    node1:
      disks:
        - sdb1
        - sdc1
        - sdd1
        - sde1
        - sdf1
        - sdg1
```

license
-------
this is released under the Apache license
