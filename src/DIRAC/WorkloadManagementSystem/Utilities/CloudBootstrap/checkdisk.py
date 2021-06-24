#!/usr/bin/env python

from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import sys
import os
from subprocess import Popen, PIPE


def getstatusoutput(cmd):
  try:
    inst = Popen(cmd, stdout=PIPE, stderr=PIPE)
    output, _ = inst.communicate()
    status = inst.returncode
    return status, output
  except EnvironmentError:
    # Missing executable
    return -1, ""


def getMountedPartitions():

  status, output = getstatusoutput(['disk', '-h'])

  partitionList = []
  for line in output.split('\n'):
    if line.startswith('/dev'):
      part = os.path.basename(line.split()[0])
      size = line.split()[1].rstrip('G')
      mount = line.split()[5]
      partitionList.append((part, size, mount))

  return partitionList


def getBlockDevices():

  suitableDevs = ['vda', 'vdb', 'sdb', 'hdb', 'xvdb']
  devListDir = os.listdir('/dev')
  devList = []
  for dev in devListDir:
    for pattern in suitableDevs:
      if dev.startswith(pattern):
        devList.append(dev)

  return devList


def getPartitionSize(partition):
  """ Return the partition size in GiB
  """

  status, output = getstatusoutput(['fdisk', '-l', partition])
  if status != 0:
    return -1

  size = -1
  for line in output.split('\n'):
    if line.startswith('Disk'):
      size = float(line.split()[4]) / 1024. / 1024. / 1024.
      break

  return '%.1f' % size


def getRootPartition():

  partList = getMountedPartitions()
  for part, size, mount in partList:
    if mount == '/':
      return part
  return None


def checkPartition(partition):

  partSize = getPartitionSize(partition)
  freeSize = getPartitionSize(partition) - getPartitionSize('%s1' % partition) - getPartitionSize('%s2' % partition)
  if float(freeSize) / float(partSize) > 0.5:
    mountedList = [p[0] for p in getMountedPartitions()]
    if '%s1' % partition not in mountedList and '%s2' % partition not in mountedList:
      return partition
    if '%s2' % partition not in mountedList:
      pass


def getScratchDevice():

  # get unmounted partitions
  partList = getMountedPartitions()
  devList = getBlockDevices()

  unmountedList = []
  for dev in devList:
    if dev not in [p[0] for p in partList] + ['vda', 'vdb']:
      unmountedList.append(dev)

  freevda = getPartitionSize('vda') - getPartitionSize('vda1') - getPartitionSize('vda2')
  freevdb = getPartitionSize('vdb') - getPartitionSize('vdb1') - getPartitionSize('vdb2')

  maxDev = None
  maxSize = 0.
  for dev in unmountedList:
    if getPartitionSize(dev) > maxSize:
      maxSize = getPartitionSize(dev)
      maxDev = dev


if __name__ == "__main__":
  partition = sys.argv[1]
  size = getPartitionSize(partition)
  print(size)
  partList = getMountedPartitions()
  print(partList)
  devList = getBlockDevices()
  print(devList)
  rootPart = getRootPartition()
  print(rootPart)
