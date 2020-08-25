#!/usr/bin/env python
""" This script instantiate a DFC client against a given service,
    and hammers it with read request (listDirectory) for a given time.
    It produces two files : time.txt and clock.txt which contain time measurement,
    using time.time and time.clock (see respective doc)
    It assumes that the DB has been filled with the scripts in generateDB

    Tunable parameters:
      * maxDuration : time it will run. Cannot be too long, otherwise job
                      is killed because staled
      * port: list of ports on which we can find a service (assumes all the service running on one machine)
      * hostname: name of the host hosting the service
      * readDepth: depth of the path when reading


The depths are to be put in relation with the depths you used to generate the db
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
import os
import random
import time

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

port = random.choice([9196, 9197,9198, 9199])
hostname = 'yourmachine.somewhere.something'
servAddress = 'dips://%s:%s/DataManagement/FileCatalog' % ( hostname, port )

maxDuration = 1800  # 30mn

fc = FileCatalogClient(servAddress)

# lfc size = 9, huge db small req = 12, huge db big = 6
readDepth = 12

f = open('time.txt', 'w')
f2 = open('clock.txt', 'w')
f.write("QueryStart\tQueryEnd\tQueryTime\textra(port %s)\n"%port)
f2.write("QueryStart\tQueryEnd\tQueryClock\textra(port %s)\n"%port)

start = time.time()

done = False

while not done:
  # Between 0 and 3 because in generate we have 4 subdirs per dir. Adapt :-)
  rndTab = [random.randint(0, 3) for i in range(readDepth)]
  dirPath = '/' + '/'.join(map(str,rndTab))
  before = time.time()
  beforeC = time.clock()
  res = fc.listDirectory(dirPath)
  afterC = time.clock()
  after = time.time()
  queryTime = after - before
  queryTimeC = afterC - beforeC
  if not res['OK']:
    extra = res['Message']
  else:
    out = res['Value']['Successful'][dirPath]
    extra = "%s %s %s"%(dirPath, len(out['Files']), len(out['SubDirs']))

  f.write("%s\t%s\t%s\t%s\n"%(before, after, queryTime, extra))
  f.flush()
  os.fsync(f)
  f2.write("%s\t%s\t%s\t%s\n"%(beforeC, afterC, queryTimeC, extra))
  f2.flush()
  os.fsync(f2)
  if (time.time() - start > maxDuration):
    done = True

f.close()
f2.close()

