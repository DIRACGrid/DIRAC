#!/usr/bin/env python

""" This script instantiate a DFC client against a given service,
    and hammers it with mixed request (read/write/delete) for a given time.
    It produces two files : time.txt and clock.txt which contain time measurement,
    using time.time and time.clock (see respective doc)
    It assumes that the DB has been filled with the scripts in generateDB

    Tunable parameters:
      * maxDuration : time it will run. Cannot be too long, otherwise job
                      is killed because staled
      * port: list of ports on which we can find a service (assumes all the service running on one machine)
      * hostname: name of the host hosting the service
      * storageElements: list of storage element names
      * writeDepth: depth of the path when writing
      * maxInsert: max number of files inserted at the same time

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
import string
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

port = random.choice([9196, 9197,9198, 9199])
hostname = 'yourmachine.somewhere.something'
servAddress = 'dips://%s:%s/DataManagement/FileCatalog' % ( hostname, port )

maxDuration = 1800  # 30mn
maxInsert = 10
fc = FileCatalogClient(servAddress)

storageElements = ['se0', 'se1', 'se2', 'se3', 'se4', 'se5', 'se6', 'se7', 'se8', 'se9']

writeDepth = 13

fl = open('time.txt', 'w')
fl2 = open('clock.txt', 'w')
fl.write("QueryStart\tQueryEnd\tQueryTime\textra(port %s)\n"%port)
fl2.write("QueryStart\tQueryEnd\tQueryClock\textra(port %s)\n"%port)

start = time.time()

done = False

while not done:
  # Between 0 and 3 because in generate we have 4 subdirs per dir. Adapt :-)
  rndTab = [random.randint(0, 3) for i in range(writeDepth)]
  rndLetters = [random.choice(string.ascii_letters) for i in range(3)]
  rndTab += rndLetters
  dirPath = '/' + '/'.join(map(str, rndTab))
  nbOfFiles = random.randint(1, maxInsert)
  lfnDict = {}
  for f in range(nbOfFiles):
    filename = "%s.txt"%(f)
    lfn = "%s/%s"%(dirPath, filename)
    size = random.randint(1,1000)
    se = random.choice(storageElements)
    guid = "%s%s"%(''.join(map(str,rndTab)),filename)
    checksum = guid
    lfnDict[ lfn ] = { 'PFN' : lfn, 'SE' : se, 'Size' :  size, 'GUID' : guid, 'Checksum' : checksum}


  beforeI = time.time()
  beforeCI = time.clock()
  res = fc.addFile(lfnDict)
  afterCI = time.clock()
  afterI = time.time()
  queryInsertTime = afterI - beforeI
  queryInsertTimeC = afterCI - beforeCI
  extra = "insert "
  if not res['OK']:
    extra += res['Message']
  else:
    extra += "%s %s %s"%(len(lfnDict), len(res['Value'].get('Successful', [])), len(res['Value'].get('Failed', [])))
    pass

  fl.write("%s\t%s\t%s\t%s\n"%(beforeI, afterI, queryInsertTime, extra))
  fl.flush()
  os.fsync(fl)
  fl2.write("%s\t%s\t%s\t%s\n"%(beforeCI, afterCI, queryInsertTimeC, extra))
  fl2.flush()
  os.fsync(fl2)

  beforeR = time.time()
  beforeCR = time.clock()
  res = fc.removeFile(lfnDict)
  afterCR = time.clock()
  afterR = time.time()
  queryRemoveTimeC = afterCR - beforeCR
  queryRemoveTime = afterR - beforeR
  extra = "remove "
  if not res['OK']:
    extra += res['Message']
  else:
    extra += "%s %s %s"%(nbOfFiles, len(res['Value'].get('Successful', [])), len(res['Value'].get('Failed', [])))
    pass
  
  fl.write("%s\t%s\t%s\t%s\n"%(beforeR, afterR, queryRemoveTime, extra))
  fl.flush()
  os.fsync(fl)
  fl2.write("%s\t%s\t%s\t%s\n"%(beforeCR, afterCR, queryRemoveTimeC, extra))
  fl2.flush()
  os.fsync(fl2)
  if (time.time() - start > maxDuration):
    done = True

fl.close()
fl2.close()
