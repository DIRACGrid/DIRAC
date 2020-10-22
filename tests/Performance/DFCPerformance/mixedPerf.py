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
      * proportions: the proportions between reads (r), inserts (i) and delete (d)
                    Note than before doing a delete, we do a read to know what to delete
      * readDepth: depth of the path when reading
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
servAddress = 'dips://%s:%s/DataManagement/FileCatalog' %(hostname, port)

# Max duration of the test in seconds
maxDuration = 1800 # 30mn

fc = FileCatalogClient(servAddress)

storageElements = ['se0', 'se1', 'se2', 'se3', 'se4', 'se5', 'se6', 'se7', 'se8', 'se9']
#proportions = [ ('r',37), ('i',2), ('d', 2) ]
proportions = [ ('r',80), ('i',2), ('d', 2) ]


# lfc size = 9, huge db small req = 12, huge db big = 6
readDepth = 12
# lfc size 13, huge 7 
writeDepth = 7
maxInsert = 60

timeFile = open('time.txt', 'w')
clockFile = open('clock.txt', 'w')
timeFile.write("QueryStart\tQueryEnd\tQueryTime\textra(port %s)\n"%port)
clockFile.write("QueryStart\tQueryEnd\tQueryClock\textra(port %s)\n"%port)

start = time.time()

done = False


def generatePath(depth, withLetters=False):
  # Between 0 and 3 because in generate we have 4 subdirs per dir. Adapt :-)
  rndTab = [random.randint(0, 3) for _i in range(depth)]
  if withLetters:
    rndLetters = [random.choice(string.ascii_letters) for _i in range(3)]
    rndTab += rndLetters
  dirPath = '/' + '/'.join(map(str, rndTab))
  return dirPath


def doRead(depth):
  dirPath = generatePath(depth)
  before = time.time()
  beforeC = time.clock()
  res = fc.listDirectory(dirPath)
  afterC = time.clock()
  after = time.time()
  queryTime = after - before
  queryTimeC = afterC - beforeC
  extra = "list "
  lfnDict = None
  if not res['OK']:
    extra += res['Message']
  else:
    #print "RES %s"%res
    out = res['Value']['Successful'][dirPath]
    lfnDict = out['Files']
    extra += "%s %s %s"%(dirPath, len(out['Files']), len(out['SubDirs']))
  
  

  return before, after, queryTime, beforeC, afterC, queryTimeC, extra, lfnDict





def doInsert(depth, maxFile):

  dirPath = generatePath(depth, True)
  nbOfFiles = random.randint(1,maxFile)
  lfnDict = {}  
  for f in range(nbOfFiles):
    filename = "%s.txt"%(f)
    lfn = "%s/%s"%(dirPath, filename)
    size = random.randint(1,1000)
    se = random.choice(storageElements)
    guid = ("%s%s"%(dirPath,filename))[:36]
    checksum = guid[:32]
    lfnDict[ lfn ] = { 'PFN' : lfn, 'SE' : se, 'Size' :  size, 'GUID' : guid, 'Checksum' : checksum}


  beforeI = time.time()
  beforeCI = time.clock()
#  res = {'OK' : False, 'Message' : 'not executed' }
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

  return beforeI, afterI, queryInsertTime, beforeCI, afterCI, queryInsertTimeC, extra


def doRemove(lfnDict):
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
    extra += "%s %s %s"%(len(lfnDict), len(res['Value'].get('Successful', [])), len(res['Value'].get('Failed', [])))

  return beforeR, afterR, queryRemoveTime, beforeCR, afterCR, queryRemoveTimeC, extra



#choices = [ (value, weight) ] 
def weighted_choice(choices):
  total = sum(w for c, w in choices)
  r = random.uniform(0, total )
  upto = 0
  for c, w in choices:
    if upto + w > r:
      return c
    upto += w

  assert False, "Shouldn't get here"

while not done:

  action = weighted_choice(proportions)

  if action == 'r':
    before, after, queryTime, beforeClock, afterClock, queryClock, extra, lfnDict = doRead(readDepth)
  elif action == 'i':
    before, after, queryTime, beforeClock, afterClock, queryClock, extra = doInsert(writeDepth, maxInsert)
  elif action == 'd':
    before, after, queryTime, beforeClock, afterClock, queryClock, extra, lfnDict = doRead(readDepth)
    before, after, queryTime, beforeClock, afterClock, queryClock, extra  = doRemove(lfnDict)



  timeFile.write("%s\t%s\t%s\t%s\n"%(before, after, queryTime, extra))
  timeFile.flush()
  os.fsync(timeFile)
  clockFile.write("%s\t%s\t%s\t%s\n"%(beforeClock, afterClock, queryClock, extra))
  clockFile.flush()
  os.fsync(clockFile)


  if ( time.time() - start ) > maxDuration:
    done = True

timeFile.close()
clockFile.close()
