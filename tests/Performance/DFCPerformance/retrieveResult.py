#!/usr/bin/env python

""" This script retrieves the output of all the jobs of a given
    test. <jobName> is the path of the directory created by submitMyJob
"""


from __future__ import print_function
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import sys

from DIRAC.Interfaces.API.Dirac import Dirac

import os

if len(sys.argv)< 2 :
  print("Usage %s <jobName>" % sys.argv[0])
  sys.exit(1)

jobName = sys.argv[1]

finalStatus = ['Done', 'Failed']

dirac = Dirac()

idstr = open("%s/jobIdList.txt"%jobName, 'r').readlines()
ids = map(int, idstr)
print("found %s jobs" % (len(ids)))

res = dirac.getJobSummary(ids)
if not res['OK']:
  print(res['Message'])
  sys.exit(1)

metadata = res['Value']

for jid in ids:
  jobMeta = metadata.get( jid, None )
  if not jobMeta :
    print("No metadata for job ", jid)
    continue

  status = jobMeta['Status']
  print("%s %s" % (jid, status))
  if status  in finalStatus:
    outputDir =  '%s/%s'%(jobName,status)
    if not os.path.exists( "%s/%s" % ( outputDir, jid ) ):
      print("Retrieving sandbox")
      res = dirac.getOutputSandbox( jid, outputDir = outputDir )
