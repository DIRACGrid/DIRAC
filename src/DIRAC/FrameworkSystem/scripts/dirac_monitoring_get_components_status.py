#!/usr/bin/env python
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

import sys
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript

__RCSID__ = "$Id$"


@DIRACScript()
def main():
  Script.parseCommandLine(ignoreErrors=True)
  args = Script.getPositionalArgs()

  fieldsToShow = ('ComponentName', 'Type', 'Host', 'Port', 'Status', 'Message')

  from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor

  result = gMonitor.getComponentsStatusWebFormatted(sortingList=[['ComponentName', 'ASC']])
  if not result['OK']:
    print("ERROR: %s" % result['Message'])
    sys.exit(1)
  paramNames = result['Value']['ParameterNames']
  records = result['Value']['Records']
  fieldLengths = []
  for param in paramNames:
    fieldLengths.append(len(param))

  for record in records:
    for i, _ in enumerate(record):
      if paramNames[i] in fieldsToShow:
        fieldLengths[i] = max(fieldLengths[i], len(str(record[i])))
  # Print time!
  line = []
  sepLine = []
  for i, param in enumerate(paramNames):
    if param in fieldsToShow:
      line.append("%s%s" % (param, " " * (fieldLengths[i] - len(param))))
      sepLine.append("-" * fieldLengths[i])
  print("|".join(line))
  sepLine = "+".join(sepLine)
  print(sepLine)
  for record in records:
    line = []
    for i, _ in enumerate(record):
      if paramNames[i] in fieldsToShow:
        val = str(record[i])
        line.append("%s%s" % (val, " " * (fieldLengths[i] - len(val))))
    print("|".join(line))
    # print sepLine


if __name__ == "__main__":
  main()
