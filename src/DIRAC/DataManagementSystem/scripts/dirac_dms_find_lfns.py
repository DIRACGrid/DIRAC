#!/usr/bin/env python
"""
Find files in the FileCatalog using file metadata

Usage:
  dirac-dms-find-lfns [options] metaspec [metaspec ...]

Arguments:
  metaspec: metadata index specification (of the form: "meta=value" or "meta<value", "meta!=value", etc.)

Examples::

  $ dirac-dms-find-lfns Path=/lhcb/user "Size>1000" "CreationDate<2015-05-15"
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

from DIRAC.Core.Utilities.DIRACScript import DIRACScript

@DIRACScript()
def main():
  from DIRAC.Core.Base import Script

  Script.registerSwitch('', 'Path=', '    Path to search for')
  Script.registerSwitch('', 'SE=', '    (comma-separated list of) SEs/SE-groups to be searched')
  Script.parseCommandLine(ignoreErrors=True)
  args = Script.getPositionalArgs()

  import DIRAC
  from DIRAC import gLogger
  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
  from DIRAC.DataManagementSystem.Client.MetaQuery import MetaQuery, FILE_STANDARD_METAKEYS
  from DIRAC.DataManagementSystem.Utilities.DMSHelpers import resolveSEGroup

  path = '/'
  seList = None
  for opt, val in Script.getUnprocessedSwitches():
    if opt == 'Path':
      path = val
    elif opt == 'SE':
      seList = resolveSEGroup(val.split(','))

  if seList:
    args.append("SE=%s" % ','.join(seList))
  fc = FileCatalog()
  result = fc.getMetadataFields()
  if not result['OK']:
    gLogger.error('Can not access File Catalog:', result['Message'])
    DIRAC.exit(-1)
  typeDict = result['Value']['FileMetaFields']
  typeDict.update(result['Value']['DirectoryMetaFields'])
  # Special meta tags
  typeDict.update(FILE_STANDARD_METAKEYS)

  if len(args) < 1:
    print("Error: No argument provided\n%s:" % Script.scriptName)
    gLogger.notice("MetaDataDictionary: \n%s" % str(typeDict))
    Script.showHelp(exitCode=1)

  mq = MetaQuery(typeDict=typeDict)
  result = mq.setMetaQuery(args)
  if not result['OK']:
    gLogger.error("Illegal metaQuery:", result['Message'])
    DIRAC.exit(-1)
  metaDict = result['Value']
  path = metaDict.pop('Path', path)

  result = fc.findFilesByMetadata(metaDict, path)
  if not result['OK']:
    gLogger.error('Can not access File Catalog:', result['Message'])
    DIRAC.exit(-1)
  lfnList = sorted(result['Value'])

  gLogger.notice('\n'.join(lfn for lfn in lfnList))


if __name__ == "__main__":
  main()
