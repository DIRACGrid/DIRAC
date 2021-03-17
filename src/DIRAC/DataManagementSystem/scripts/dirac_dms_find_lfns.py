#!/usr/bin/env python
"""
Find files in the FileCatalog using file metadata

Examples:
  $ dirac-dms-find-lfns Path=/lhcb/user "Size>1000" "CreationDate<2015-05-15"
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

from DIRAC.Core.Utilities.DIRACScript import DIRACScript

@DIRACScript()
def main(self):
  import DIRAC
  from DIRAC import gLogger
  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
  from DIRAC.DataManagementSystem.Client.MetaQuery import MetaQuery, FILE_STANDARD_METAKEYS
  from DIRAC.DataManagementSystem.Utilities.DMSHelpers import resolveSEGroup

  self.registerSwitch('', 'Path=', '    Path to search for')
  self.registerSwitch('', 'SE=', '    (comma-separated list of) SEs/SE-groups to be searched')
  self.registerArgument(['metaspec: metadata index specification (of the form: \
                            "meta=value" or "meta<value", "meta!=value", etc.)'], mandatory=False)
  self.parseCommandLine(ignoreErrors=True)
  args = self.getPositionalArgs()

  path = '/'
  seList = None
  for opt, val in self.getUnprocessedSwitches():
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
    print("Error: No argument provided\n%s:" % self.scriptName)
    gLogger.notice("MetaDataDictionary: \n%s" % str(typeDict))
    self.showHelp(exitCode=1)

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
  main()  # pylint: disable=no-value-for-parameter
