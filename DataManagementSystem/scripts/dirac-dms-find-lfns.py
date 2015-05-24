#!/usr/bin/env python

"""
Find files in the FileCatalog using file metadata
"""

import DIRAC

if __name__ == "__main__":

  from DIRAC.Core.Base import Script

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [options] metaspec [metaspec ...]' % Script.scriptName,
                                       'Arguments:',
                                       ' metaspec:    metadata index specification (of the form: "meta=value" or "meta<value", "meta!=value", etc.)',
                                       '', 'Examples:',
                                       '  $ dirac-dms-find-lfns Path=/lhcb/user "Size>1000" "CreationDate<2015-05-15"',
                                       ] )
                          )

  Script.parseCommandLine( ignoreErrors = True )
  args = Script.getPositionalArgs()

  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
  from DIRAC.DataManagementSystem.Client.MetaQuery import MetaQuery, FILE_STANDARD_METAKEYS
  from DIRAC import gLogger

  if len( args ) < 1:
    print "Error: No argument provided\n%s:" % Script.scriptName
    Script.showHelp( )
    DIRAC.exit( -1 )

  fc = FileCatalog()
  result = fc.getMetadataFields()
  if not result['OK']:
    gLogger.error( 'Can not access File Catalog:', result['Message'] )
    DIRAC.exit( -1 )
  typeDict = result['Value']['FileMetaFields']
  typeDict.update(result['Value']['DirectoryMetaFields'])
  # Special meta tags
  typeDict.update( FILE_STANDARD_METAKEYS )

  mq = MetaQuery( typeDict = typeDict )
  result = mq.setMetaQuery( args )
  if not result['OK']:
    gLogger.error( "Illegal metaQuery:", result['Message'] )
    DIRAC.exit( -1 )
  metaDict = result['Value']
  path = metaDict.get( 'Path', '/' )
  metaDict.pop( 'Path' )

  print metaDict

  result = fc.findFilesByMetadata( metaDict, path )
  if not result['OK']:
    gLogger.error( 'Can not access File Catalog:', result['Message'] )
    DIRAC.exit( -1 )
  lfnList = result['Value']

  for lfn in lfnList:
    gLogger.notice( lfn )
