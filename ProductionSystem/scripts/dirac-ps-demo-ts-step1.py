#!/usr/bin/env python

"""
  Create a transformation as first step of a demo production
"""

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s transName' % Script.scriptName,
                                     'Arguments:',
                                     '  transName: Transformation Name'
                                     ] ) )


Script.parseCommandLine()

from DIRAC.TransformationSystem.Client.TransformationClient   import TransformationClient
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.DataManagementSystem.Client.MetaQuery import MetaQuery

transClient = TransformationClient()
fc = FileCatalog()

# get arguments
args = Script.getPositionalArgs()
if ( len( args ) != 1 ):
  Script.showHelp()

transName = args[0]

# ## Add metadata fields to the DFC
MDFieldDict = {'metaData1':'VARCHAR(128)', 'metaData2':'int' }
for MDField in MDFieldDict:
   MDFieldType = MDFieldDict[MDField]
   res = fc.addMetadataField( MDField, MDFieldType )
   if not res['OK']:
     DIRAC.gLogger.error( res['Message'] )
     DIRAC.exit ( -1 )

### Create a MCSimulation transformation with an output meta query
outputquery = MetaQuery( {'metaData1':'metaDataValue1', 'metaData2': 1} )
outputquery = outputquery.getMetaQueryAsJson()

res = transClient.addTransformation( transName, 'description', 'longDescription', 'MCSimulation', 'Standard',
                                              'Manual', '', outputMetaQuery=outputquery )

if not res['OK']:
  DIRAC.gLogger.error( res['Message'] )
  DIRAC.exit( -1 )

transID = res['Value']

DIRAC.gLogger.notice( 'Transformation %s successfully created with OutputMetaQuery %s' % ( transID, outputquery  ) )

DIRAC.exit( 0 )


