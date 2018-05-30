#!/usr/bin/env python

"""
  Create a transformation as second step of a demo production
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
from DIRAC.DataManagementSystem.Client.MetaQuery import MetaQuery

transClient = TransformationClient()

# get arguments
args = Script.getPositionalArgs()
if ( len( args ) != 1 ):
  Script.showHelp()

transName = args[0]

### Create an Analysis transformation with an input meta query
inputquery = MetaQuery( {'metaData1':'metaDataValue1', 'metaData2': 1} )
inputquery = inputquery.getMetaQueryAsJson()

res = transClient.addTransformation( transName, 'description', 'longDescription', 'Analysis', 'Standard',
                                              'Manual', '', inputMetaQuery=inputquery )

if not res['OK']:
  DIRAC.gLogger.error( res['Message'] )
  DIRAC.exit( -1 )

transID = res['Value']

DIRAC.gLogger.notice( 'Transformation %s successfully created with InputMetaQuery %s' % ( transID, inputquery  ) )

DIRAC.exit( 0 )


