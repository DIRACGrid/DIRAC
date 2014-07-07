from DIRAC import gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.DataManagementSystem.private.FTS2.FTS2Placement import FTS2Placement
from DIRAC.DataManagementSystem.private.FTS3.FTS3Placement import FTS3Placement

import os

def FTSPlacement( csPath = None, **kwargs ):
  """ Instantiate the proper FTSPlacement depending
      on the Operations/DataManagement/FTSVersion flag
   """

  if not csPath:
    csPath = 'DataManagement/FTSVersion'

  ftsVersion = Operations().getValue( csPath, 'FTS2' )
  gLogger.debug( "FTSPlacement: using version %s" % ftsVersion )

  csPath = os.path.join( csPath, ftsVersion )
  if ftsVersion == 'FTS2':
    return FTS2Placement( csPath, **kwargs )
  elif ftsVersion == 'FTS3':
    return FTS3Placement( csPath, **kwargs )
  else:
    raise Exception( 'FTSPlacement: version %s of FTS is not supported' % ftsVersion )

