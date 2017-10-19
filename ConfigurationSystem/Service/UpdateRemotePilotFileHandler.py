"""
  Module that update the pilot json file according to the update of the CS information.
  It also uploads the last version of the pilot scripts to the web server defined in the dirac.cfg.
"""

__RCSID__ = '$Id: $'

from DIRAC import gConfig, S_OK
from DIRAC.Core.DISET.RequestHandler import RequestHandler, getServiceOption
from DIRAC.WorkloadManagementSystem.Utilities.Pilot3Synchronizer import Pilot3Synchronizer


def initializeUpdateRemotePilotFileHandler( _serviceInfo ):
  '''
    Handler initialization.
    The service options need to be defined in the dirac.cfg.
  '''

  paramDict = {}
  paramDict['pilotFileServer'] = getServiceOption( _serviceInfo, "pilotFileServer", '' )
  paramDict['pilotRepo'] = getServiceOption( _serviceInfo, "pilotRepo", '' )
  paramDict['pilotVORepo'] = getServiceOption( _serviceInfo, "pilotVORepo", '' )
  paramDict['projectDir'] = getServiceOption( _serviceInfo, "projectDir", '' )
  paramDict['pilotVOScriptPath'] = getServiceOption( _serviceInfo, "pilotVOScriptPath", '' )
  paramDict['pilotScriptsPath'] = getServiceOption( _serviceInfo, "pilotScriptsPath", '' )

  syncObject = Pilot3Synchronizer( paramDict )
  gConfig.addListenerToNewVersionEvent( syncObject.sync )

  return S_OK()

class UpdateRemotePilotFileHandler( RequestHandler ):
  """ No functions are exposed
  """

  pass
