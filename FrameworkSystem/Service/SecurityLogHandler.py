########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/Service/Attic/SecurityLogHandler.py,v 1.1 2008/09/30 19:01:01 acasajus Exp $
########################################################################

""" ProxyManager is the implementation of the ProxyManagement service
    in the DISET framework
"""

__RCSID__ = "$Id: SecurityLogHandler.py,v 1.1 2008/09/30 19:01:01 acasajus Exp $"

import types
import os
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR, gConfig, rootPath
from DIRAC.FrameworkSystem.private.SecurityFileLog import SecurityFileLog
from DIRAC.FrameworkSystem.Client.SecurityLogClient import SecurityLogClient

gSecurityFileLog = False

def initializeSecurityLogHandler( serviceInfo ):
  global gSecurityFileLog

  serviceCS = serviceInfo [ 'serviceSectionPath' ]
  dataPath = gConfig.getValue( "%s/DataLocation" % serviceCS, "data/securityLog" )
  dataPath = dataPath.strip()
  if "/" != dataPath[0]:
    dataPath = os.path.realpath( "%s/%s" % ( rootPath, dataPath ) )
  gLogger.info( "Data will be written into %s" % dataPath )
  try:
    os.makedirs( dataPath )
  except:
    pass
  try:
    testFile = "%s/seclog.jarl.test" % dataPath
    fd = file( testFile, "w" )
    fd.close()
    os.unlink( testFile )
  except IOError:
    gLogger.fatal( "Can't write to %s" % dataPath )
    return S_ERROR( "Data location is not writable" )
  #Define globals
  gSecurityFileLog = SecurityFileLog( dataPath )
  SecurityLogClient().setLogStore( gSecurityFileLog )
  return S_OK()

class SecurityLogHandler( RequestHandler ):

  types_logAction = [ ( types.ListType, types.TupleType ) ]
  def export_logAction( self, secMsg ):
    """ Log a single action
    """
    result = gSecurityFileLog.logAction( secMsg )
    if not result[ 'OK' ]:
      return S_OK( [ ( secMsg, result[ 'Message' ] ) ] )
    return S_OK()

  types_logActionBundle = [ ( types.ListType, types.TupleType ) ]
  def export_logActionBundle( self, secMsgList ):
    """ Log a list of actions
    """
    errorList = []
    for secMsg in secMsgList:
      result = gSecurityFileLog.logAction( secMsg )
      if not result[ 'OK' ]:
        errorList.append( ( secMsg, result[ 'Message' ] ) )
    if errorList:
      return S_OK( errorList )
    return S_OK()