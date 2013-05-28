#
# DONT KNOW WHAT IT IS, CONFISING ABANDONWARE, SHOULD BE REMOVED!!!
# K.C.
#

# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/DataManagementSystem/Agent/TransferAgent.py $
__RCSID__ = "$Id: TransferAgent.py 18891 2009-12-02 17:12:46Z atsareg $"

from DIRAC  import gLogger, gConfig, gMonitor, S_OK, S_ERROR, rootPath
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.ThreadPool import ThreadPool, ThreadedJob
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient
from DIRAC.Core.DISET.RPCClient import RPCClient

import time, os
from types import *

__RCSID__ = "$Id: TransferAgent.py 18891 2009-12-02 17:12:46Z atsareg $"

COMPONENT_NAME = 'RequestManagement/PutAndRegister'

class PutAndRegister:

  def __init__( self, argumentsDict ):
    """ Standard constructor """
    self.name = COMPONENT_NAME
    self.log = gLogger.getSubLogger( self.name )
    self.argumentsDict = argumentsDict
    self.rm = False
    gMonitor.registerActivity( "Put and register", "Put and register operations", "PutAndRegister", "Attempts/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "Put failed", "Failed puts", "PutAndRegister", "Failed/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "Put successful", "Successful puts", "PutAndRegister", "Successful/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "File registration successful", "Successful file registrations", "PutAndRegister", "Successful/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "File registration failed", "Failed file registrations", "TransferAgent", "Failed/min", gMonitor.OP_SUM )

  def setReplicaManager( self, rm ):
    self.rm = rm

  def __getReplicaManager( self ):
    if not self.rm:
      self.rm = ReplicaManager()

  def execute( self ):
    subRequestAttributes = self.argumentsDict.get( 'SubRequestAttributes', {} )
    if not subRequestAttributes:
      return S_ERROR( "SubRequestAttributes not supplied" )
    subRequestFiles = self.argumentsDict.get( 'Files', {} )
    if not subRequestFiles:
      return S_ERROR( "Files not supplied" )
    targetSE = subRequestAttributes.get( 'TargetSE', '' )
    if not targetSE:
      return S_ERROR( "TargetSE not supplied" )
    catalog = subRequestAttributes.get( 'Catalogue', '' )
    status = subRequestAttributes.get( 'Status', 'Waiting' )
    if status != 'Waiting':
      return S_OK()

    for subRequestFile in subRequestFiles:
      lfn = str( subRequestFile.get( 'LFN', '' ) )
      if not lfn:
        self.log.error( "LFN not supplied" )
        continue
      file = subRequestFile.get( 'PFN', '' )
      if not file:
        self.log.error( "PFN not supplied" )
        continue
      addler = subRequestFile.get( 'Addler', '' )
      guid = subRequestFile.get( 'GUID', '' )
      status = subRequestFile.get( 'Status', 'Waiting' )
      if status != 'Waiting':
        self.log.info( "%s in %s status and not Waiting" % ( lfn, status ) )
        continue
      gMonitor.addMark( "Put and register", 1 )
      self.__getReplicaManager()
      res = self.rm.putAndRegister( lfn, file, targetSE, guid = guid, checksum = addler, catalog = catalog )
      if not res['OK']:
        gMonitor.addMark( "Put failed", 1 )
        self.log.error( "Failed to put file", res['Message'] )
        continue
      successful = res['Value']['Successful']
      failed = res['Value']['Failed']
      if ( not successful.has_key( lfn ) ) or ( not successful[lfn].has_key( 'put' ) ):
        gMonitor.addMark( "Put failed", 1 )
        self.log.error( "Failed to put file" )
        continue
      if successful[lfn].has_key( 'put' ):
        gMonitor.addMark( "Put successful", 1 )
        gLogger.info( "Successfully put %s to %s in %s seconds." % ( lfn, targetSE, successful[lfn]['put'] ) )
        subRequestFile['Status'] = 'Done'
      if successful[lfn].has_key( 'register' ):
        gMonitor.addMark( "File registration successful", 1 )
        gLogger.info( "Successfully registered %s to %s in %s seconds." % ( lfn, targetSE, successful[lfn]['register'] ) )
      else:
        gMonitor.addMark( "File registration failed", 1 )
        gLogger.error( "Failed to register %s to %s." % ( lfn, targetSE ) )
        fileDict = failed[lfn]['register']
        registerRequestDict = {'Attributes':{'TargetSE': fileDict['TargetSE'], 'Operation':'registerFile'}, 'Files':[{'LFN': fileDict['LFN'], 'PFN':fileDict['PFN'], 'Size':fileDict['Size'], 'Addler':fileDict['Addler'], 'GUID':fileDict['GUID']}]}
        print registerRequestDict
    return S_OK()
