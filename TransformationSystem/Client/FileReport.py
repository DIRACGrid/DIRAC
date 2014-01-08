""" FileReport class encapsulates methods to report file status to the transformation DB """

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities                                     import DEncode
from DIRAC.TransformationSystem.Client.TransformationClient   import TransformationClient
from DIRAC.RequestManagementSystem.Client.Operation           import Operation

import copy

__RCSID__ = "$Id$"

class FileReport( object ):
  """ A stateful object for reporting to TransformationDB
  """

  def __init__( self, server = 'Transformation/TransformationManager' ):
    self.transClient = TransformationClient()
    self.transClient.setServer( server )
    self.statusDict = {}
    self.transformation = None
    self.force = False

  def setFileStatus( self, transformation, lfn, status, sendFlag = False ):
    """ Set file status in the context of the given transformation """
    if not self.transformation:
      self.transformation = transformation
    self.statusDict[lfn] = status
    if sendFlag:
      return self.commit()
    return S_OK()

  def setCommonStatus( self, status ):
    """ Set common status for all files in the internal cache """
    for lfn in self.statusDict.keys():
      self.statusDict[lfn] = status
    return S_OK()

  def getFiles( self ):
    """ Get the statuses of the files already accumulated in the FileReport object """
    return copy.deepcopy( self.statusDict )

  def commit( self ):
    """ Commit pending file status update records """
    if not self.statusDict:
      return S_OK({})

    return self.transClient.setFileStatusForTransformation( self.transformation, self.statusDict, force = self.force )

  def generateForwardDISET( self ):
    """ Commit the accumulated records and generate request eventually """
    result = self.commit()
    forwardDISETOp = None
    if not result['OK']:
      # Generate Request
      if result.has_key( 'rpcStub' ):
        forwardDISETOp = Operation()
        forwardDISETOp.Type = "ForwardDISET"
        forwardDISETOp.Arguments = DEncode.encode( result['rpcStub'] )

      else:
        return S_ERROR( 'Could not create ForwardDISET operation' )

    return S_OK( forwardDISETOp )
