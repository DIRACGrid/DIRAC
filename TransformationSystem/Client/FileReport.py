""" FileReport class encapsulates methods to report file status to the transformation DB """

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities                                     import DEncode
from DIRAC.TransformationSystem.Client.TransformationClient   import TransformationClient
from DIRAC.RequestManagementSystem.Client.Operation           import Operation

import copy

class FileReport( object ):
  """ A stateful object for reporting to TransformationDB
  """

  def __init__( self, server = 'Transformation/TransformationManager' ):
    self.transClient = TransformationClient()
    self.transClient.setServer( server )
    self.statusDict = {}
    self.transformation = None

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
      return S_OK()

    # create intermediate status dictionary
    sDict = {}
    for lfn, status in self.statusDict.items():
      if not sDict.has_key( status ):
        sDict[status] = []
      sDict[status].append( lfn )

    summaryDict = {}
    failedResults = []
    for status, lfns in sDict.items():
      res = self.transClient.setFileStatusForTransformation( self.transformation, status, lfns )
      if not res['OK']:
        failedResults.append( res )
        continue
      for lfn, error in res['Value']['Failed'].items():
        gLogger.error( "Failed to update file status", "%s %s" % ( lfn, error ) )
      if res['Value']['Successful']:
        summaryDict[status] = len( res['Value']['Successful'] )
        for lfn in res['Value']['Successful']:
          self.statusDict.pop( lfn )

    if not self.statusDict:
      return S_OK( summaryDict )
    result = S_ERROR( "Failed to update all file statuses" )
    result['FailedResults'] = failedResults
    return result

  def generateForwardDISET( self ):
    """ Commit the accumulated records and generate request eventually """
    result = self.commit()
    forwardDISETOp = None
    if not result['OK']:
      # Generate Request
      if "FailedResults" in result:
        for res in result['FailedResults']:
          if 'rpcStub' in res:
            forwardDISETOp = Operation()
            forwardDISETOp.Type = "ForwardDISET"
            forwardDISETOp.Arguments = DEncode.encode( res['rpcStub'] )

    return S_OK( forwardDISETOp )
