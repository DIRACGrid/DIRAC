''' FileReport class encapsulates methods to report file status to the transformation DB '''

__RCSID__ = "$Id: FileReport.py 18161 2009-11-11 12:07:09Z acasajus $"

from DIRAC                                                      import S_OK, S_ERROR, gLogger
from DIRAC.TransformationSystem.Client.TransformationClient   import TransformationClient
from DIRAC.RequestManagementSystem.Client.RequestContainer      import RequestContainer

import copy

class FileReport:
  ''' A stateful object for reporting to TransformationDB
  '''

  def __init__( self, server = 'Transformation/TransformationManager' ):
    self.client = TransformationClient()
    self.client.setServer( server )
    self.statusDict = {}
    self.transformation = None

  def setFileStatus( self, transformation, lfn, status, sendFlag = False ):
    ''' Set file status in the context of the given transformation '''
    if not self.transformation:
      self.transformation = transformation
    self.statusDict[lfn] = status
    if sendFlag:
      return self.commit()
    return S_OK()

  def setCommonStatus( self, status ):
    ''' Set common status for all files in the internal cache '''
    for lfn in self.statusDict.keys():
      self.statusDict[lfn] = status
    return S_OK()

  def getFiles( self ):
    ''' Get the statuses of the files already accumulated in the FileReport object '''
    return copy.deepcopy( self.statusDict )

  def commit( self ):
    ''' Commit pending file status update records '''
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
      res = self.client.setFileStatusForTransformation( self.transformation, status, lfns )
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

  def generateRequest( self ):
    ''' Commit the accumulated records and generate request eventually '''
    result = self.commit()
    request = None
    if not result['OK']:
      # Generate Request
      request = RequestContainer()
      if result.has_key( 'FailedResults' ):
        for res in result['FailedResults']:
          if res.has_key( 'rpcStub' ):
            request.setDISETRequest( res['rpcStub'] )
    return S_OK( request )
