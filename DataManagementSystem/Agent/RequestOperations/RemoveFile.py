########################################################################
# $HeadURL $
# File: RemoveFile.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/25 07:44:19
########################################################################

""" :mod: RemoveFile
    ================

    .. module: RemoveFile
    :synopsis: removeFile operation handler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    removeFile operation handler
"""

__RCSID__ = "$Id $"

# #
# @file RemoveFile.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/25 07:44:27
# @brief Definition of RemoveFile class.

# # imports
import os
import re
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gMonitor
from DIRAC.RequestManagementSystem.private.OperationHandlerBase import OperationHandlerBase

########################################################################
class RemoveFile( OperationHandlerBase ):
  """
  .. class:: RemoveFile

  remove file operation handler
  """

  def __init__( self, operation = None, csPath = None ):
    """c'tor

    :param self: self reference
    :param Operation operation: Operation to execute
    :param str csPath: CS path for this handler
    """
    # # call base class ctor
    OperationHandlerBase.__init__( self, operation, csPath )
    # # gMOnitor stuff goes here
    gMonitor.registerActivity( "RemoveFileAtt", "File removals attempted",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RemoveFileOK", "Successful file removals",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RemoveFileFail", "Failed file removals",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    # # re pattern for not existing files
    self.reNotExisting = re.compile( "(no|not) such file or directory", re.IGNORECASE )

  def __call__( self ):
    """ action for 'removeFile' operation  """
    # # get waiting files
    waitingFiles = self.getWaitingFilesList()
    # # prepare waiting file dict
    toRemoveDict = dict( [ ( opFile.LFN, opFile ) for opFile in waitingFiles ] )
    gMonitor.addMark( "RemoveFileAtt", len( toRemoveDict ) )

    # # 1st step - bulk removal
    self.log.debug( "bulk removal of %s files" % len( toRemoveDict ) )
    bulkRemoval = self.bulkRemoval( toRemoveDict )
    if not bulkRemoval["OK"]:
      self.log.error( "bulk removal failed: %s" % bulkRemoval["Message"] )
    else:
      gMonitor.addMark( "RemoveFileOK", len( toRemoveDict ) - len( bulkRemoval["Value"] ) )
      toRemoveDict = bulkRemoval["Value"]

    # # 2nd step - single file removal
    for lfn, opFile in toRemoveDict.items():
      self.log.info( "removing single file %s" % lfn )
      singleRemoval = self.singleRemoval( opFile )
      if not singleRemoval["OK"]:
        self.log.error( 'Error removing single file', singleRemoval["Message"] )
        gMonitor.addMark( "RemoveFileFail", 1 )
      else:
        self.log.info( "file %s has been removed" % lfn )
        gMonitor.addMark( "RemoveFileOK", 1 )

    # # set
    failedFiles = [ ( lfn, opFile ) for ( lfn, opFile ) in toRemoveDict.items()
                    if opFile.Status in ( "Failed", "Waiting" ) ]
    if failedFiles:
      self.operation.Error = "failed to remove %d files" % len( failedFiles )

    return S_OK()

  def bulkRemoval( self, toRemoveDict ):
    """ bulk removal using request owner DN

    :param dict toRemoveDict: { lfn: opFile, ... }
    :return: S_ERROR or S_OK( { lfn: opFile, ... } ) -- dict with files still waiting to be removed
    """
    bulkRemoval = self.dm.removeFile( toRemoveDict.keys(), force = True )
    if not bulkRemoval["OK"]:
      self.log.error( "unable to remove files: %s" % bulkRemoval["Message"] )
      self.operation.Error = bulkRemoval["Message"]
      return bulkRemoval
    bulkRemoval = bulkRemoval["Value"]
    # # filter results
    for lfn, opFile in toRemoveDict.items():
      if lfn in bulkRemoval["Successful"]:
        opFile.Status = "Done"
      elif lfn in bulkRemoval["Failed"]:

        error = bulkRemoval["Failed"][lfn]
        if type( error ) == dict:
          error = ";".join( [ "%s-%s" % ( k, v ) for k, v in error.items() ] )
        opFile.Error = error
        if self.reNotExisting.search( opFile.Error ):
          opFile.Status = "Done"

    # # return files still waiting
    toRemoveDict = dict( [ ( opFile.LFN, opFile ) for opFile in self.operation if opFile.Status == "Waiting" ] )
    return S_OK( toRemoveDict )

  def singleRemoval( self, opFile ):
    """ remove single file

    :param opFile: File instance
    """
    # # try to remove with owner proxy
    proxyFile = None
    if "Write access not permitted for this credential" in opFile.Error:
      if "DataManager" in self.shifter:
        # #  you're a data manager - get proxy for LFN and retry
        saveProxy = os.environ["X509_USER_PROXY"]
        try:
          fileProxy = self.getProxyForLFN( opFile.LFN )
          if not fileProxy["OK"]:
            opFile.Error = "Error getting owner's proxy : %s" % fileProxy['Message']
          else:
            proxyFile = fileProxy["Value"]
            self.log.info( "Trying to remove file with owner's proxy (file %s)" % proxyFile )

            removeFile = self.dm.removeFile( opFile.LFN, force = True )
            self.log.always( str( removeFile ) )

            if not removeFile["OK"]:
              opFile.Error = str( removeFile["Message"] )
              if self.reNotExisting.search( str( removeFile["Message"] ).lower() ):
                opFile.Status = "Done"
            else:
              removeFile = removeFile["Value"]
              if opFile.LFN in removeFile["Failed"]:
                error = removeFile["Failed"][opFile.LFN]
                if type( error ) == dict:
                  error = ";".join( [ "%s-%s" % ( k, v ) for k, v in error.items() ] )
                if self.reNotExisting.search( error ):
                  # This should never happen due to the "force" flag
                  opFile.Status = "Done"
                else:
                  opFile.Error = error
              else:
                opFile.Status = "Done"
        finally:
          if proxyFile:
            os.unlink( proxyFile )
          # # put back request owner proxy to env
          os.environ["X509_USER_PROXY"] = saveProxy
    # # file removed? update its status to 'Done'
    if opFile.Status == "Done":
      return S_OK()
    return S_ERROR( opFile.Error )
