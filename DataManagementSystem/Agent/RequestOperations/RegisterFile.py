########################################################################
# $HeadURL $
# File: RegisterOperation.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/19 13:55:14
########################################################################
""" :mod: RegisterFile
    ==================

    .. module: RegisterFile
    :synopsis: register operation handler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    RegisterFile operation handler
"""

__RCSID__ = "$Id $"

# #
# @file RegisterOperation.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/19 13:55:24
# @brief Definition of RegisterOperation class.

# # imports
from DIRAC import gMonitor, S_OK, S_ERROR
from DIRAC.RequestManagementSystem.private.OperationHandlerBase import OperationHandlerBase
from DIRAC.DataManagementSystem.Client.DataManager import DataManager

########################################################################
class RegisterFile( OperationHandlerBase ):
  """
  .. class:: RegisterOperation

  RegisterFile operation handler
  """

  def __init__( self, operation = None, csPath = None ):
    """c'tor

    :param self: self reference
    :param Operation operation: Operation instance
    :param str csPath: CS path for this handler
    """
    OperationHandlerBase.__init__( self, operation, csPath )
    # # RegisterFile specific monitor info
    gMonitor.registerActivity( "RegisterAtt", "Attempted file registrations",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RegisterOK", "Successful file registrations",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RegisterFail", "Failed file registrations",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )

  def __call__( self ):
    """ call me maybe """
    # # counter for failed files
    failedFiles = 0
    # # catalog to use
    catalog = self.operation.Catalog
    dm = DataManager( catalogs = catalog )
    # # get waiting files
    waitingFiles = self.getWaitingFilesList()
    # # loop over files
    for opFile in waitingFiles:

      gMonitor.addMark( "RegisterAtt", 1 )

      # # get LFN
      lfn = opFile.LFN
      # # and others
      fileTuple = ( lfn , opFile.PFN, opFile.Size, self.operation.targetSEList[0], opFile.GUID, opFile.Checksum )
      # # call DataManager
      registerFile = dm.registerFile( fileTuple )
      # # check results
      if not registerFile["OK"] or lfn in registerFile["Value"]["Failed"]:

        gMonitor.addMark( "RegisterFail", 1 )
        self.dataLoggingClient().addFileRecord( lfn, "RegisterFail", catalog, "", "RegisterFile" )

        reason = registerFile.get( "Message", registerFile.get( "Value", {} ).get( "Failed", {} ).get( lfn, 'Unknown' ) )
        errorStr = "failed to register LFN %s: %s" % ( lfn, reason )
        opFile.Error = errorStr
        self.log.warn( errorStr )
        failedFiles += 1

      else:

        gMonitor.addMark( "RegisterOK", 1 )
        self.dataLoggingClient().addFileRecord( lfn, "Register", catalog, "", "RegisterFile" )

        self.log.info( "file %s has been registered at %s" % ( lfn, catalog ) )
        opFile.Status = "Done"

    # # final check
    if failedFiles:
      self.log.info( "all files processed, %s files failed to register" % failedFiles )
      self.operation.Error = "some files failed to register"
      return S_ERROR( self.operation.Error )

    return S_OK()



