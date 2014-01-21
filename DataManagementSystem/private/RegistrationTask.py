########################################################################
# $HeadURL $
# File: RegistrationTask.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/10/20 12:06:50
########################################################################

""" :mod: RegistrationTask 
    =======================
 
    .. module: RegistrationTask
    :synopsis: 
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com


    :deprecated:
"""

__RCSID__ = "$Id $"

##
# @file RegistrationTask.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/10/20 12:06:59
# @brief Definition of RegistrationTask class.

## imports 
from DIRAC import S_OK, S_ERROR
from DIRAC.DataManagementSystem.private.RequestTask import RequestTask

########################################################################
class RegistrationTask( RequestTask ):
  """
  .. class:: RegistrationTask
  
  """

  def __init__( self, *args, **kwargs ):
    """c'tor

    :param self: self reference
    """
    RequestTask.__init__( self, *args, **kwargs )
    self.setRequestType( "register" )
    self.addOperationAction( "registerFile", self.registerFile )

  def registerFile( self, index, requestObj, subRequestAttrs, subRequestFiles ):
    """ registerFile operation handler

    :param self: self reference
    :param int index: execution order index
    :param RequestContainer requestObj: a request object
    :param subRequestAttrs: SubRequest attributes
    :param subRequestFiles: subRequest files
    """
    self.info( "registerFile: processing subrequest %s" % index )
    if requestObj.isSubRequestEmpty( index, "register" )["Value"]:
      self.info("registerFile: subrequest %s is empty, setting its status to 'Done'" % index )
      requestObj.setSubRequestStatus( index, "register", "Done" )
      return S_OK( requestObj )

    ## list of targetSE
    targetSEs = list( set( [ targetSE.strip() for targetSE in  subRequestAttrs["TargetSE"].split(",") 
                             if targetSE.strip() ] ) )
    if not targetSEs:
      self.warn( "registerFile: no TargetSE specified!")

    ## dict for failed LFNs
    failed = {}
    failedFiles = 0

    ## get catalogue
    catalogue = subRequestAttrs["Catalogue"] if subRequestAttrs["Catalogue"] else ""

    for subRequestFile in subRequestFiles:
      lfn = subRequestFile.get( "LFN", "" ) 
      self.info("registerFile: processing file %s" % lfn )
      if subRequestFile["Status"] != "Waiting":
        self.info("registerFile: skipping file %s, status is %s" % ( lfn, subRequestFile["Status"] ) )
        continue

      failed.setdefault( lfn, {} )
      pfn = subRequestFile.get( "PFN", "" ) 
      size = subRequestFile.get( "Size", 0 ) 
      guid = subRequestFile.get( "GUID", "" ) 
      addler = subRequestFile.get( "Addler", "" ) 

      for targetSE in targetSEs:
        
        fileTuple = ( lfn, pfn, size, targetSE, guid, addler )
        if "" in fileTuple:
          self.warn( "registerFile: missing arg in (LFN, PFN, Size, TargetSE, GUID, Addler) = %s" % str(fileTuple) )

        res = self.dm.registerFile( fileTuple, catalogue )

        if not res["OK"] or lfn in res["Value"]["Failed"]:
          self.dataLoggingClient().addFileRecord( lfn, "RegisterFail", targetSE, "", "RegistrationAgent" )
          reason = res["Message"] if not res["OK"] else res["Value"]["Failed"][lfn]  # "registration in DataManager failed"
          errorStr = "failed to register LFN %s: %s" % ( lfn, reason )
          failed[lfn][targetSE] = reason
          self.warn( "registerFile: %s" % errorStr )
          failedFiles += 1
        else:
          self.dataLoggingClient().addFileRecord( lfn, "Register", targetSE, "", "RegistrationAgent" )
          self.info( "registerFile: file %s has been registered at %s" % ( lfn, targetSE ) )
     
      if not failed[lfn]:
        requestObj.setSubRequestFileAttributeValue( index, "register", lfn, "Status", "Done")
        self.info( "registerFile: file %s has been registered at all targetSEs" % lfn )        
      else:
        fileError = ";".join( [ "%s:%s" % (targetSE, reason) for targetSE, reason in failed[lfn].items() ] )
        requestObj.setSubRequestFileAttributeValue( index, "register", lfn, "Error", fileError.replace("'","")[:255] )

    ##################################################################
    ## all files were registered or no files at all in this subrequest
    if requestObj.isSubRequestDone( index, "register" )["Value"]:
      self.info("registerFile: all files processed, setting subrequest status to 'Done'")
      requestObj.setSubRequestStatus( index, "register", "Done" )
    elif failedFiles:
      self.info("registerFile: all files processed, %s files failed to register" % failedFiles )
      errors = []
      for lfn in failed:
        for targetSE, reason in failed[lfn].items():
          error = "%s:%s:%s" % ( lfn, targetSE, str(reason).replace("'", "") )
          self.warn( "registerFile: %s@%s - %s" % ( lfn, targetSE, reason ) )
          errors.append( error )
      requestObj.setSubRequestAttributeValue( index, "register", "Error", ";".join( errors )[:255] )
    ## return requestObj
    return S_OK( requestObj )
          
    
