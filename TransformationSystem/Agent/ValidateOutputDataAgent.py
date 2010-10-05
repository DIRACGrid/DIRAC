########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/TransformationSystem/Agent/ValidateOutputDataAgent.py $
########################################################################
__RCSID__ = "$Id: ValidateOutputDataAgent.py 28415 2010-09-15 17:47:54Z acsmith $"
__VERSION__ = "$Revision: 1.5 $"

from DIRAC                                                     import S_OK, S_ERROR, gConfig, gMonitor, gLogger, rootPath
from DIRAC.Core.Base.AgentModule                               import AgentModule
from DIRAC.Core.Utilities.List                                 import sortList
from DIRAC.Core.Utilities.Shifter                              import setupShifterProxyInEnv
from DIRAC.DataManagementSystem.Client.DataIntegrityClient     import DataIntegrityClient
from DIRAC.DataManagementSystem.Client.ReplicaManager          import ReplicaManager
from DIRAC.TransformationSystem.Client.TransformationDBClient  import TransformationDBClient
import re, os

AGENT_NAME = 'Transformation/ValidateOutputDataAgent'

class ValidateOutputDataAgent(AgentModule):

  #############################################################################
  def initialize( self ):
    """Sets defaults
    """
    self.integrityClient = DataIntegrityClient()
    self.replicaManager = ReplicaManager()
    self.transClient = TransformationDBClient()
    self.am_setModuleParam( "shifterProxy", "DataManager" )
    self.transformationTypes = self.am_getOption('TransformationTypes', ['MCSimulation', 'DataReconstruction', 'DataStripping', 'MCStripping', 'Merge'])
    self.activeStorages = self.am_getOption('ActiveSEs',[])
    return S_OK()

  #############################################################################
  def execute( self ):
    """ The VerifyOutputData execution method """
    self.enableFlag = self.am_getOption( 'EnableFlag', 'True' )
    if not self.enableFlag == 'True':
      self.log.info( 'VerifyOutputData is disabled by configuration option %s/EnableFlag' % ( self.section ) )
      return S_OK( 'Disabled via CS flag' )

    gLogger.info( "-" * 40 )
    self.updateWaitingIntegrity()
    gLogger.info( "-" * 40 )

    res = self.transClient.getTransformations({'Status':'ValidatingOutput','Type':self.transformationTypes})
    if not res['OK']:
      gLogger.error( "Failed to get ValidatingOutput transformations", res['Message'] )
      return res
    transDicts = res['Value']
    if not transDicts:
      gLogger.info( "No transformations found in ValidatingOutput status" )
      return S_OK()
    gLogger.info( "Found %s transformations in ValidatingOutput status" % len( prods ) )
    for transDict in transDicts:
      transID = transDict['TransformationID']
      res = self.checkTransformationIntegrity(int(transID))
      if not res['OK']:
        gLogger.error( "Failed to perform full integrity check for transformation %d" % transID )
      else:
        self.finalizeCheck(transID)
        gLogger.info( "-" * 40 )
    return S_OK()

  def updateWaitingIntegrity(self):
    gLogger.info( "Looking for transformations in the WaitingIntegrity status to update" )
    res = self.transClient.getTransformations({'Status':'WaitingIntegrity'})
    if not res['OK']:
      gLogger.error( "Failed to get WaitingIntegrity transformations", res['Message'] )
      return res
    transDicts = res['Value']
    if not transDicts:
      gLogger.info( "No transformations found in WaitingIntegrity status" )
      return S_OK()
    gLogger.info( "Found %s transformations in WaitingIntegrity status" % len( prods ) )
    for transDict in transDicts:
      transID = transDict['TransformationID']
      gLogger.info( "-" * 40 )
      res = self.integrityClient.getTransformationProblematics(int(transID))
      if not res['OK']:
        gLogger.error("Failed to determine waiting problematics for transformation", res['Message'])
      elif not res['Value']:
        res = self.transClient.setTransformationParameter(transID,'Status','ValidatedOutput')
        if not res['OK']:
          gLogger.error("Failed to update status of transformation %s to ValidatedOutput" % (transID))
        else:
          gLogger.info("Updated status of transformation %s to ValidatedOutput" % (transID))
      else:
        gLogger.info("%d problematic files for transformation %s were found" % (len(res['Value']), transID))
    return

  #############################################################################
  #
  # Get the transformation directories for checking
  #

  def getTransformationDirectories(self,transID):
    """ Get the directories for the supplied transformation from the transformation system """
    directories = []
    res = self.transClient.getParameters( transID, pname = 'OutputDirectories' )
    if not res['OK']:
      gLogger.error("Failed to obtain transformation directories",res['Message'])
      return res
    directories = res['Value'].splitlines()
    from DIRAC.Core.DISET.RPCClient import RPCClient
    client = RPCClient("DataManagement/StorageUsage")
    res = client.getStorageDirectories('','',transID,[])
    if not res['OK']:
      gLogger.error("Failed to obtain storage usage directories",res['Message'])
      return res
    for dir in res['Value']:
      if not dir in directories:
        directories.append(dir)
    for dir in directories:
      transStr = str(transID).zfill(8)
      if not re.search(transStr,dir):
        directories.remove(dir)
    if not directories:
      gLogger.info("No output directories found")
    return S_OK(directories)

  #############################################################################
  def checkTransformationIntegrity(self, transID):
    """ This method contains the real work """
    gLogger.info( "-" * 40 )
    gLogger.info( "Checking the integrity of transformation %s" % transID )
    gLogger.info( "-" * 40 )

    res = self.getTransformationDirectories(transID)
    if not res['OK']:
      return res
    directories = res['Value']
    if not directories:
      return S_OK()
    
    ######################################################
    #
    # This check performs Catalog->SE for possible output directories
    #
    res = self.replicaManager.getCatalogExists(directories)
    if not res['OK']:
      gLogger.error( res['Message'] )
      return res
    for directory, error in res['Value']['Failed']:
      gLogger.error( 'Failed to determine existance of directory', '%s %s' % ( directory, error ) )
    if res['Value']['Failed']:
      return S_ERROR( "Failed to determine the existance of directories" )
    directoryExists = res['Value']['Successful']
    for directory in sortList( directoryExists.keys() ):
      if not directoryExists[directory]:
        continue
      iRes = self.integrityClient.catalogDirectoryToSE( directory )
      if not iRes['OK']:
        gLogger.error( iRes['Message'] )
        return iRes
      #catalogDirMetadata = iRes['Value']['CatalogMetadata']
      #catalogDirReplicas = iRes['Value']['CatalogReplicas']

    ###################################################### 
    #
    # This check performs SE->Catalog for possible output directories
    #
    for storageElementName in sortList(self.activeStorages):
      res = self.integrityClient.storageDirectoryToCatalog(directories,storageElementName)
      if not res['OK']:
        gLogger.error( res['Message'] )
        return res
      #catalogMetadata = res['Value']['CatalogMetadata']
      #storageMetadata = res['Value']['StorageMetadata']

    gLogger.info( "-" * 40 )
    gLogger.info( "Completed integrity check for transformation %s" % transID)
    return S_OK()

  def finalizeCheck(self,transID):
    res = self.integrityClient.getTransformationProblematics(int(transID))
    if not res['OK']:
      gLogger.error("Failed to determine whether there were associated problematic files",res['Message'])
      newStatus = ''
    elif res['Value']:
      gLogger.info( "%d problematic files for transformation %s were found" % (len(res['Value'] ),transID))
      newStatus = "WaitingIntegrity"
    else:
      gLogger.info("No problematics were found for transformation %s" % transID)
      newStatus = "ValidatedOutput"
    if newStatus:
      res = self.transClient.setTransformationParameter(transID,'Status',newStatus)
      if not res['OK']:
        gLogger.error("Failed to update status of transformation %s to %s" % (transID,newStatus))
      else:
        gLogger.info( "Updated status of transformation %s to %s" % (transID,newStatus))
    gLogger.info( "-" * 40 )
    return S_OK()
