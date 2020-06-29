""" Runs few integrity checks

The following options can be set for the ValidateOutputDataAgent.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN ValidateOutputDataAgent
  :end-before: ##END
  :dedent: 2
  :caption: ValidateOutputDataAgent options
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import re
import ast

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.DataManagementSystem.Client.DataIntegrityClient import DataIntegrityClient
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.DataManagementSystem.Client.ConsistencyInspector import ConsistencyInspector

AGENT_NAME = 'Transformation/ValidateOutputDataAgent'


class ValidateOutputDataAgent(AgentModule):

  def __init__(self, *args, **kwargs):
    """ c'tor
    """
    AgentModule.__init__(self, *args, **kwargs)

    self.consistencyInspector = ConsistencyInspector()
    self.integrityClient = DataIntegrityClient()
    self.fc = FileCatalog()
    self.transClient = TransformationClient()
    self.fileCatalogClient = FileCatalogClient()

    agentTSTypes = self.am_getOption('TransformationTypes', [])
    if agentTSTypes:
      self.transformationTypes = agentTSTypes
    else:
      self.transformationTypes = Operations().getValue('Transformations/DataProcessing', ['MCSimulation', 'Merge'])

    self.directoryLocations = sorted(self.am_getOption('DirectoryLocations', ['TransformationDB',
                                                                              'MetadataCatalog']))
    self.transfidmeta = self.am_getOption('TransfIDMeta', "TransformationID")
    self.enableFlag = True

  #############################################################################

  def initialize(self):
    """ Sets defaults
    """

    # This sets the Default Proxy to used as that defined under
    # /Operations/Shifter/DataManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption('shifterProxy', 'DataManager')

    gLogger.info("Will treat the following transformation types: %s" % str(self.transformationTypes))
    gLogger.info("Will search for directories in the following locations: %s" % str(self.directoryLocations))
    gLogger.info("Will use %s as metadata tag name for TransformationID" % self.transfidmeta)
    return S_OK()

  #############################################################################

  def execute(self):
    """ The VerifyOutputData execution method
    """
    self.enableFlag = self.am_getOption('EnableFlag', 'True')
    if not self.enableFlag == 'True':
      self.log.info("VerifyOutputData is disabled by configuration option 'EnableFlag'")
      return S_OK('Disabled via CS flag')

    gLogger.info("-" * 40)
    self.updateWaitingIntegrity()
    gLogger.info("-" * 40)

    res = self.transClient.getTransformations({'Status': 'ValidatingOutput', 'Type': self.transformationTypes})
    if not res['OK']:
      gLogger.error("Failed to get ValidatingOutput transformations", res['Message'])
      return res
    transDicts = res['Value']
    if not transDicts:
      gLogger.info("No transformations found in ValidatingOutput status")
      return S_OK()
    gLogger.info("Found %s transformations in ValidatingOutput status" % len(transDicts))
    for transDict in transDicts:
      transID = transDict['TransformationID']
      res = self.checkTransformationIntegrity(int(transID))
      if not res['OK']:
        gLogger.error("Failed to perform full integrity check for transformation %d" % transID)
      else:
        self.finalizeCheck(transID)
        gLogger.info("-" * 40)
    return S_OK()

  def updateWaitingIntegrity(self):
    """ Get 'WaitingIntegrity' transformations, update to 'ValidatedOutput'
    """
    gLogger.info("Looking for transformations in the WaitingIntegrity status to update")
    res = self.transClient.getTransformations({'Status': 'WaitingIntegrity'})
    if not res['OK']:
      gLogger.error("Failed to get WaitingIntegrity transformations", res['Message'])
      return res
    transDicts = res['Value']
    if not transDicts:
      gLogger.info("No transformations found in WaitingIntegrity status")
      return S_OK()
    gLogger.info("Found %s transformations in WaitingIntegrity status" % len(transDicts))
    for transDict in transDicts:
      transID = transDict['TransformationID']
      gLogger.info("-" * 40)
      res = self.integrityClient.getTransformationProblematics(int(transID))
      if not res['OK']:
        gLogger.error("Failed to determine waiting problematics for transformation", res['Message'])
      elif not res['Value']:
        res = self.transClient.setTransformationParameter(transID, 'Status', 'ValidatedOutput')
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

  def getTransformationDirectories(self, transID):
    """ Get the directories for the supplied transformation from the transformation system
    """
    directories = []
    if 'TransformationDB' in self.directoryLocations:
      res = self.transClient.getTransformationParameters(transID, ['OutputDirectories'])
      if not res['OK']:
        gLogger.error("Failed to obtain transformation directories", res['Message'])
        return res
      if not isinstance(res['Value'], list):
        transDirectories = ast.literal_eval(res['Value'])
      else:
        transDirectories = res['Value']
      directories = self._addDirs(transID, transDirectories, directories)

    if 'MetadataCatalog' in self.directoryLocations:
      res = self.fileCatalogClient.findDirectoriesByMetadata({self.transfidmeta: transID})
      if not res['OK']:
        gLogger.error("Failed to obtain metadata catalog directories", res['Message'])
        return res
      transDirectories = res['Value']
      directories = self._addDirs(transID, transDirectories, directories)
    if not directories:
      gLogger.info("No output directories found")
    directories = sorted(directories)
    return S_OK(directories)

  @staticmethod
  def _addDirs(transID, newDirs, existingDirs):
    for nDir in newDirs:
      transStr = str(transID).zfill(8)
      if re.search(transStr, nDir):
        if nDir not in existingDirs:
          existingDirs.append(nDir)
    return existingDirs

  #############################################################################
  def checkTransformationIntegrity(self, transID):
    """ This method contains the real work
    """
    gLogger.info("-" * 40)
    gLogger.info("Checking the integrity of transformation %s" % transID)
    gLogger.info("-" * 40)

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
    res = self.fc.exists(directories)
    if not res['OK']:
      gLogger.error('Failed to check directory existence', res['Message'])
      return res
    for directory, error in res['Value']['Failed']:
      gLogger.error('Failed to determine existance of directory', '%s %s' % (directory, error))
    if res['Value']['Failed']:
      return S_ERROR("Failed to determine the existance of directories")
    directoryExists = res['Value']['Successful']
    for directory in sorted(directoryExists):
      if not directoryExists[directory]:
        continue
      iRes = self.consistencyInspector.catalogDirectoryToSE(directory)
      if not iRes['OK']:
        gLogger.error(iRes['Message'])
        return iRes

    gLogger.info("-" * 40)
    gLogger.info("Completed integrity check for transformation %s" % transID)
    return S_OK()

  def finalizeCheck(self, transID):
    """ Move to 'WaitingIntegrity' or 'ValidatedOutput'
    """
    res = self.integrityClient.getTransformationProblematics(int(transID))

    if not res['OK']:
      gLogger.error("Failed to determine whether there were associated problematic files", res['Message'])
      newStatus = ''
    elif res['Value']:
      gLogger.info("%d problematic files for transformation %s were found" % (len(res['Value']), transID))
      newStatus = "WaitingIntegrity"
    else:
      gLogger.info("No problematics were found for transformation %s" % transID)
      newStatus = "ValidatedOutput"
    if newStatus:
      res = self.transClient.setTransformationParameter(transID, 'Status', newStatus)
      if not res['OK']:
        gLogger.error("Failed to update status of transformation %s to %s" % (transID, newStatus))
      else:
        gLogger.info("Updated status of transformation %s to %s" % (transID, newStatus))
    gLogger.info("-" * 40)
    return S_OK()
