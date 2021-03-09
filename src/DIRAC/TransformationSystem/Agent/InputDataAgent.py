'''
Update the transformation files of active transformations,
given an InputDataQuery fetched from the Transformation Service.

Possibility to speedup the query time by only fetching files that were added since the last iteration.
Use the CS option RefreshOnly (False by default) and set the DateKey (empty by default) to the meta data
key set in the DIRAC FileCatalog.

The following options can be set for the InputDataAgent.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN InputDataAgent
  :end-before: ##END
  :dedent: 2
  :caption: InputDataAgent options
'''
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time
import datetime

from errno import ENOENT

from DIRAC import S_OK
from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.DErrno import cmpError
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

__RCSID__ = "$Id$"

AGENT_NAME = 'Transformation/InputDataAgent'


class InputDataAgent(AgentModule):

  def __init__(self, *args, **kwargs):
    ''' c'tor
    '''
    AgentModule.__init__(self, *args, **kwargs)

    self.fileLog = {}
    self.timeLog = {}
    self.fullTimeLog = {}

    self.pollingTime = self.am_getOption('PollingTime', 120)
    self.fullUpdatePeriod = self.am_getOption('FullUpdatePeriod', 86400)
    self.refreshonly = self.am_getOption('RefreshOnly', False)
    self.dateKey = self.am_getOption('DateKey', None)

    self.transClient = TransformationClient()
    self.metadataClient = FileCatalogClient()
    self.transformationTypes = None

  #############################################################################
  def initialize(self):
    ''' Make the necessary initializations
    '''
    gMonitor.registerActivity("Iteration", "Agent Loops", AGENT_NAME, "Loops/min", gMonitor.OP_SUM)
    agentTSTypes = self.am_getOption('TransformationTypes', [])
    if agentTSTypes:
      self.transformationTypes = sorted(agentTSTypes)
    else:
      dataProc = Operations().getValue('Transformations/DataProcessing', ['MCSimulation', 'Merge'])
      dataManip = Operations().getValue('Transformations/DataManipulation', ['Replication', 'Removal'])
      self.transformationTypes = sorted(dataProc + dataManip)
    extendables = Operations().getValue('Transformations/ExtendableTransfTypes', [])
    if extendables:
      for extendable in extendables:
        if extendable in self.transformationTypes:
          self.transformationTypes.remove(extendable)
          # This is because the Extendables do not use this Agent (have no Input data query)

    return S_OK()

  ##############################################################################
  def execute(self):
    ''' Main execution method
    '''

    gMonitor.addMark('Iteration', 1)
    # Get all the transformations
    result = self.transClient.getTransformations({'Status': 'Active',
                                                  'Type': self.transformationTypes})
    if not result['OK']:
      self.log.error("InputDataAgent.execute: Failed to get transformations.", result['Message'])
      return S_OK()

    # Process each transformation
    for transDict in result['Value']:
      transID = int(transDict['TransformationID'])
      # res = self.transClient.getTransformationInputDataQuery( transID )
      res = self.transClient.getTransformationMetaQuery(transID, 'Input')
      if not res['OK']:
        if cmpError(res, ENOENT):
          self.log.info("InputDataAgent.execute: No input data query found for transformation", transID)
        else:
          self.log.error("InputDataAgent.execute: Failed to get input data query",
                         "for %d: %s" % (transID, res['Message']))
        continue
      inputDataQuery = res['Value']

      if self.refreshonly:
        # Determine the correct time stamp to use for this transformation
        if transID in self.timeLog:
          if transID in self.fullTimeLog:
            # If it is more than a day since the last reduced query, make a full query just in case
            if (datetime.datetime.utcnow() -
                    self.fullTimeLog[transID]) < datetime.timedelta(seconds=self.fullUpdatePeriod):
              timeStamp = self.timeLog[transID]
              if self.dateKey:
                inputDataQuery[self.dateKey] = (timeStamp -
                                                datetime.timedelta(seconds=10)).strftime('%Y-%m-%d %H:%M:%S')
              else:
                self.log.error("DateKey was not set in the CS, cannot use the RefreshOnly")
            else:
              self.fullTimeLog[transID] = datetime.datetime.utcnow()
        self.timeLog[transID] = datetime.datetime.utcnow()
        if transID not in self.fullTimeLog:
          self.fullTimeLog[transID] = datetime.datetime.utcnow()

      # Perform the query to the metadata catalog
      self.log.verbose("Using input data query for transformation", "%d: %s" % (transID, str(inputDataQuery)))
      start = time.time()
      result = self.metadataClient.findFilesByMetadata(inputDataQuery)
      rtime = time.time() - start
      self.log.verbose("Metadata catalog query time", ": %.2f seconds." % (rtime))
      if not result['OK']:
        self.log.error("InputDataAgent.execute: Failed to get response from the metadata catalog", result['Message'])
        continue
      lfnList = result['Value']

      # Check if the number of files has changed since the last cycle
      nlfns = len(lfnList)
      self.log.info("files returned for transformation from the metadata catalog: ",
                    "%d -> %d" % (int(transID), nlfns))
      if nlfns == self.fileLog.get(transID):
        self.log.verbose('No new files in metadata catalog since last check')
      self.fileLog[transID] = nlfns

      # Add any new files to the transformation
      addedLfns = []
      if lfnList:
        self.log.verbose('Processing lfns for transformation:', "%d -> %d" % (transID, len(lfnList)))
        # Add the files to the transformation
        self.log.verbose('Adding lfns for transformation:', "%d -> %d" % (transID, len(lfnList)))
        result = self.transClient.addFilesToTransformation(transID, sorted(lfnList))
        if not result['OK']:
          self.log.warn("InputDataAgent.execute: failed to add lfns to transformation", result['Message'])
          self.fileLog[transID] = 0
        else:
          if result['Value']['Failed']:
            for lfn, error in res['Value']['Failed'].items():
              self.log.warn("InputDataAgent.execute: Failed to add to transformation:", "%s: %s" % (lfn, error))
          if result['Value']['Successful']:
            for lfn, status in result['Value']['Successful'].items():
              if status == 'Added':
                addedLfns.append(lfn)
            self.log.info("InputDataAgent.execute: Added files to transformation", "(%d)" % len(addedLfns))

    return S_OK()
