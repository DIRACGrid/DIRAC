########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/ProductionManagementSystem/Agent/TransformationAgent.py $
########################################################################

"""  The Transformation Agent prepares production jobs for processing data
     according to transformation definitions in the Production database.
"""

__RCSID__ = "$Id: TransformationAgent.py 18182 2009-11-11 14:45:10Z paterson $"

from DIRAC.Core.Base.Agent      import Agent
from DIRAC                      import S_OK, S_ERROR, gConfig, gLogger, gMonitor
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.Core.Utilities.SiteSEMapping       import getSitesForSE
from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv

from LHCbDIRAC.LHCbSystem.Utilities.AncestorFiles import getAncestorFiles

import os, time, random,re

AGENT_NAME = 'ProductionManagement/TransformationAgent'

class TransformationAgent(Agent):

  #############################################################################
  def __init__(self):
    """ Standard constructor for Agent
    """
    Agent.__init__(self,AGENT_NAME)

  #############################################################################
  def initialize(self):
    """ Make the necessary initilizations
    """
    result = Agent.initialize(self)
    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',120)
    self.checkLFC = gConfig.getValue(self.section+'/CheckLFCFlag','yes')
    self.rm = ReplicaManager()
    gMonitor.registerActivity("Iteration","Agent Loops",self.name,"Loops/min",gMonitor.OP_SUM)
    self.CERNShare = 0.144
    res = setupShifterProxyInEnv("ProductionManager")
    if not res['OK']:
      self.log.error( "Can't get shifter's proxy: %s" % result[ 'Message' ] )
      return res
    return result

  ##############################################################################
  def execute(self):
    """Main execution method
    """

    gMonitor.addMark('Iteration',1)
    server = RPCClient('ProductionManagement/ProductionManager')
    transID = gConfig.getValue(self.section+'/Transformation','')
    if transID:
      self.singleTransformation = long(transID)
      gLogger.info("Initializing Replication Agent for transformation %s." % transID)
    else:
      self.singleTransformation = False
      gLogger.info("TransformationAgent.execute: Initializing general purpose agent.")

    result = server.getAllProductions()
    activeTransforms = []
    if not result['OK']:
      gLogger.error("TransformationAgent.execute: Failed to get transformations.", result['Message'])
      return S_OK()

    for transDict in result['Value']:
      transID = long(transDict['TransID'])
      transStatus = transDict['Status']

      processTransformation = True
      if self.singleTransformation:
        if not self.singleTransformation == transID:
          gLogger.verbose("TransformationAgent.execute: Skipping %s (not selected)." % transID)
          processTransformation = False

      if processTransformation:
        startTime = time.time()
        # process the active transformations
        if transStatus == 'Active':
          gLogger.info(self.name+".execute: Processing transformation '%s'." % transID)
          result = self.processTransformation(transDict, False)
          if result['OK']:
            nJobs = result['Value']
            gLogger.info(self.name+".execute: Transformation '%s' processed in %s seconds." % (transID,time.time()-startTime))
            if nJobs > 0:
              gLogger.info('%d job(s) generated' % nJobs)
          else:
            gLogger.warn('Error while processing: '+result['Message'])

        # flush transformations
        elif transStatus == 'Flush':
          gLogger.info(self.name+".execute: Flushing transformation '%s'." % transID)
          result = self.processTransformation(transDict, True)
          if not result['OK']:
            gLogger.error(self.name+".execute: Failed to flush transformation '%s'." % transID, res['Message'])
          else:
            gLogger.info(self.name+".execute: Transformation '%s' flushed in %s seconds." % (transID,time.time()-startTime))
            nJobs = result['Value']
            if nJobs > 0:
              gLogger.info('%d job(s) generated' % nJobs)
            result = server.setTransformationStatus(transID, 'Active')
            if not result['OK']:
              gLogger.error(self.name+".execute: Failed to update transformation status to 'Active'.", res['Message'])
            else:
              gLogger.info(self.name+".execute: Updated transformation status to 'Active'.")

        # skip the transformations of other statuses
        else:
          gLogger.verbose("TransformationAgent.execute: Skipping transformation '%s' with status %s." % (transID,transStatus))

    return S_OK()

  #############################################################################
  def processTransformation(self, transDict, flush=False):
    """ Process one Transformation defined by its dictionary. Jobs are generated
        using various plugin functions of the kind generateJob_<plugin_name>.
        The plugin name is defined in the Production parameters. If not defined,
        'Standard' plugin is used.
    """

    available_plugins = ['CCRC_RAW','Standard','BySize','ByRunStandard','ByRunCCRC_RAW','ByRunBySize']

    prodID = long(transDict['TransID'])
    prodName = transDict['Name']
    group_size = int(transDict['GroupSize'])
    plugin = transDict['Plugin']
    if not plugin in available_plugins:
      plugin = 'Standard'
    server = RPCClient('ProductionManagement/ProductionManager')

    res = server.getTransformationLFNs(prodName)
    if not res['OK']:
      gLogger.warn("Failed to get data for transformation","%s %s" % (prodName,res['Message']))
      return res
    lfns = res['Value']
    if not lfns:
      return S_OK(0)
    res = self.getDataReplicas(prodID,lfns)
    if not res['OK']:
      gLogger.error("Failed to obtain data replicas for transformation","%s %s" % (prodName,res['Message']))
      return res
    data = res['Value']

    ancestorDepth = 0
    if transDict.has_key('Additional'):
      if transDict['Additional'].has_key('AncestorDepth'):
        ancestorDepth = int(transDict['Additional']['AncestorDepth'])

    if ancestorDepth > 0:
      data_m = []
      ancestorSEDict = {}
      for lfn,se in data:
        # Find SEs allowed by the ancestor presence
        if not ancestorSEDict.has_key(lfn):
          result = self.checkAncestors(lfn,ancestorDepth)
          if result['OK']:
            ancestorSites = result['Value']
          else:
            result = server.setFileStatusForTransformation(prodID,[('AncestorProblem',[lfn])])
            ancestorSites = []
        result = self.getSiteForSE(se)
        if not result['OK']:
          continue
        fileSite = result['Value']
        if fileSite in ancestorSites:
          data_m.append((lfn,se))
      data = data_m

    # Obtain the sizes for the files from the catalog
    data_m = data
    res = self.rm.getCatalogFileSize(lfns)
    fileSizes = {}
    if not res['OK']:
      gLogger.error("Failed to get file sizes.")
    elif res['Value']['Failed']:
      gLogger.error("Failed to get file sizes for %s files" % len(res['Value']['Failed'].keys()))
    else:
      fileSizes = res['Value']['Successful']

    nJobs = 0
    if flush:
      while len(data) >0:
        ldata = len(data)
        data = eval('self.generateJob_'+plugin+'(data,prodID,transDict,flush,fileSizes)')
        if ldata == len(data):
          break
        else:
          nJobs += 1
    else:
      while len(data) >= group_size:
        ldata = len(data)
        data = eval('self.generateJob_'+plugin+'(data,prodID,transDict,flush,fileSizes)')
        if ldata == len(data):
          break
        else:
          nJobs += 1

    gLogger.verbose('%d jobs created' % nJobs)
    return S_OK(nJobs)

  #####################################################################################
  # For by-run splitting

  def generateJob_ByRunStandard(self,data,production,transDict,flush=False,fileSizes={}):
    return self.__ByRun(data,production,transDict,flush,fileSizes,'Standard')

  def generateJob_ByRunBySize(self,data,production,transDict,flush=False,fileSizes={}):
    return self.__ByRun(data,production,transDict,flush,fileSizes,'BySize')

  def generateJob_ByRunCCRC_RAW(self,data,production,transDict,flush=False,fileSizes={}):
    return self.__ByRun(data,production,transDict,flush,fileSizes,'CCRC_RAW')

  def __ByRun(self,data,production,transDict,flush=False,fileSizes={},additionalPlugin='Standard'):
    """ Generate a job grouping the files by run
    """
    lfnDict = {}
    for lfn,se in data:
      lfnDict[lfn] = se
    bk = RPCClient('Bookkeeping/BookkeepingManager')
    # group data by run
    start = time.time()
    res = bk.getFileMetadata(lfnDict.keys())
    gLogger.verbose("Obtained BK file metadata in %.2f seconds" % (time.time()-start))
    if not res['OK']: 
      gLogger.error("Failed to get bookkeeping metadata",res['Message'])
      return res
    if not res['Value']:
      return []
    runDict = {}
    for lfn,metadata in res['Value'].items():
      runNumber = 0
      if metadata.has_key("RunNumber"):
        runNumber = metadata["RunNumber"]
      if not runDict.has_key(runNumber):
        runDict[runNumber] = []
      runDict[runNumber].append((lfn,lfnDict[lfn]))
    # for each run try and create the jobs
    group_size = int(transDict['GroupSize'])
    for runNumber,data in runDict.items():
      gLogger.verbose("Creating jobs for Run %d" % runNumber)
      nJobs = 0
      if flush:
        while len(data) > 0:
          ldata = len(data)
          data = eval('self.generateJob_%s(data,production,transDict,flush,fileSizes)' % additionalPlugin)
          if ldata == len(data):
            break
          else:
            nJobs += 1
      else:
        while len(data) >= group_size:   
          ldata = len(data)
          data = eval('self.generateJob_%s(data,production,transDict,flush,fileSizes)' % additionalPlugin)
          if ldata == len(data):
            break
          else:
            nJobs += 1
      gLogger.verbose('%d jobs created for run %d' % (nJobs,runNumber))
    return []

  #####################################################################################
  def generateJob_BySize(self,data,production,transDict,flush=False,fileSizes={}):
    """ Generate a job according to the CCRC 2008 site shares
    """
    if not fileSizes:
      gLogger.error("Attempting to use BySize plugin and not file sizes provided")
      return S_ERROR("Attempting to use BySize plugin and not file sizes provided")

    input_size = float(transDict['GroupSize'])*1000*1000*1000  # input size in GB converted to bytes
    # Sort files by SE
    datadict = {}
    for lfn,se in data:
      if not datadict.has_key(se):
        datadict[se] = []
      datadict[se].append(lfn)

    data_m = data
    # If we have no data then return
    if not datadict:
      return data_m

    create = False
    # Group files by SE
    if flush: # flush mode
      chosenSE = datadict.keys()[0]
      lfns = datadict[chosenSE]
      create = True
    else: # normal  mode
      for chosenSE in datadict.keys():
        lfns = []
        selectedSize = 0
        candidateFiles = datadict[chosenSE]
        while selectedSize < input_size:
          if not candidateFiles:
            break
          lfn = candidateFiles[0]
          candidateFiles.remove(lfn)
          if fileSizes.has_key(lfn):
            lfns.append(lfn)
            selectedSize += fileSizes[lfn]
        if selectedSize > input_size:
          create = True
          break

    if not create:
      gLogger.verbose("Neither SE has enough input data")
    else:
      # We have found a SE with minimally (or max in case of flush) sufficient amount of data
      res = self.__createJob(production,lfns,chosenSE)
      if res['OK']:
        # Remove used files from the initial list
        data_m = []
        for lfn,se in data:
          if lfn not in lfns:
            data_m.append((lfn,se))
    return data_m

  def __createJob(self, production,lfns,lse):
    dataLog = RPCClient('DataManagement/DataLogging')
    server = RPCClient('ProductionManagement/ProductionManager')
    result = self.addJobToProduction(production,lfns,lse)
    if not result['OK']:
      gLogger.warn("Failed to add a new job to repository: "+result['Message'])
      return result
    jobID = long(result['Value'])
    if not jobID:
      gLogger.warn("Failed to obtain jobID for newly create job")

    result = server.setFileStatusForTransformation(production,[('Assigned',lfns)])
    if not result['OK']:
      gLogger.error("Failed to update file status for production %d" % production)
      
    result = server.setFileJobID(production,jobID,lfns)
    if not result['OK']:
      gLogger.error("Failed to set file job ID for production %d" % production)
      
    result = server.setFileSEForTransformation(production,lse,lfns)
    if not result['OK']:
      gLogger.error("Failed to set SE for production %d" % production)
    for lfn in lfns:
      result = dataLog.addFileRecord(lfn,'Job created','ProdID: %s JobID: %s' % (production,jobID),'','TransformationAgent')
    return S_OK()

  #####################################################################################
  def generateJob_CCRC_RAW(self,data,production,transDict,flush=False,fileSizes={}):
    """ Generate a job according to the CCRC 2008 site shares
    """

    group_size = int(transDict['GroupSize'])
    dataLog = RPCClient('DataManagement/DataLogging')
    server = RPCClient('ProductionManagement/ProductionManager')
    # Sort files by LFN
    datadict = {}
    for lfn,se in data:
      if not datadict.has_key(lfn):
        datadict[lfn] = []
      datadict[lfn].append(se)

    data_m = data

    lse = ''
    selectedLFN = ''
    for lfn,seList in datadict.items():
      if len(seList) == 1:
        if seList[0].find('CERN') == -1:
          gLogger.warn('Single replica of %s not at CERN: %s' % (lfn,seList[0]))
          print lfn,seList
        continue

      if len(seList) > 1:
        # Check that CERN se is in the list
        okCERN = False
        seCERN = ''
        seOther = ''
        for se in seList:
          if se.find('CERN') != -1:
            seCERN = se
            okCERN = True
          else:
            seOther = se

        if okCERN:
          # Try to satisfy the CERN share
          if random.random() < self.CERNShare:
            lse = seCERN
          else:
            lse = seOther
          selectedLFN = lfn
          break
        else:
          gLogger.warn('No replicas of %s at CERN' % lfn)
          continue

    if not lse and flush:
      # Send job to where it can go
      for lfn,seList in datadict.items():
        selectedLFN = lfn
        lse = seList[0]

    if lse:
      lfns = [selectedLFN]
      result = self.addJobToProduction(production,lfns,lse)
      if result['OK']:
        jobID = long(result['Value'])
        if jobID:
          result = server.setFileStatusForTransformation(production,[('Assigned',lfns)])
          if not result['OK']:
            gLogger.error("Failed to update file status for production %d"%production)

          result = server.setFileJobID(production,jobID,lfns)
          if not result['OK']:
            gLogger.error("Failed to set file job ID for production %d"%production)

          result = server.setFileSEForTransformation(production,lse,lfns)
          if not result['OK']:
            gLogger.error("Failed to set SE for production %d"%production)
          for lfn in lfns:
            result = dataLog.addFileRecord(lfn,'Job created','JobID: %s' % jobID,'','TransformationAgent')

        # Remove used files from the initial list
        data_m = []
        for lfn,se in data:
          if lfn not in lfns:
            data_m.append((lfn,se))
      else:
        gLogger.warn("Failed to add a new job to repository: "+result['Message'])
    else:
      gLogger.warn('No eligible LFNs for production %d'%production)

    return data_m

  #####################################################################################
  def generateJob_Standard(self,data,production,transDict,flush=False,fileSizes={}):
    """ Generates a job based on the input data, adds job to the repository
        and returns a reduced list of the lfns that rest to be processed
        If flush is true, the group_size is not taken into account
    """

    group_size = int(transDict['GroupSize'])
    dataLog = RPCClient('DataManagement/DataLogging')
    server = RPCClient('ProductionManagement/ProductionManager')
    # Sort files by SE
    datadict = {}
    for lfn,se in data:
      if not datadict.has_key(se):
        datadict[se] = []
      datadict[se].append(lfn)

    data_m = data

    # Group files by SE

    if flush: # flush mode
      # Find the SE with maximum sufficient amount of data
      lmax = 0
      lse = ''
      for se in datadict.keys():
        ldata = len(datadict[se])
        if ldata > lmax:
          lmax = ldata
          lse = se
      if lmax < group_size:
        group_size = lmax

    else: # normal  mode
      # Find the SE with the minimally sufficient amount of data
      lmin = len(data)+1
      lse = ''
      for se in datadict.keys():
        ldata = len(datadict[se])
        if ldata < lmin and ldata >= group_size:
          lmin = ldata
          lse = se

    if lse:
      # We have found a SE with minimally(or max in case of flush) sufficient amount of data
      lfns = datadict[lse][:group_size]
      result = self.addJobToProduction(production,lfns,lse)
      if result['OK']:
        jobID = long(result['Value'])
        if jobID:
          result = server.setFileStatusForTransformation(production,[('Assigned',lfns)])
          if not result['OK']:
            gLogger.error("Failed to update file status for production %d"%production)

          result = server.setFileJobID(production,jobID,lfns)
          if not result['OK']:
            gLogger.error("Failed to set file job ID for production %d"%production)

          result = server.setFileSEForTransformation(production,lse,lfns)
          if not result['OK']:
            gLogger.error("Failed to set SE for production %d"%production)
          for lfn in lfns:
            result = dataLog.addFileRecord(lfn,'Job created','ProdID: %s JobID: %s' % (production,jobID),'','TransformationAgent')

        # Remove used files from the initial list
        data_m = []
        for lfn,se in data:
          if lfn not in lfns:
            data_m.append((lfn,se))
      else:
        gLogger.warn("Failed to add a new job to repository: "+result['Message'])

    else:
      gLogger.verbose("Neither SE has enough input data")

    return data_m

  ######################################################################################
  def addJobToProduction(self, prodID, lfns, se):
    """ Adds a new job to the production giving an lfns list of input files.
        Argument se can be used to specify the target destination if necessary
    """

    #inputVector = {}
    #inputVector['InputData'] = lfns
    #lfns is the list!! we have to convert it into string with proper separator
    vector =""
    for lfn in lfns:
      vector = vector + 'LFN:'+lfn+';'
    #removing last ';'
    vector = vector.rstrip(';')
    server = RPCClient('ProductionManagement/ProductionManager')
    result = server.addProductionJob(prodID, vector, se)
    return result

  ######################################################################################

  def getDataReplicas(self,production,lfns):
    """ Get the replicas for the LFNs and check their statuses
    """
    start = time.time()
    result = self.rm.getActiveReplicas(lfns)
    delta = time.time() - start
    gLogger.verbose('Replica results for %d files obtained in %.2f seconds' % (len(lfns),delta))
    lfc_datadict = {}
    lfc_data = []
    if not result['OK']:
      return result
    failover_lfns = []
    replicas = result['Value']['Successful']
    for lfn, replicaDict in replicas.items():
      lfc_datadict[lfn] = []
      for se,pfn in replicaDict.items():
        # Do not consider replicas in FAILOVER type storage
        if se.lower().find('failover') == -1:
          lfc_datadict[lfn].append(se)
          lfc_data.append((lfn,se))
        else:
          failover_lfns.append(lfn)
    # Check the input files if they are known by LFC
    missing_lfns = []
    for lfn,reason in result['Value']['Failed'].items():
      if re.search("No such file or directory",reason):
        missing_lfns.append(lfn)
        gLogger.warn('LFN: %s not found in the LFC' % lfn)
    if missing_lfns: 
      # Mark this file in the transformation
      server = RPCClient('ProductionManagement/ProductionManager')
      result = server.setFileStatusForTransformation(production,[('MissingLFC',missing_lfns)])
      if not result['OK']:
        gLogger.warn(result['Message'])
    return S_OK(lfc_data)

  def checkAncestors(self,lfn,ancestorDepth):
    """ Check ancestor availability on sites. Returns a list of SEs where all the ancestors
        are present
    """

    result = getAncestorFiles(lfn,ancestorDepth)
    if not result['OK']:
      gLogger.warn(result['Message'])
      return result

    fileList = result['Value']
    if not fileList:
      return S_ERROR('No ancestors returned')
    numAncestors = len(fileList)

    # Determine common SEs now
    result = self.rm.getActiveReplicas(fileList)
    if not result['OK']:
      gLogger.warn(result['Message'])
      return S_ERROR('Failed to get results from LFC: %s' % result['Message'])
    replicas = result['Value']['Successful']
    if numAncestors != len(replicas):
      return S_ERROR('Can not find all replica info: ancestors %d, replicas %d' % (numAncestors,len(replicas)))

    ancestorSites = []
    for lfn, replicaDict in replicas.items():
      ancestorSEs = replicaDict.keys()
      ancestorSites = self.getSitesForSEs(ancestorSEs)
      break

    for lfn, replicaDict in replicas.items():
      SEs = replicaDict.keys()
      sites = self.getSitesForSEs(SEs)
      tmp_sites = []
      for site in sites:
        if site in ancestorSites:
          tmp_sites.append(site)
      ancestorSites = tmp_sites
      if not ancestorSites:
        break
    return S_OK(ancestorSites)

  def getSiteForSE(self,se):
    """ Get site name for the given SE
    """

    result = getSitesForSE(se)
    if not result['OK']:
      return result
    if result['Value']:
      return S_OK(result['Value'][0])

    return S_OK('')

  def getSitesForSEs(self, seList):
    """ Get all the sites for the given SE list
    """

    sites = []
    for se in seList:
      result = getSitesForSE(se)
      if result['OK']:
        sites += result['Value']

    return sites
