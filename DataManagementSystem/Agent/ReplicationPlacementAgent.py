"""  ReplicationPlacementAgent determines the replications to be performed based on operations defined in the operations database
"""

from DIRAC  import gLogger,gMonitor, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.RequestManagementSystem.Client.Request import RequestClient
from DIRAC.RequestManagementSystem.Client.DataManagementRequest import DataManagementRequest
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.DataManagementSystem.Client.FileCatalog import FileCatalog
from DIRAC.DataManagementSystem.Client.Catalog.PlacementDBClient import PlacementDBClient
from DIRAC.Core.DISET.RPCClient import RPCClient

import time
from types import *

AGENT_NAME = 'DataManagement/ReplicationPlacementAgent'

class ReplicationPlacementAgent(Agent):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    result = Agent.initialize(self)
    #self.RequestDB = RequestClient()
    self.PlacementDB = PlacementDBClient()
    self.server = RPCClient("DataManagement/PlacementDB")
    gMonitor.registerActivity("Iteration","Agent Loops","ReplicationPlacementAgent","Loops/min",gMonitor.OP_SUM)
    return result

  def execute(self):
    gMonitor.addMark('Iteration',1)

    transName = gConfig.getValue(self.section+'/Transformation')
    if transName:
      self.singleTransformation = transName
      gLogger.info("Initializing Replication Agent for transformation %s." % transName)
    else:
      self.singleTransformation = False
      gLogger.info("ReplicationPlacementAgent.execute: Initializing general purpose agent.")

    res = self.server.getAllTransformations()
    activeTransforms = []
    if not res['OK']:
      gLogger.error("ReplicationPlacementAgent.execute: Failed to get transformations.", res['Message'])

    for transDict in res['Value']:
      transName = transDict['Name']
      transStatus = transDict['Status']

      processTransformation = True
      if self.singleTransformation:
        if not self.singleTransformation == transName:
          gLogger.info("ReplicationPlacementAgent.execute: Skipping %s (not selected)." % transName)
          processTransformation = False

      if processTransformation:
        startTime = time.time()
        # process the active transformations
        if transStatus == 'Active':
          gLogger.info("ReplicationPlacementAgent.execute: Processing transformation '%s'." % transName)
          res = self.processTransformation(transDict, False)
          gLogger.info("ReplicationPlacementAgent.execute: Transformation '%s' processed in %s seconds." % (transName,time.time()-startTime))

        # flush transformations
        elif transStatus == 'Flush':
          gLogger.info("ReplicationPlacementAgent.execute: Flushing transformation '%s'." % transName)
          res = self.processTransformation(transDict, True)
          if not res['OK']:
            gLogger.error("ReplicationPlacementAgent.execute: Failed to flush transformation '%s'." % transName, res['Message'])
          else:
            gLogger.info("ReplicationPlacementAgent.execute: Transformation '%s' flushed in %s seconds." % (transName,time.time()-startTime))
            res = self.server.setTransformationStatus(transName, 'Stopped')
            if not res['OK']:
              gLogger.error("ReplicationPlacementAgent.execute: Failed to update transformation status to 'Stopped'.", res['Message'])
            else:
              gLogger.info("ReplicationPlacementAgent.execute: Updated transformation status to 'Stopped'.")

        # skip the transformations of other statuses
        else:
          gLogger.info("ReplicationPlacementAgent.execute: Skipping transformation '%s' with status %s." % (transName,transStatus))

    return S_OK()

  def processTransformation(self,transDict,flush=False):
    """ Process a single transformation
    """
    res = self.server.getInputData(transDict['Name'],'')
    if not res['OK']:
      errStr = "ReplicationPlacementAgent.processTransformation: Failed to obtain input data."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    data = res['Value']
    plugin = transDict['Plugin']
    params = transDict['Additional']

    ### need to instanciate the plugin module to group the files
    ### need another module which will submit the jobs/transfers
    return S_OK()

    """
    transName = 'T0-Export'
    desciption = 'Export of RAW file from T0 to the Tier1s'
    longDesription = ''
    type = 'Replication'
    mode = 'Automatic'
    fileMask = '/*'
    res = self.server.publishTransformation(transName, desciption,longDesription, type, mode, fileMask)
    print res

    import time
    lfn = '/lfn/file.%s' % time.time()
    pfn = 'srm://host:/path/%s' % lfn
    size = 0
    se = 'storage-element'
    guid = 'IGNORED-GUID'
    checksum = 'IGNORED-CHECKSUM'

    fileTuple = (lfn,pfn,size,se,guid,checksum)
    res = self.PlacementDB.addFile(fileTuple)
    print res
    if not res['OK']:
      print res

    replicaTuple = (lfn,pfn,se+'2',False)
    res = self.PlacementDB.addReplica(replicaTuple)
    print res
    if not res['OK']:
      print res

    replicaTuple = (lfn,se)
    res = self.PlacementDB.getReplicaStatus(replicaTuple)
    if not res['OK']:
      print res

    replicaTuple = (lfn,pfn,se,'storage-element3')
    res = self.PlacementDB.setReplicaHost(replicaTuple)
    print res
    if not res['OK']:
      print res

    res = self.PlacementDB.getReplicas(lfn)
    print res
    if not res['OK']:
      print res

    replicaTuple = (lfn,pfn,se,'Problematic')
    res = self.PlacementDB.setReplicaStatus(replicaTuple)
    if not res['OK']:
      print res

    replicaTuple = (lfn,pfn,se)
    res = self.PlacementDB.removeReplica(replicaTuple)
    if not res['OK']:
      print res

    res = self.PlacementDB.removeFile(lfn)
    if not res['OK']:
      print res
    """

  """

  def processTransformation(self,prodID):
    result = self.getTransformationStream(prodID)
    if result['Status'] == "OK":
      transID,sname,mask,sflag,group_size = result['Value']
      #result = self.procDB.getInputDataForStream(transID,sname,'AprioriGood')
      result = self.adtDB.getInputDataForStream(transID,sname,'AprioriGood')
      if result['Status'] == "OK":
        data = result['Data']
      else:
        print "Failed to get data for stream",sname,"transformation",transID
        return S_ERROR("Failed to get data for stream "+sname)

      if DEBUG:
        print "Input data number of files",len(data)

      sourceSE = self.activeProductionConfig[sname]['SourceSE']
      targetSEs = self.activeProductionConfig[sname]['TargetSEs']
      sourceData = []
      for lfn,se in data:
        #Make sure that file is available at transformation source site
        if se == sourceSE:
          #Get all transformations for which the lfn is active
          #eventually want if() logic for differing transformation types
          #if this transforma is broadcast:
          #result = self.procDB.getTransformationsForLFN(lfn)
          result = self.adtDB.getTransformationsForLFN(lfn)
          exclude = 0
          for lfnTransform in result['Transformations']:
            #get the sname of the transformation
            transsname = self.getTransformationStream(lfnTransform['Production'])['Value'][1]
            #if this transformation is a broadcast then the lfn should be exluded
            if self.activeProductionConfig[transsname]['Broadcast'] == '1':
              exclude = 1
            #else:
            #  print 'isnt boradcast!!!!!!!!!!!!'
            #  #get all target ses of lfn transforms
            #  transTargetSEs = self.activeProductionConfig[sname]['TargetSEs']
            #  print 'target sesi of transform', transTargetSEs
            #  #if the lfn is active in any transform with a targetSE the same as that of the current transformation then don't include
            #  for transTargetSE in transTargetSEs:
            #    if targetSEs.__contains__(transTargetSE):
            #      exclude = 1
          if exclude == 0:
            #print 'LFN '+lfn+ ' selected for transformation '+sname
            sourceData.append((lfn,se))

      group_size = int(cfgSvc.get(sname, 'GroupSize'))
      print 'GroupSize: ' + str(group_size)
      print len(sourceData)
      while len(sourceData) >= group_size:
        ldata = len(sourceData)
        sourceData = self.generateJob(sourceData,prodID,transID,sname,sflag,group_size)
        if ldata == len(sourceData): break

      return S_OK()
    else:
      if result['Value'] == 'Transformation is not Active':
        return S_OK()
      else:
        return result

  def generateJob(self,data,production,transID,sname,sflag,group_size):
    targetSites = self.activeProductionConfig[sname]['TargetSEs']
    distinguishSites= self.activeProductionConfig[sname]['DistinguishSites']
    broadcast = self.activeProductionConfig[sname]['Broadcast']
    sourcese = self.activeProductionConfig[sname]['SourceSE']

    siteLoad = {}
    if distinguishSites == '1':
       print 'Distinguishing Sites For Load Balancing'
       for site in targetSites:
         siteLoad[site] = int(cfgSvc.get(sname, site))
       #Find out the sum of the numbers obtained from the config service
       totalratio = 0
       for ratio in siteLoad.values():
         totalratio += ratio
       #Find the number of files per site based on obtained ratio
       totalLoad = 0
       for site in siteLoad.keys():
         #Number of files per site = (site-ratio/total-ratio)*numberoffiles
         numberOfFiles = int((siteLoad[site].__float__()/totalratio)*group_size)
         siteLoad[site] = numberOfFiles
         totalLoad += numberOfFiles
    else:
       #Do not discriminate sites, all get the same number of files
       print 'Not Distinguishing Tier1 Sites'
       #Number of files per site = (numberoffiles/numberofsites)
       numberOfFiles = int(group_size.__float__()/len(targetSites))
       if numberOfFiles == 0:
         numberOfFiles = 1
       totalLoad = numberOfFiles*len(targetSites)
       for site in targetSites:
         siteLoad[site] = numberOfFiles
    print siteLoad


    lfnsedict = {}
    listIndex = 0
    if broadcast == '1':
      infoTuples = data[:numberOfFiles]
      for site in siteLoad.keys():
        lfnsedict[site] = []
        for lfn,se in infoTuples:
          lfnsedict[site].append(lfn)
    else:
      fileinfo = data[:totalLoad]
      for tier1 in siteLoad.keys():
        indexEnd = listIndex+siteLoad[tier1]
        infoTuples = fileinfo[listIndex:indexEnd]
        listIndex = indexEnd
        if len(infoTuples) > 0:
          lfnsedict[tier1] = []
          for lfn,se in infoTuples:
            lfnsedict[tier1].append(lfn)

    for t1site in lfnsedict.keys():
      #files already present at targetsite that will be registered in processingDB, as list of tuples (lfn,pfn,se)
      listOfTuples = []
      #check files to see whether they already exist at target site
      replicas = self.rm.getPFNsForLFNs(lfnsedict[t1site])
      print t1site
      for lfn in replicas:
        if replicas[lfn].keys().__contains__(t1site):
          pfn = replicas[lfn][t1site]
          listOfTuples.append((lfn,pfn,t1site))
          print lfn
      #Mark the files as assigned so they are not picked up by another transformation
      if len(listOfTuples) != 0:
        listOfLfns   = []
        #register the files already present at target in 'production' ProcDB
        result = self.procDB.addPfns(listOfTuples)
        #remove files from transfer job
        for lfn,pfn,se in listOfTuples:
          lfnsedict[t1site].remove(lfn)
          listOfLfns.append(lfn)
        result = self.adtDB.setFileStatusForTransformation(transID,sname,'assigned',listOfLfns)

      #if there are files to submit....
      if len(lfnsedict[t1site]) != 0:
        #create xml, create jdl, submit job
        print t1site
        for lfn in lfnsedict[t1site]:
          print lfn
        xmlfilename = self.createTransferXML(lfnsedict[t1site],sourcese,t1site)
        jdlfilename = self.createTransferJDL(xmlfilename,production)#sname)
        result = self.submitBulkTransfer(jdlfilename)
        if result['Status'] == "OK":
          jobID = result['JobID']
          #result = self.procDB.setFileStatusForTransformation(transID,sname,'assigned',lfnsedict[t1site])
          result = self.adtDB.setFileStatusForTransformation(transID,sname,'assigned',lfnsedict[t1site])
          if result['Status'] != "OK":
            print "Failed to update file status for transformation",self.transID
          #result = self.procDB.setFileJobID(transID,sname,jobID,lfnsedict[t1site])
          result = self.adtDB.setFileJobID(transID,sname,jobID,lfnsedict[t1site])
          if result['Status'] != "OK":
            print "Failed to set file job ID for transformation",transID
        else:
          print "Failed to submit bulk transfer to "+t1site+" to the WMS:",result['Message']

      #Remove used files from the initial list
    data_m = data[totalLoad:]
    return data_m

  """
class TransformationGrouping:

  def loadBalance(self):
    pass

  def broadCast(self):
    pass
