"""  ReplicationPlacementAgent determines the replications to be performed based on operations defined in the operations database
"""

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse
from DIRAC.RequestManagementSystem.Client.Request import RequestClient
from DIRAC.RequestManagementSystem.Client.DataManagementRequest import DataManagementRequest
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.DataManagementSystem.Client.Catalog import LcgFileCatalogCombinedClient
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
    # need to get the online requestDB URL.'http://lbora01.cern.ch:9135'
    self.RequestDBClient = RequestClient()
    self.ReplicaManager = ReplicaManager()
    return result

  def execute(self):
    return S_OK()




  def initialize(self):

    AgentBase.initialize(self)

    mode = cfgSvc.get( "Site", "Mode" )
    # get the dirac scripts path
    self.scriptdir = cfgSvc.get( "Site", "Root" )+"/DIRAC/scripts"
    # get urls for the processing db and adtdb urls
    adtDB_URL = cfgSvc.get( mode, "AutoDataTransferDBURL")
    self.adtDB = xmlrpclib.Server(adtDB_URL)
    procDB_URL = cfgSvc.get( mode, "ProcessingDBURL" )
    self.procDB = xmlrpclib.Server(procDB_URL)



    prod_id = cfgSvc.get( "ReplicationAgent", "ProductionID")
    if prod_id == "ALL":
      self.production = None
      print 'Initializing general purpose Replication Agent'
    else:
      self.production = prod_id
      print "Initializing Replication Agent for production",prod_id

    print'Obtaining the config values of the active productions'
    #activeTransforms = self.procDB.getTransformationsWithStatus('Active')
    activeTransforms = self.adtDB.getTransformationsWithStatus('Active')
    print activeTransforms
    self.activeProductionConfig = {}
    if activeTransforms['Status'] == 'OK':
      for transform in activeTransforms['Transformations']:
        inputStream = self.getTransformationStream(transform['Production'])
        sname = inputStream['Value'][1]
        self.activeProductionConfig[sname] = {}
        self.activeProductionConfig[sname]['TargetSEs'] = cfgSvc.get(sname,'targetse').replace(' ','').split(',')
        self.activeProductionConfig[sname]['SourceSE'] = cfgSvc.get(sname,'sourcese')
        self.activeProductionConfig[sname]['Broadcast'] = cfgSvc.get(sname,'Broadcast')
        self.activeProductionConfig[sname]['DistinguishSites'] =  cfgSvc.get(sname,'DistinguishSites')


  def execute(self):

    if self.production:
      # Transformation agent is defined for a definite production
      if DEBUG:
        print "Processing Transformation for Production",self.production
      result = self.processTransformation(self.production)
      return result
    else:
      OK = 1
      # Process all the active transformations otherwise
      #result = self.procDB.getTransformationsWithStatus('Active')
      result = self.adtDB.getTransformationsWithStatus('Active')
      if result['Status'] == "OK":
        transformations = result['Transformations']
        for transformation in transformations:
          production = transformation['Production']
          if DEBUG:
            print "Processing Transformation for Production",production
            start = time.time()
          result = self.processTransformation(production)
          if DEBUG:
            total = time.time() - start
            print "Processing done in",total,"seconds"
          if result['Status'] != "OK":
            OK = 0
      else:
        return S_ERROR('Can not get data from the Processing DB')

    if not OK:
      return S_ERROR('Can not process all the active transformations')
    else:
      return S_OK()


  def getTransformationStream(self,prodID,active=True):
    """Get definition of the input stream for an Active Transformation,
       return error if the Transformation defined by prodID is not active
       or stream data is not available
    """

    #result = self.procDB.getTransformation(prodID)
    result = self.adtDB.getTransformation(prodID)

    #print result

    if result['Status'] == "OK":
      transformation = result['Transformation']
      status = transformation['Status']
      if active:
        if status != "Active":
          result = S_ERROR('Transformation is not Active')
          return result
    else:
      result = S_ERROR('Can not get data for transformation '+str(prodID))
      return result

    transID = transformation['TransID']

    #result = self.procDB.getInputStreams(transID)
    result = self.adtDB.getInputStreams(transID)
    if result['Status'] == "OK":
      inputs = result['InputStreams']
      if len(inputs) > 1:
        print "Multiple input streams are not supported yet"
        return S_ERROR("Multiple input streams are not supported yet")
      else:
        sname = inputs.keys()[0]
        mask,site_flag,group_size = inputs[sname]
        sflag = 1
        if site_flag == "False": sflag = 0
    else:
      result = S_ERROR('Can not get data for transformation '+str(prodID))
      return result

    result = S_OK((transID,sname,mask,sflag,group_size))
    return result

  def processTransformation(self,prodID):
    """Process one Transformation defined by prodID
    """
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
            """
            else:
              print 'isnt boradcast!!!!!!!!!!!!'
              #get all target ses of lfn transforms
              transTargetSEs = self.activeProductionConfig[sname]['TargetSEs']
              print 'target sesi of transform', transTargetSEs
              #if the lfn is active in any transform with a targetSE the same as that of the current transformation then don't include
              for transTargetSE in transTargetSEs:
                if targetSEs.__contains__(transTargetSE):
                  exclude = 1
            """
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
    """Generate a job

       Generates a job based on the input data, adds job to the repository
       and returns a reduced list of the lfns that rest to be processed
    """
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

class TransformationGrouping:

  def loadBalance(self):
    pass

  def broadCast(self):
    pass
