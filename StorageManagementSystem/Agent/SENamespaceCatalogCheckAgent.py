########################################################################
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"

from DIRAC                                                     import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base.AgentModule                               import AgentModule
from DIRAC.DataManagementSystem.Client.ReplicaManager          import ReplicaManager
from DIRAC.Core.Utilities.List                                 import sortList, breakListIntoChunks

import re, urllib2


AGENT_NAME = 'DataManagement/SENamespaceCatalogCheckAgent'

class SENamespaceCatalogCheckAgent( AgentModule ):

  #############################################################################
  def initialize( self ):
    """Sets defaults
    """
    self.replicaManager = ReplicaManager()

    # This sets the Default Proxy to used as that defined under 
    # /Operations/Shifter/DataManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )

    return S_OK()

  #############################################################################
  def execute( self ):
    """ The SENamespaceCatalogCheck execution method.
    """
    self.enableFlag = self.am_getOption( 'EnableFlag', 'True' )
    if not self.enableFlag == 'True':
      self.log.info( 'SENamespaceCatalogCheck is disabled by configuration option EnableFlag' )
      return S_OK( 'Disabled via CS flag' )

    """
    castorLocations = {  'http://castor.web.cern.ch/castor/DiskPoolDump/lhcb.lhcbmdst.last'      : ['CERN_M-DST'],
                         'http://castor.web.cern.ch/castor/DiskPoolDump/lhcb.lhcbraw.last'       : ['CERN-RAW'],
                         'http://castor.web.cern.ch/castor/DiskPoolDump/lhcb.lhcbrdst.last'      : ['CERN-RDST','CERN-tape'],
                         'http://castor.web.cern.ch/castor/DiskPoolDump/lhcb.lhcbfailover.last'  : ['CERN-FAILOVER','CERN-HIST','CERN-DEBUG'],
                         'http://castor.web.cern.ch/castor/DiskPoolDump/lhcb.lhcbuser.last'      : ['CERN-USER']}
                         'http://castor.web.cern.ch/castor/DiskPoolDump/lhcb.lhcbdata.last'      : ['CERN-disk','CERN_MC_M-DST'],
    """
    castorLocations = {  'http://castor.web.cern.ch/castor/DiskPoolDump/lhcb.lhcbdata.last'      : ['CERN-disk', 'CERN_MC_M-DST']}
    for dumpLocation, storageElements in castorLocations.items():
      gLogger.info( 'Attempting to get dump at %s for SEs %s' % ( dumpLocation, storageElements ) )
      res = self.__getDump( dumpLocation )
      if not res['OK']:
        return res
      res = self.verifyCastorDump( res['Value'], storageElements )
      if not res['OK']:
        gLogger.error( "Failed to verify Castor dump", res['Message'] )
      else:
        gLogger.info( "Successfully completed Castor dump for %s" % storageElements )
    return S_OK()

  def verifyCastorDump( self, dump, storageElements ):
    pfns = self.__getCastorPfns( dump )
    pfnsDict = {}
    for pfn in pfns:
      if not re.search( 'dirac_directory', pfn[0] ):
        pfnsDict[pfn[0]] = pfn[1]
    res = self.__verifyPfns( pfnsDict, storageElements )
    if not res['OK']:
      return res
    pfnsToRemove = res['Value']['Remove']
    incorrectlyRegistered = res['Value']['ReRegister']
    """
    inputFileName = os.path.basename()
    if pfnsToRemove:
      gLogger.info("Found %d files to remove from %s" % (len(pfnsToRemove),storageElements[0]))
      outputFile = open('%s-toRemove' % inputFileName,'w')
      for pfn in sortList(pfnsToRemove):
        outputFile.write('%s\n' % pfn)
      outputFile.close()
    if incorrectlyRegistered:
      gLogger.info("Found %d files incorrectly registered on %s" % (len(incorrectlyRegistered),storageElements[0]))
      outputFile = open('%s-wrongSE' % inputFileName,'w')
      for lfn in sortList(incorrectlyRegistered):
        outputFile.write('%s\n' % lfn)
      outputFile.close()
    """
    return S_OK()

  def __verifyPfns( self, pfnSizes, storageElements ):
    gLogger.info( 'Checking %s storage files exist in the catalog' % len( pfnSizes ) )
    pfnsToRemove = []
    incorrectlyRegistered = []
    allDone = True
    # First get all the PFNs as they should be registered in the catalog
    for pfns in breakListIntoChunks( sortList( pfnSizes.keys() ), 100 ):
      res = self.replicaManager.getPfnForProtocol( pfns, storageElements[0], withPort = False )
      if not res['OK']:
        allDone = False
        continue
      for pfn, error in res['Value']['Failed'].items():
        gLogger.error( 'Failed to obtain registered PFN for physical file', '%s %s' % ( pfn, error ) )
      if res['Value']['Failed']:
        allDone = False
      catalogStoragePfns = res['Value']['Successful']
      # Determine whether these PFNs are registered and if so obtain the LFN
      res = self.replicaManager.getCatalogLFNForPFN( catalogStoragePfns.values() )
      if not res['OK']:
        allDone = False
        continue
      for surl in sortList( res['Value']['Failed'].keys() ):
        if res['Value']['Failed'][surl] == 'No such file or directory':
          #pfnsToRemove.append(surl)
          print surl
        else:
          gLogger.error( 'Failed to get LFN for PFN', '%s %s' % ( surl, res['Value']['Failed'][surl] ) )
      existingLFNs = res['Value']['Successful'].values()
      if existingLFNs:
        res = self.replicaManager.getCatalogReplicas( existingLFNs )
        if not res['OK']:
          allDone = False
          continue
        for lfn, error in res['Value']['Failed'].items():
          gLogger.error( 'Failed to obtain registered replicas for LFN', '%s %s' % ( lfn, error ) )
        if res['Value']['Failed']:
          allDone = False
        for lfn, replicas in res['Value']['Successful'].items():
          match = False
          for storageElement in storageElements:
            if storageElement in replicas.keys():
              match = True
          if not match:
            pass#incorrectlyRegistered.append(lfn)
            #print lfn
    gLogger.info( "Verification of PFNs complete" )
    if incorrectlyRegistered:
      gLogger.info( "Found %d files incorrectly registered" % len( incorrectlyRegistered ) )
    if pfnsToRemove:
      gLogger.info( "Found %d files to be removed" % len( pfnsToRemove ) )
    resDict = {'Remove':pfnsToRemove, 'ReRegister':incorrectlyRegistered, 'AllDone':allDone}
    return S_OK( resDict )

  def __getDCachePfns( self, dumpStr ):
    #exp = re.compile(r'<entry name="(\S+)"><size>(\d+)</size><checksum name="adler32">(\S+)</checksum></entry>')
    exp = re.compile( r'<entry name="(\S+)"><size>(\d+)</size>' )#</entry>')
    return re.findall( exp, dumpStr )

  def __getCastorPfns( self, dumpStr ):
    exp = re.compile( r'(\S+) \(\d+\) size (\d+)' )#</entry>')
    return re.findall( exp, dumpStr )

  def __getDump( self, location ):
    try:
      response = urllib2.urlopen( location )
      dumpStr = response.read()
      gLogger.info( "Found %s lines in the dump" % len( dumpStr.splitlines() ) )
      return S_OK( dumpStr )
    except Exception, x:
      gLogger.exception( "Exception getting dump", location, x )
      return S_ERROR( "Exception getting dump" )
