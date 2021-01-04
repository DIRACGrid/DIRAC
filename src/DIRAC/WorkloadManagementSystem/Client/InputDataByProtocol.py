########################################################################
# File :    InputDataByProtocol.py
# Author :  Stuart Paterson
########################################################################

""" The Input Data By Protocol module wraps around the Replica Management
    components to provide access to datasets by available site protocols as
    defined in the CS for the VO.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import six
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.WorkloadManagementSystem.Client.JobStateUpdateClient import JobStateUpdateClient

COMPONENT_NAME = 'InputDataByProtocol'


class InputDataByProtocol(object):

  #############################################################################
  def __init__(self, argumentsDict):
    """ Standard constructor
    """
    self.name = COMPONENT_NAME
    self.log = gLogger.getSubLogger(self.name)
    self.inputData = argumentsDict['InputData']
    self.configuration = argumentsDict['Configuration']
    self.fileCatalogResult = argumentsDict['FileCatalog']
    self.jobID = None
    # This is because  replicas contain SEs and metadata keys!
    # FIXME: the structure of the dictionary must be fixed to avoid this mess
    self.metaKeys = set(['ChecksumType', 'Checksum', 'NumberOfLinks', 'Mode', 'GUID',
                         'Status', 'ModificationDate', 'CreationDate', 'Size',
                         'Owner', 'OwnerGroup', 'GID', 'UID', 'FileID'])

  #############################################################################
  def execute(self, dataToResolve=None):
    """This method is called to obtain the TURLs for all requested input data
       firstly by available site protocols and redundantly via TURL construction.
       If TURLs are missing these are conveyed in the result to
    """

    # Define local configuration options present at every site
    localSEList = self.configuration['LocalSEList']
    self.jobID = self.configuration.get('JobID')
    allReplicas = self.configuration.get('AllReplicas', False)
    if allReplicas:
      self.log.info('All replicas will be used in the resolution')

    if dataToResolve:
      self.log.verbose('Data to resolve passed directly to InputDataByProtocol module')
      self.inputData = dataToResolve  # e.g. list supplied by another module

    if isinstance(self.inputData, six.string_types):
      self.inputData = self.inputData.replace(' ', '').split(',')

    self.inputData = [x.replace('LFN:', '') for x in self.inputData]
    self.log.verbose('InputData requirement to be resolved by protocol is:\n%s' % '\n'.join(self.inputData))

    # First make a check in case replicas have been removed or are not accessible
    # from the local site (remove these from consideration for local protocols)
    replicas = self.fileCatalogResult['Value']['Successful']
    self.log.debug('File Catalogue result is:\n%s' % str(replicas))

    # First get the preferred replica:
    requestedProtocol = self.configuration.get('Protocol', '')
    result = self.__resolveReplicas(localSEList, replicas, requestedProtocol=requestedProtocol)
    if not result['OK']:
      return result
    success = result['Value']['Successful']
    if not allReplicas:
      bestReplica = {}
      for lfn in success:
        bestReplica[lfn] = success[lfn][0]
      return S_OK({'Successful': bestReplica, 'Failed': result['Value']['Failed']})

    # Keep the failed LFNs from local SEs
    failed = set(result['Value']['Failed'])
    # If all replicas are requested, get results for other SEs
    seList = set()
    localSESet = set(localSEList)
    for lfn in list(replicas):
      extraSEs = set(replicas[lfn]) - localSESet
      # If any extra SE, add it to the set, othewise don't consider that file
      if extraSEs:
        seList.update(extraSEs)
      else:
        replicas.pop(lfn)
    seList -= self.metaKeys

    if seList:
      requestedProtocol = self.configuration.get('RemoteProtocol', '')
      result = self.__resolveReplicas(seList, replicas, ignoreTape=True, requestedProtocol=requestedProtocol)
      if not result['OK']:
        return result
      for lfn in result['Value']['Successful']:
        success.setdefault(lfn, []).extend(result['Value']['Successful'][lfn])
      failed.update(result['Value']['Failed'])
    # Only consider failed the files that are not successful as well
    failed -= set(success)
    return S_OK({'Successful': success, 'Failed': sorted(failed)})

  def __resolveReplicas(self, seList, replicas, ignoreTape=False, requestedProtocol=''):
    diskSEs = set()
    tapeSEs = set()
    if not seList:
      return S_OK({'Successful': {}, 'Failed': []})

    for localSE in seList:
      seStatus = StorageElement(localSE).status()
      if seStatus['Read'] and seStatus['DiskSE']:
        diskSEs.add(localSE)
      elif seStatus['Read'] and seStatus['TapeSE']:
        tapeSEs.add(localSE)

    # For the case that a file is found on two SEs at the same site
    # disk-based replicas are favoured.
    # Problematic files will be returned and can be handled by another module
    failedReplicas = set()
    newReplicasDict = {}
    for lfn, reps in replicas.items():
      if lfn in self.inputData:
        # Check that all replicas are on a valid local SE
        if not [se for se in reps if se in diskSEs.union(tapeSEs)]:
          failedReplicas.add(lfn)
        else:
          sreps = set(reps)
          for seName in diskSEs & sreps:
            newReplicasDict.setdefault(lfn, []).append(seName)
          if not newReplicasDict.get(lfn) and not ignoreTape:
            for seName in tapeSEs & sreps:
              newReplicasDict.setdefault(lfn, []).append(seName)

    # Check that all LFNs have at least one replica and GUID
    if failedReplicas:
      # in principle this is not a failure but depends on the policy of the VO
      # datasets could be downloaded from another site
      self.log.info('The following file(s) were found not to have replicas on any of %s:\n%s' %
                    (str(seList), '\n'.join(sorted(failedReplicas))))

    # Need to group files by SE in order to stage optimally
    # we know from above that all remaining files have a replica
    # (preferring disk if >1) in the local storage.
    # IMPORTANT, only add replicas for input data that is requested
    # since this module could have been executed after another.
    seFilesDict = {}
    for lfn, seList in newReplicasDict.items():
      for seName in seList:
        seFilesDict.setdefault(seName, []).append(lfn)

    sortedSEs = sorted(((len(lfns), seName) for seName, lfns in seFilesDict.items()), reverse=True)

    trackLFNs = {}
    for _len, seName in sortedSEs:
      for lfn in seFilesDict[seName]:
        if 'Size' in replicas[lfn] and 'GUID' in replicas[lfn]:
          trackLFNs.setdefault(lfn, []).append({'pfn': replicas.get(lfn, {}).get(seName, lfn),
                                                'se': seName,
                                                'size': replicas[lfn]['Size'],
                                                'guid': replicas[lfn]['GUID']})

    self.log.debug('Files grouped by SEs are:\n%s' % str(seFilesDict))
    for seName, lfns in seFilesDict.items():
      self.log.info(' %s LFNs found from catalog at SE %s' % (len(lfns), seName))
      self.log.verbose('\n'.join(lfns))

    # Can now start to obtain TURLs for files grouped by localSE
    # for requested input data
    for seName, lfns in seFilesDict.items():
      if not lfns:
        continue
      failedReps = set()
      result = StorageElement(seName).getFileMetadata(lfns)
      if not result['OK']:
        self.log.error("Error getting metadata.", result['Message'] + ':\n%s' % '\n'.join(lfns))
        # If we can not get MetaData, most likely there is a problem with the SE
        # declare the replicas failed and continue
        failedReps.update(lfns)
        continue
      failed = result['Value']['Failed']
      if failed:
        # If MetaData can not be retrieved for some PFNs
        # declared them failed and go on
        for lfn in failed:
          lfns.remove(lfn)
          if isinstance(failed, dict):
            self.log.error(failed[lfn], lfn)
          failedReps.add(lfn)
      for lfn, metadata in result['Value']['Successful'].items():
        if metadata.get('Lost', False):
          error = "File has been Lost by the StorageElement %s" % seName
        elif metadata.get('Unavailable', False):
          error = "File is declared Unavailable by the StorageElement %s" % seName
        elif seName in tapeSEs and not metadata.get('Cached', metadata['Accessible']):
          error = "File is not online in StorageElement %s Cache" % seName
        elif not metadata.get('Accessible', True):
          error = "File is not accessible"
        else:
          error = ''
        if error:
          lfns.remove(lfn)
          self.log.error(error, lfn)
          # If PFN is not available
          # declared it failed and go on
          failedReps.add(lfn)

      if None in failedReps:
        failedReps.remove(None)
      if not failedReps:
        self.log.info('Preliminary checks OK, getting TURLS at %s for:\n%s' % (seName, '\n'.join(lfns)))
      else:
        self.log.warn("Errors during preliminary checks for %d files" % len(failedReps))

      result = StorageElement(seName).getURL(lfns, protocol=requestedProtocol)
      if not result['OK']:
        self.log.error("Error getting TURLs", result['Message'])
        return result

      badTURLCount = 0
      badTURLs = []
      seResult = result['Value']

      for lfn, cause in seResult['Failed'].items():
        badTURLCount += 1
        badTURLs.append('Failed to obtain TURL for %s: %s' % (lfn, cause))
        failedReps.add(lfn)

      if badTURLCount:
        self.log.warn('Found %s problematic TURL(s) for job %s' % (badTURLCount, self.jobID))
        param = '\n'.join(badTURLs)
        self.log.info(param)
        result = self.__setJobParam('ProblematicTURLs', param)
        if not result['OK']:
          self.log.warn("Error setting job param", result['Message'])

      failedReplicas.update(failedReps)
      for lfn, turl in seResult['Successful'].items():
        for track in trackLFNs[lfn]:
          if track['se'] == seName:
            track['turl'] = turl
            break
        self.log.info('Resolved input data\n>>>> SE: %s\n>>>>LFN: %s\n>>>>TURL: %s' %
                      (seName, lfn, turl))
      ##### End of loop on SE #######

    # Check if the files were actually resolved (i.e. have a TURL)
    # If so, remove them from failed list
    for lfn, mdataList in trackLFNs.items():  # There is a pop below, can't use iteritems()
      for mdata in list(mdataList):
        if 'turl' not in mdata:
          mdataList.remove(mdata)
          self.log.info('No TURL resolved for %s at %s' % (lfn, mdata['se']))
      if not mdataList:
        trackLFNs.pop(lfn, None)
        failedReplicas.add(lfn)
      elif lfn in failedReplicas:
        failedReplicas.remove(lfn)
    self.log.debug('All resolved data', sorted(trackLFNs))
    self.log.debug('All failed data', sorted(failedReplicas))

    return S_OK({'Successful': trackLFNs, 'Failed': sorted(failedReplicas)})

  #############################################################################
  def __setJobParam(self, name, value):
    """Wraps around setJobParameter of state update client
    """
    if not self.jobID:
      return S_ERROR('JobID not defined')

    self.log.verbose('setJobParameter(%s, %s, %s)' % (self.jobID, name, value))
    return JobStateUpdateClient().setJobParameter(int(self.jobID),
                                                  str(name),
                                                  str(value))
