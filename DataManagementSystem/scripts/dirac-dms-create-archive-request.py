"""Create and put Requests to archive files.

List of operations:

#. Optionally replicate files to SourceSE
#. ArchiveFiles: Create a tarball from input files, upload tarball  to TarballSE
#. ReplicateAndRegister Tarball to TargetSE
#. Optionally: Add LFNs to an ArchiveSE
#. Optionally: Check for Tarball Migration
#. Remove all other replicas for these files, or remove all files
#. Remove original replica of Tarball

Will copy all the respective files and place them in to tarballs. Then the tarballs are migrated to
another storage element. Once the file is migrated to tape the original files will be
removed. Optionally the original files can be registered in a special archive SE, so that their
metadata is preserved.

"""
import os

from DIRAC import gLogger
from DIRAC.Core.Utilities import DEncode
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult
from DIRAC.Core.Base import Script

from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File

from DIRAC.DataManagementSystem.Client.RequestUtilities import BaseRequest

LOG = gLogger.getSubLogger('AddArchive')
__RCSID__ = '$Id$'
MAX_SIZE = 2 * 1024 * 1024 * 1024  # 2 GB
MAX_FILES = 2000


class CreateArchiveRequest(BaseRequest):
  """Create the request to archive files."""

  def __init__(self):
    """Constructor."""
    super(CreateArchiveRequest, self).__init__()
    options = [('A', 'ArchiveSE', 'SE for registering archive files at'),
               ('I', 'TarballSE', 'SE to initially upload tarball'),
               ('P', 'Path', 'LFN path to folder, all files in the folder will be archived'),
               ('N', 'Name', 'Name of the Tarball, if not given Path_Tars/Path_N.tar'
                'will be used to store tarballs'),
               ('L', 'List', 'File containing list of LFNs to archive, requires Name to be given'),
               ('', 'MaxFiles', 'Maximum number to put in one tarball: Default %d' % MAX_FILES),
               ('', 'MaxSize', 'Maximum number of Bytes to put in one tarball: Default %d' % MAX_SIZE),
               ]
    flags = [('C', 'ReplicateTarball', 'Replicate the tarball'),
             ('D', 'RemoveReplicas', 'Remove Replicas from non-ArchiveSE'),
             ('U', 'RemoveFiles', 'Remove Archived files completely'),
             ('', 'AllowReplication', 'Enable first replicating to SOURCE-SE'),
             ('', 'SourceOnly', 'Only treat files that are already at the SOURCE-SE'),
             ]
    self.setUsage()
    self.registerSwitchesAndParseCommandLine(options, flags)
    self.switches['MaxSize'] = int(self.switches.setdefault('MaxSize', MAX_SIZE))
    self.switches['MaxFiles'] = int(self.switches.setdefault('MaxFiles', MAX_FILES))
    self.getLFNList()
    self.getLFNMetadata()
    self.lfnChunks = []
    self.replicaSEs = []

  @staticmethod
  def setUsage():
    """Set flags and options."""
    Script.setUsageMessage('\n'.join([__doc__,
                                      'Usage:',
                                      ' %s [option|cfgfile] LFNs tarBallName' % Script.scriptName,
                                      ]))

  def checkSwitches(self):
    """Check the switches, set autoName if needed."""
    super(CreateArchiveRequest, self).checkSwitches()
    if not self.name and self.lfnFolderPath:
      self.switches['AutoName'] = os.path.join(os.path.dirname(self.lfnFolderPath),
                                               os.path.basename(self.lfnFolderPath) + '.tar')
      LOG.notice('Using %r for tarball' % self.switches.get('AutoName'))

    if self.switches.get('RemoveReplicas') and self.switches.get('ArchiveSE') is None:
      LOG.error("Use either 'RemoveReplicas' Does not work without 'ArchiveSE'")
      raise RuntimeError('Too many removal flags')

    if self.switches.get('RemoveReplicas') and self.switches.get('RemoveFiles'):
      LOG.error("Use either 'RemoveReplicas' or 'RemoveFiles', not both!")
      raise RuntimeError('Too many removal flags')

  def splitLFNsBySize(self):
    """Split LFNs into MAX_SIZE chunks of at most MAX_FILES length.

    :return: list of list of lfns
    """
    LOG.notice('Splitting files by Size')
    lfnChunk = []
    totalSize = 0
    for lfn, info in self.metaData['Successful'].iteritems():
      if (totalSize > self.switches['MaxSize'] or len(lfnChunk) >= self.switches['MaxFiles']):
        self.lfnChunks.append(lfnChunk)
        LOG.notice('Created Chunk of %s lfns with %s bytes' % (len(lfnChunk), totalSize))
        lfnChunk = []
        totalSize = 0
      lfnChunk.append(lfn)
      totalSize += info['Size']

    self.lfnChunks.append(lfnChunk)
    LOG.notice('Created Chunk of %s lfns with %s bytes' % (len(lfnChunk), totalSize))

    self.replicaSEs = set([seItem for se in self.fcClient.getReplicas(self.lfnList)['Value']['Successful'].values()
                           for seItem in se.keys()])

  def run(self):
    """Perform checks and create the request."""
    if self.switches.get('AutoName'):
      baseArchiveLFN = archiveLFN = self.switches['AutoName']
      tarballName = os.path.basename(archiveLFN)
    else:
      baseArchiveLFN = archiveLFN = self.name
      tarballName = os.path.basename(archiveLFN)
    baseRequestName = requestName = 'Archive_%s' % tarballName.rsplit('.', 1)[0]

    from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator

    self.splitLFNsBySize()

    for count, lfnChunk in enumerate(self.lfnChunks):
      if not lfnChunk:
        LOG.error('LFN list is empty!!!')
        return 1

      if len(self.lfnChunks) > 1:
        requestName = '%s_%d' % (baseRequestName, count)
        baseName = os.path.split(baseArchiveLFN.rsplit('.', 1)[0])
        archiveLFN = '%s/%s_Tars/%s_%d.tar' % (baseName[0], baseName[1], baseName[1], count)

      self.checkArchive(archiveLFN)

      request = self.createRequest(requestName, archiveLFN, lfnChunk)

      valid = RequestValidator().validate(request)
      if not valid['OK']:
        LOG.error('putRequest: request not valid', '%s' % valid['Message'])
        return 1
      else:
        self.requests.append(request)

    self.putOrRunRequests()
    return 0

  def createRequest(self, requestName, archiveLFN, lfnChunk):
    """Create the Request."""
    request = Request()
    request.RequestName = requestName

    self._checkReplicaSites(request, lfnChunk)

    archiveFiles = Operation()
    archiveFiles.Type = 'ArchiveFiles'
    archiveFiles.Arguments = DEncode.encode({'SourceSE': self.sourceSEs[0],
                                             'TarballSE': self.switches['TarballSE'],
                                             'ArchiveSE': self.switches.get('ArchiveSE'),
                                             'TargetSE': self.targetSE,
                                             'ArchiveLFN': archiveLFN})
    self.addLFNs(archiveFiles, lfnChunk)
    request.addOperation(archiveFiles)

    # Replicate the Tarball, ArchiveFiles will upload it
    if self.switches.get('ReplicateTarball'):
      replicateAndRegisterTarBall = Operation()
      replicateAndRegisterTarBall.Type = 'ReplicateAndRegister'
      replicateAndRegisterTarBall.TargetSE = self.targetSE
      opFile = File()
      opFile.LFN = archiveLFN
      replicateAndRegisterTarBall.addFile(opFile)
      request.addOperation(replicateAndRegisterTarBall)

      checkMigrationTarBall = Operation()
      checkMigrationTarBall.Type = 'CheckMigration'
      checkMigrationTarBall.TargetSE = self.targetSE
      opFile = File()
      opFile.LFN = archiveLFN
      checkMigrationTarBall.addFile(opFile)
      request.addOperation(checkMigrationTarBall)

    # Register Archive Replica for LFNs
    if self.switches.get('ArchiveSE'):
      registerArchived = Operation()
      registerArchived.Type = 'RegisterReplica'
      registerArchived.TargetSE = self.switches.get('ArchiveSE')
      self.addLFNs(registerArchived, lfnChunk, addPFN=True)
      request.addOperation(registerArchived)

      # Remove all Other Replicas for LFNs
      if self.switches.get('RemoveReplicas'):
        removeArchiveReplicas = Operation()
        removeArchiveReplicas.Type = 'RemoveReplica'
        removeArchiveReplicas.TargetSE = ','.join(self.replicaSEs)
        self.addLFNs(removeArchiveReplicas, lfnChunk)
        request.addOperation(removeArchiveReplicas)

    # Remove all Other Replicas for LFNs
    if self.switches.get('RemoveFiles'):
      removeArchiveFiles = Operation()
      removeArchiveFiles.Type = 'RemoveFile'
      self.addLFNs(removeArchiveFiles, lfnChunk)
      request.addOperation(removeArchiveFiles)

    # Remove Original tarball replica
    if self.switches.get('ReplicateTarball'):
      removeTarballOrg = Operation()
      removeTarballOrg.Type = 'RemoveReplica'
      removeTarballOrg.TargetSE = self.sourceSEs[0]
      opFile = File()
      opFile.LFN = archiveLFN
      removeTarballOrg.addFile(opFile)
      request.addOperation(removeTarballOrg)
    return request

  def checkArchive(self, archiveLFN):
    """Check that archiveLFN does not exist yet."""
    LOG.notice('Using Tarball: %s' % archiveLFN)
    exists = returnSingleResult(self.fcClient.isFile(archiveLFN))
    LOG.debug('Checking for Tarball existance %r' % exists)
    if exists['OK'] and exists['Value']:
      raise RuntimeError('Tarball %r already exists' % archiveLFN)

    LOG.debug('Checking permissions for %r' % archiveLFN)
    hasAccess = returnSingleResult(self.fcClient.hasAccess(archiveLFN, 'addFile'))
    if not archiveLFN or not hasAccess['OK'] or not hasAccess['Value']:
      LOG.error('Error checking tarball location: %r' % hasAccess)
      raise ValueError('%s is not a valid path, parameter "Name" must be correct' % archiveLFN)

  def _checkReplicaSites(self, request, lfnChunk):
    """Ensure that all lfns can be found at the SourceSE, otherwise add replication operation to request.

    If SourceOnly is set just rejetct those LFNs

    TODO: Abort if too many files are not at the source?
    """
    resReplica = self.fcClient.getReplicas(lfnChunk)
    if not resReplica['OK']:
      LOG.error('Failed to get replica information:', resReplica['Message'])
      raise RuntimeError('Failed to get replica information')

    atSource = []
    notAt = []
    failed = []
    sourceSE = self.sourceSEs[0]
    for lfn, replInfo in resReplica['Value']['Successful'].iteritems():
      if sourceSE in replInfo:
        atSource.append(lfn)
      else:
        LOG.notice('WARN: LFN %r not found at source, only at: %s' % (lfn, ','.join(replInfo.keys())))
        notAt.append(lfn)

    for lfn, errorMessage in resReplica['Value']['Failed'].iteritems():
      LOG.error('Failed to get replica info', '%s: %s' % (lfn, errorMessage))
      failed.append(lfn)

    if failed:
      raise RuntimeError('Failed to get replica information')
    LOG.notice('Found %d files to replicate' % len(notAt))
    if not notAt:
      return
    if notAt and self.switches.get('AllowReplication'):
      self._replicateSourceFiles(request, notAt)
    else:
      raise RuntimeError('Not all files are at the Source, exiting')

  def _replicateSourceFiles(self, request, lfns):
    """Create the replicateAndRegisterRequest.

    :param request: The request to add the operation to
    :param lfns: list of LFNs
    """
    registerSource = Operation()
    registerSource.Type = 'ReplicateAndRegister'
    registerSource.TargetSE = self.sourceSEs[0]
    self.addLFNs(registerSource, lfns, addPFN=True)
    request.addOperation(registerSource)


if __name__ == '__main__':
  CAR = CreateArchiveRequest()
  CAR.run()
