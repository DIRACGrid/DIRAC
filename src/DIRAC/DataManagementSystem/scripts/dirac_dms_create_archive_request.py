"""
Create and put Requests to archive files.

**List of operations**

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

**Related Options**

This script only works if the ``ArchiveFiles`` and ``CheckMigration`` RequestHandlers are configured.
To prevent submission of broken requests the script needs to be enabled in the Operations section of the CS

* Operations/DataManagement/ArchiveFiles/Enabled=True

Default values for any of the command line options can also be set in the CS

* Operations/DataManagement/ArchiveFiles/ArchiveSE
* Operations/DataManagement/ArchiveFiles/TarballSE
* Operations/DataManagement/ArchiveFiles/SourceSE
* Operations/DataManagement/ArchiveFiles/MaxFiles
* ...
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import os

import DIRAC
from DIRAC import gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities import DEncode
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.FrameworkSystem.private.standardLogging.LogLevels import LogLevels
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation

sLog = gLogger.getSubLogger('AddArchive')
__RCSID__ = '$Id$'
MAX_SIZE = 2 * 1024 * 1024 * 1024  # 2 GB
MAX_FILES = 2000


class CreateArchiveRequest(object):
  """Create the request to archive files."""

  def __init__(self):
    """Constructor."""
    self._fcClient = None
    self._reqClient = None
    self.switches = {}
    self.requests = []
    self.lfnList = []
    self.metaData = None
    self.options = [('A', 'ArchiveSE', 'SE for registering archive files at'),
                    ('I', 'TarballSE', 'SE to initially upload tarball'),
                    ('P', 'Path', 'LFN path to folder, all files in the folder will be archived'),
                    ('N', 'Name', 'Name of the Tarball, if not given: Path_Tars/Path_N.tar'
                     ' will be used to store tarballs'),
                    ('L', 'List', 'File containing list of LFNs to archive, requires Name to be given'),
                    ('', 'MaxFiles', 'Maximum number to put in one tarball: Default %d' % MAX_FILES),
                    ('', 'MaxSize', 'Maximum number of Bytes to put in one tarball: Default %d' % MAX_SIZE),
                    ('S', 'SourceSE', 'Where to remove the LFNs from'),
                    ('T', 'TargetSE', 'Where to move the Tarball to'),
                    ]
    self.flags = [('M', 'ReplicateTarball', 'Replicate the tarball'),
                  ('C', 'CheckMigration',
                   'Ensure the tarball is migrated to tape before removing any files or replicas'),
                  ('D', 'RemoveReplicas', 'Remove Replicas from non-ArchiveSE'),
                  ('U', 'RemoveFiles', 'Remove Archived files completely'),
                  ('R', 'RegisterDescendent', 'Register the Tarball as a descendent of the archived LFNs'),
                  ('', 'AllowReplication', 'Enable first replicating to Source-SE'),
                  ('', 'SourceOnly', 'Only treat files that are already at the Source-SE'),
                  ('X', 'Execute', 'Put Requests, else dryrun'),
                  ]
    self.registerSwitchesAndParseCommandLine()

    self.switches['MaxSize'] = int(self.switches.setdefault('MaxSize', MAX_SIZE))
    self.switches['MaxFiles'] = int(self.switches.setdefault('MaxFiles', MAX_FILES))

    self.getLFNList()
    self.getLFNMetadata()
    self.lfnChunks = []
    self.replicaSEs = []

  @property
  def fcClient(self):
    """Return FileCatalogClient."""
    if not self._fcClient:
      from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
      self._fcClient = FileCatalog()
    return self._fcClient

  @property
  def reqClient(self):
    """Return RequestClient."""
    if not self._reqClient:
      from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
      self._reqClient = ReqClient()
    return self._reqClient

  @property
  def dryRun(self):
    """Return dry run flag."""
    return self.switches['DryRun']

  @property
  def targetSE(self):
    """Return the list of targetSE."""
    return self.switches['TargetSE']

  @property
  def sourceSEs(self):
    """Return the list of sourceSEs."""
    return self.switches['SourceSE']

  @property
  def name(self):
    """Return the name of the Request."""
    return self.switches.get('Name', None)

  @property
  def lfnFolderPath(self):
    """Return the lfn folder path where to find the files of the request."""
    return self.switches.get('Path', None)

  def registerSwitchesAndParseCommandLine(self):
    """Register the default plus additional parameters and parse options.

    :param list options: list of three tuple for options to add to the script
    :param list flags:  list of three tuple for flags to add to the script
    :param str opName
    """
    for short, longOption, doc in self.options:
      Script.registerSwitch(short + ':' if short else '', longOption + '=', doc)
    for short, longOption, doc in self.flags:
      Script.registerSwitch(short, longOption, doc)
      self.switches[longOption] = False
    Script.parseCommandLine()
    if Script.getPositionalArgs():
      Script.showHelp(exitCode=1)

    ops = Operations()
    if not ops.getValue('DataManagement/ArchiveFiles/Enabled', False):
      sLog.error('The "ArchiveFiles" operation is not enabled, contact your administrator!')
      DIRAC.exit(1)
    for _short, longOption, _doc in self.options:
      defaultValue = ops.getValue('DataManagement/ArchiveFiles/%s' % longOption, None)
      if defaultValue:
        sLog.verbose('Found default value in the CS for %r with value %r' % (longOption, defaultValue))
        self.switches[longOption] = defaultValue
    for _short, longOption, _doc in self.flags:
      defaultValue = ops.getValue('DataManagement/ArchiveFiles/%s' % longOption, False)
      if defaultValue:
        sLog.verbose('Found default value in the CS for %r with value %r' % (longOption, defaultValue))
        self.switches[longOption] = defaultValue

    for switch in Script.getUnprocessedSwitches():
      for short, longOption, doc in self.options:
        if switch[0] == short or switch[0].lower() == longOption.lower():
          sLog.verbose('Found switch %r with value %r' % (longOption, switch[1]))
          self.switches[longOption] = switch[1]
          break
      for short, longOption, doc in self.flags:
        if switch[0] == short or switch[0].lower() == longOption.lower():
          self.switches[longOption] = True
          break

    self.checkSwitches()
    self.switches['DryRun'] = not self.switches.get('Execute', False)
    self.switches['SourceSE'] = self.switches.get('SourceSE', '').split(',')

  def getLFNList(self):
    """Get list of LFNs.

    Either read the provided file, or get the files found beneath the provided folder.

    :param dict switches: options from command line
    :returns: list of lfns
    :raises: RuntimeError, ValueError
    """
    if self.switches.get('List'):
      if os.path.exists(self.switches.get('List')):
        self.lfnList = list(set([line.split()[0]
                                 for line in open(self.switches.get('List')).read().splitlines()]))
      else:
        raise ValueError('%s not a file' % self.switches.get('List'))
    elif self.lfnFolderPath:
      path = self.lfnFolderPath
      sLog.debug('Check if %r is a directory' % path)
      isDir = returnSingleResult(self.fcClient.isDirectory(path))
      sLog.debug('Result: %r' % isDir)
      if not isDir['OK'] or not isDir['Value']:
        sLog.error('Path is not a directory', isDir.get('Message', ''))
        raise RuntimeError('Path %r is not a directory' % path)
      sLog.notice('Looking for files in %r' % path)

      metaDict = {'SE': self.sourceSEs[0]} if self.switches.get('SourceOnly') else {}
      lfns = self.fcClient.findFilesByMetadata(metaDict=metaDict, path=path)
      if not lfns['OK']:
        sLog.error('Could not find files')
        raise RuntimeError(lfns['Message'])
      self.lfnList = lfns['Value']

    if self.lfnList:
      sLog.notice('Will create request(s) with %d lfns' % len(self.lfnList))
      if len(self.lfnList) == 1:
        raise RuntimeError('Only 1 file in the list, aborting!')
      return

    raise ValueError('"Path" or "List" need to be provided!')

  def putOrRunRequests(self):
    """Run or put requests."""
    requestIDs = []

    if self.dryRun:
      sLog.notice('Would have created %d requests' % len(self.requests))
      for reqID, req in enumerate(self.requests):
        sLog.notice('Request %d:' % reqID)
        for opID, op in enumerate(req):
          sLog.notice('        Operation %d: %s #lfn %d' % (opID, op.Type, len(op)))
      return 0
    for request in self.requests:
      putRequest = self.reqClient.putRequest(request)
      if not putRequest['OK']:
        sLog.error('unable to put request %r: %s' % (request.RequestName, putRequest['Message']))
        continue
      requestIDs.append(str(putRequest['Value']))
      sLog.always('Request %r has been put to ReqDB for execution.' % request.RequestName)

    if requestIDs:
      sLog.always('%d requests have been put to ReqDB for execution' % len(requestIDs))
      sLog.always('RequestID(s): %s' % ' '.join(requestIDs))
      sLog.always('You can monitor the request status using the command: dirac-rms-request <requestName/ID>')
      return 0

    sLog.error('No requests created')
    return 1

  def checkSwitches(self):
    """Check the switches, set autoName if needed."""
    if not self.switches.get('SourceSE'):
      raise RuntimeError('Have to set "SourceSE"')
    if not self.switches.get('List') and not self.switches.get('Path'):
      raise RuntimeError('Have to set "List" or "Path"')
    if not self.name and self.lfnFolderPath:
      self.switches['AutoName'] = os.path.join(os.path.dirname(self.lfnFolderPath),
                                               os.path.basename(self.lfnFolderPath) + '.tar')
      sLog.notice('Using %r for tarball' % self.switches.get('AutoName'))

    if self.switches.get('List') and not self.name:
      raise RuntimeError('Have to set "Name" with "List"')

    if self.switches.get('RemoveReplicas') and self.switches.get('ArchiveSE') is None:
      sLog.error("'RemoveReplicas' does not work without 'ArchiveSE'")
      raise RuntimeError('ArchiveSE missing')

    if self.switches.get('RemoveReplicas') and self.switches.get('RemoveFiles'):
      sLog.error("Use either 'RemoveReplicas' or 'RemoveFiles', not both!")
      raise RuntimeError('Too many removal flags')

    if self.switches.get('ReplicateTarball') and not self.switches.get('TargetSE'):
      sLog.error("Have to set 'TargetSE' with 'ReplicateTarball'")
      raise RuntimeError('ReplicateTarball missing TargetSE')

  def splitLFNsBySize(self):
    """Split LFNs into MAX_SIZE chunks of at most MAX_FILES length.

    :return: list of list of lfns
    """
    sLog.notice('Splitting files by Size')
    lfnChunk = []
    totalSize = 0
    for lfn, info in self.metaData['Successful'].items():
      if (totalSize > self.switches['MaxSize'] or len(lfnChunk) >= self.switches['MaxFiles']):
        self.lfnChunks.append(lfnChunk)
        sLog.notice('Created Chunk of %s lfns with %s bytes' % (len(lfnChunk), totalSize))
        lfnChunk = []
        totalSize = 0
      lfnChunk.append(lfn)
      totalSize += info['Size']

    self.lfnChunks.append(lfnChunk)
    sLog.notice('Created Chunk of %s lfns with %s bytes' % (len(lfnChunk), totalSize))

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
        sLog.error('LFN list is empty!!!')
        return 1

      if len(self.lfnChunks) > 1:
        requestName = '%s_%d' % (baseRequestName, count)
        baseName = os.path.split(baseArchiveLFN.rsplit('.', 1)[0])
        archiveLFN = '%s/%s_Tars/%s_%d.tar' % (baseName[0], baseName[1], baseName[1], count)

      self.checkArchive(archiveLFN)

      request = self.createRequest(requestName, archiveLFN, lfnChunk)

      valid = RequestValidator().validate(request)
      if not valid['OK']:
        sLog.error('putRequest: request not valid', '%s' % valid['Message'])
        return 1
      else:
        self.requests.append(request)

    self.putOrRunRequests()
    return 0

  def addLFNs(self, operation, lfns, addPFN=False):
    """Add lfns to operation.

    :param operation: the operation instance to which the files will be added
    :param list lfns: list of lfns
    :param bool addPFN: if true adds PFN to each File
    """
    if not self.metaData:
      self.getLFNMetadata()

    for lfn in lfns:
      metaDict = self.metaData['Successful'][lfn]
      opFile = File()
      opFile.LFN = lfn
      if addPFN:
        opFile.PFN = lfn
      opFile.Size = metaDict['Size']
      if 'Checksum' in metaDict:
        # should check checksum type, now assuming Adler32 (metaDict['ChecksumType'] = 'AD')
        opFile.Checksum = metaDict['Checksum']
        opFile.ChecksumType = 'ADLER32'
      operation.addFile(opFile)

  def getLFNMetadata(self):
    """Get the metadata for all the LFNs."""
    metaData = self.fcClient.getFileMetadata(self.lfnList)
    error = False
    if not metaData['OK']:
      sLog.error('Unable to read metadata for lfns: %s' % metaData['Message'])
      raise RuntimeError('Could not read metadata: %s' % metaData['Message'])

    self.metaData = metaData['Value']
    for failedLFN, reason in self.metaData['Failed'].items():
      sLog.error('skipping %s: %s' % (failedLFN, reason))
      error = True
    if error:
      raise RuntimeError('Could not read all metadata')

    for lfn in self.metaData['Successful'].keys():
      sLog.verbose('found %s' % lfn)

  def createRequest(self, requestName, archiveLFN, lfnChunk):
    """Create the Request."""
    request = Request()
    request.RequestName = requestName

    self._checkReplicaSites(request, lfnChunk)

    archiveFiles = Operation()
    archiveFiles.Type = 'ArchiveFiles'
    archiveFiles.Arguments = DEncode.encode({'SourceSE': self.sourceSEs[0],
                                             'TarballSE': self.switches['TarballSE'],
                                             'RegisterDescendent': self.switches['RegisterDescendent'],
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

    if self.switches.get('CheckMigration'):
      checkMigrationTarBall = Operation()
      checkMigrationTarBall.Type = 'CheckMigration'
      migrationTarget = self.targetSE if self.switches.get('ReplicateTarball') else self.switches['TarballSE']
      checkMigrationTarBall.TargetSE = migrationTarget
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

    # Remove all Replicas for LFNs
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
    sLog.notice('Using Tarball: %s' % archiveLFN)
    exists = returnSingleResult(self.fcClient.isFile(archiveLFN))
    sLog.debug('Checking for Tarball existence %r' % exists)
    if exists['OK'] and exists['Value']:
      raise RuntimeError('Tarball %r already exists' % archiveLFN)

    sLog.debug('Checking permissions for %r' % archiveLFN)
    hasAccess = returnSingleResult(self.fcClient.hasAccess(archiveLFN, 'addFile'))
    if not archiveLFN or not hasAccess['OK'] or not hasAccess['Value']:
      sLog.error('Error checking tarball location: %r' % hasAccess)
      raise ValueError('%s is not a valid path, parameter "Name" must be correct' % archiveLFN)

  def _checkReplicaSites(self, request, lfnChunk):
    """Ensure that all lfns can be found at the SourceSE, otherwise add replication operation to request.

    If SourceOnly is set just rejetct those LFNs.

    """
    resReplica = self.fcClient.getReplicas(lfnChunk)
    if not resReplica['OK']:
      sLog.error('Failed to get replica information:', resReplica['Message'])
      raise RuntimeError('Failed to get replica information')

    atSource = []
    notAt = []
    failed = []
    sourceSE = self.sourceSEs[0]
    for lfn, replInfo in resReplica['Value']['Successful'].items():
      if sourceSE in replInfo:
        atSource.append(lfn)
      else:
        sLog.notice('WARN: LFN %r not found at source, only at: %s' % (lfn, ','.join(replInfo.keys())))
        notAt.append(lfn)

    for lfn, errorMessage in resReplica['Value']['Failed'].items():
      sLog.error('Failed to get replica info', '%s: %s' % (lfn, errorMessage))
      failed.append(lfn)

    if failed:
      raise RuntimeError('Failed to get replica information')
    sLog.notice('Found %d files to replicate' % len(notAt))
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


@DIRACScript()
def main():
  try:
    CAR = CreateArchiveRequest()
    CAR.run()
  except Exception as e:
    if LogLevels.getLevelValue(sLog.getLevel()) <= LogLevels.VERBOSE:
      sLog.exception('Failed to create Archive Request')
    else:
      sLog.error('ERROR: Failed to create Archive Request:', str(e))
    exit(1)
  exit(0)


if __name__ == "__main__":
  main()
