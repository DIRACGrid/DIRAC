"""Module containing utilities to create Requests."""

import os

import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult
from DIRAC.RequestManagementSystem.Client.File import File

LOG = gLogger.getSubLogger(__name__)


class BaseRequest(object):
  """Base class for creating Request Creator scripts."""

  def __init__(self):
    """Initialise default switches etc."""
    self._fcClient = None
    self._reqClient = None
    self.switches = {}
    self.requests = []
    self.lfnList = []
    self.metaData = None

    self.options = [('S', 'SourceSE', 'Where to remove the LFNs from'),
                    ('T', 'TargetSE', 'Where to move the LFNs'),
                    ]
    self.flags = [('X', 'Execute', 'Put Requests, else dryrun'),
                  ('', 'RunLocal', 'Run Requests locally'),
                  ]

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
    """Return the lfn folder Path where to find the files of the Request."""
    return self.switches.get('Path', None)

  @property
  def dryRun(self):
    """Return dry run flag"""
    return self.switches['DryRun']

  def registerSwitchesAndParseCommandLine(self, options, flags):
    """Register the default plus additional parameters and parse options.

    :param list options: list of three tuple for options to add to the script
    :param list flags:  list of three tuple for flags to add to the script
    """
    self.options.extend(options)
    self.flags.extend(flags)
    for short, longOption, doc in self.options:
      Script.registerSwitch(short + ':' if short else '', longOption + '=', doc)
    for short, longOption, doc in self.flags:
      Script.registerSwitch(short, longOption, doc)
      self.switches[longOption] = False
    Script.parseCommandLine()
    if Script.getPositionalArgs():
      Script.showHelp()
      DIRAC.exit(1)

    for switch in Script.getUnprocessedSwitches():
      for short, longOption, doc in self.options:
        if switch[0] == short or switch[0].lower() == longOption.lower():
          LOG.debug('Found switch %r with value %r' % (longOption, switch[1]))
          self.switches[longOption] = switch[1]
          break
      for short, longOption, doc in self.flags:
        if switch[0] == short or switch[0].lower() == longOption.lower():
          self.switches[longOption] = True
          break

    self.checkSwitches()
    self.switches['DryRun'] = not self.switches.get('Execute', False)
    self.switches['SourceSE'] = self.switches.get('SourceSE', '').split(',')

  def checkSwitches(self):
    """Check consistency of given command line."""
    if not self.switches.get('SourceSE'):
      raise RuntimeError('Have to set "SourceSE"')
    if self.switches.get('List') and not self.name:
      raise RuntimeError('Have to set "Name" with "List"')
    if not self.switches.get('List') and not self.switches.get('Path'):
      raise RuntimeError('Have to set "List" or "Path"')

  def getLFNList(self):
    """Get list of LFNs.

    Either read the provided file, or get the files found beneath the provided folder.
    Also Set TarBall name if given folder and not given it already.

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
      LOG.debug('Check if %r is a directory' % path)
      isDir = returnSingleResult(self.fcClient.isDirectory(path))
      LOG.debug('Result: %r' % isDir)
      if not isDir['OK'] or not isDir['Value']:
        LOG.error('Path is not a directory', isDir.get('Message', ''))
        raise RuntimeError('Path %r is not a directory' % path)
      LOG.notice('Looking for files in %r' % path)

      metaDict = {'SE': self.sourceSEs[0]} if self.switches.get('SourceOnly') else {}
      lfns = self.fcClient.findFilesByMetadata(metaDict=metaDict, path=path)
      if not lfns['OK']:
        LOG.error('Could not find files')
        raise RuntimeError(lfns['Message'])
      self.lfnList = lfns['Value']

    if self.lfnList:
      LOG.notice('Will create request(s) with %d lfns' % len(self.lfnList))
      if len(self.lfnList) == 1:
        raise RuntimeError('Only 1 file in the list, aborting!')
      return

    raise ValueError('"Path" or "List" need to be provided!')

  def getLFNMetadata(self):
    """Get the metadata for all the LFNs."""
    metaData = self.fcClient.getFileMetadata(self.lfnList)
    error = False
    if not metaData['OK']:
      LOG.error('Unable to read metadata for lfns: %s' % metaData['Message'])
      raise RuntimeError('Could not read metadata: %s' % metaData['Message'])

    self.metaData = metaData['Value']
    for failedLFN, reason in self.metaData['Failed'].items():
      LOG.error('skipping %s: %s' % (failedLFN, reason))
      error = True
    if error:
      raise RuntimeError('Could not read all metadata')

    for lfn in self.metaData['Successful'].keys():
      LOG.verbose('found %s' % lfn)

  def putOrRunRequests(self):
    """Run or put requests."""
    handlerDict = {}
    handlerDict['ArchiveFiles'] = 'DIRAC.DataManagementSystem.Agent.RequestOperations.ArchiveFiles'
    handlerDict['CheckMigration'] = 'DIRAC.DataManagementSystem.Agent.RequestOperations.CheckMigration'
    handlerDict['ReplicateAndRegister'] = 'DIRAC.DataManagementSystem.Agent.RequestOperations.ReplicateAndRegister'
    handlerDict['RemoveFile'] = 'DIRAC.DataManagementSystem.Agent.RequestOperations.RemoveFile'
    handlerDict['RemoveReplica'] = 'DIRAC.DataManagementSystem.Agent.RequestOperations.RemoveReplica'
    requestIDs = []
    dryRun = self.switches['DryRun']
    runLocal = self.switches['RunLocal']

    if dryRun or runLocal:
      LOG.notice('Would have created %d requests' % len(self.requests))
      for reqID, req in enumerate(self.requests):
        LOG.notice('Request %d:' % reqID)
        for opID, op in enumerate(req):
          LOG.notice('        Operation %d: %s #lfn %d' % (opID, op.Type, len(op)))
      if not runLocal:
        return 0
      for request in self.requests:
        from DIRAC.RequestManagementSystem.private.RequestTask import RequestTask
        rq = RequestTask(request.toJSON()['Value'],
                         handlerDict,
                         '/Systems/RequestManagement/Development/Agents/RequestExecutingAgents',
                         'RequestManagement/RequestExecutingAgent', standalone=True)
        rq()
      return 0
    for request in self.requests:
      putRequest = self.reqClient.putRequest(request)
      if not putRequest['OK']:
        LOG.error('unable to put request %r: %s' % (request.RequestName, putRequest['Message']))
        continue
      requestIDs.append(str(putRequest['Value']))
      LOG.always('Request %r has been put to ReqDB for execution.' % request.RequestName)

    if requestIDs:
      LOG.always('%d requests have been put to ReqDB for execution' % len(requestIDs))
      LOG.always('RequestID(s): %s' % ' '.join(requestIDs))
      LOG.always('You can monitor the request status using the command: dirac-rms-request <requestName/ID>')
      return 0

    LOG.error('No requests created')
    return 1

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
