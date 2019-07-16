"""Create and put Requests to move files

List of operations:

#. ReplicateAndRegister LFNs
#. Check for Migration
#. Remove all other replicas for these files


"""

from DIRAC import gLogger
from DIRAC.Core.Utilities.List import breakListIntoChunks

from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation

from DIRAC.DataManagementSystem.Client.RequestUtilities import BaseRequest


LOG = gLogger.getSubLogger('CreateMoving')
__RCSID__ = '$Id$'


class CreateMovingRequest(BaseRequest):
  """Create the request to archive files."""

  def __init__(self):
    """Constructor."""
    super(CreateMovingRequest, self).__init__()
    self.registerSwitchesAndParseCommandLine(options=[], flags=[])
    self.getLFNList()
    self.getLFNMetadata()

  def run(self):
    """Perform checks and create the request."""

    from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator
    for count, lfnChunk in enumerate(breakListIntoChunks(self.lfnList, 20)):
      if not lfnChunk:
        LOG.error('LFN list is empty!!!')
        return 1

      requestName = 'Moving_%s_%d' % (self.switches.get('Name'), count)
      request = self.createRequest(requestName, lfnChunk)
      valid = RequestValidator().validate(request)
      if not valid['OK']:
        LOG.error('putRequest: request not valid', '%s' % valid['Message'])
        return 1
      else:
        self.requests.append(request)

    self.putOrRunRequests()
    return 0

  def createRequest(self, requestName, lfnChunk):
    """Create the Request."""
    request = Request()
    request.RequestName = requestName

    replicate = Operation()
    replicate.Type = 'ReplicateAndRegister'
    replicate.TargetSE = self.switches.get('TargetSE')
    self.addLFNs(replicate, lfnChunk, addPFN=True)
    request.addOperation(replicate)

    checkMigration = Operation()
    checkMigration.Type = 'CheckMigration'
    checkMigration.TargetSE = self.switches.get('TargetSE')
    self.addLFNs(checkMigration, lfnChunk, addPFN=True)
    request.addOperation(checkMigration)

    removeReplicas = Operation()
    removeReplicas.Type = 'RemoveReplica'
    removeReplicas.TargetSE = ','.join(self.switches.get('SourceSE', []))
    self.addLFNs(removeReplicas, lfnChunk)
    request.addOperation(removeReplicas)

    return request


if __name__ == '__main__':
  CAR = CreateMovingRequest()
  CAR.run()
