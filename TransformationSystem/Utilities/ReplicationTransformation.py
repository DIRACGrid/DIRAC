"""
Utilities to create replication transformations
"""

from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC import gLogger, S_OK, S_ERROR


def createDataTransformation(transformationType, targetSE, sourceSE,
                             metaKey, metaValue,
                             extraData=None, extraname='',
                             groupSize=1,
                             plugin='Broadcast',
                             enable=False,
                             ):
  """Creates the replication transformation based on the given parameters

  :param str transformationType: Type of replication to create
  :param targetSE: Destination for files
  :type targetSE: python:list or str
  :param str sourceSE: Origin of files.
  :param int metaKey: Meta key to identify input files
  :param int metaValue: Meta value to identify input files
  :param dict metaData: Additional meta data to use to identify input files
  :param str extraname: addition to the transformation name, only needed if the same transformation was already created
  :param int groupSize: number of files per transformation taks
  :param str plugin: plugin to use
  :param bool enable: if true submit the transformation, otherwise dry run
  :returns: S_OK, S_ERROR
  """

  metadata = {metaKey: metaValue}
  if isinstance(extraData, dict):
    metadata.update(extraData)

  gLogger.debug("Using %r for metadata search" % metadata)

  if isinstance(targetSE, basestring):
    targetSE = [targetSE]

  if transformationType not in ('Replication', 'Moving'):
    return S_ERROR('Unsupported transformationType %s' % transformationType)

  transType = {'Replication': 'Replicate', 'Moving': 'Move'}[transformationType]
  transGroup = {'Replication': 'Replication', 'Moving': 'Moving'}[transformationType]

  trans = Transformation()
  transName = '%s_%s_%s' % (transType, str(metaValue), ",".join(targetSE))
  if extraname:
    transName += "_%s" % extraname

  trans.setTransformationName(transName)
  description = '%s files for %s %s to %s' % (transType, metaKey, str(metaValue), ",".join(targetSE))
  trans.setDescription(description)
  trans.setLongDescription(description)
  trans.setType('Replication')
  trans.setGroup(transGroup)
  trans.setGroupSize(groupSize)
  trans.setPlugin(plugin)

  transBody = {'Moving': [("ReplicateAndRegister", {"SourceSE": sourceSE, "TargetSE": targetSE}),
                          ("RemoveReplica", {"TargetSE": sourceSE})],
               'Replication': ''  # empty body
               }[transformationType]

  trans.setBody(transBody)

  if sourceSE:
    res = trans.setSourceSE(sourceSE)
    if not res['OK']:
      return S_ERROR("SourceSE not valid: %s" % res['Message'])
  res = trans.setTargetSE(targetSE)
  if not res['OK']:
    return S_ERROR("TargetSE not valid: %s" % res['Message'])

  if not enable:
    gLogger.always("Dry run, not creating transformation")
    return S_OK()

  res = trans.addTransformation()
  if not res['OK']:
    return res
  gLogger.verbose(res)
  trans.setStatus('Active')
  trans.setAgentType('Automatic')
  currtrans = trans.getTransformationID()['Value']
  client = TransformationClient()
  res = client.createTransformationInputDataQuery(currtrans, metadata)
  if not res['OK']:
    return res

  gLogger.always("Successfully created replication transformation")
  return S_OK()
