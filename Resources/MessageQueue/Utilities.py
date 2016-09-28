""" Utilities for the MessageQueue package
"""

from DIRAC import S_OK, S_ERROR, gConfig

__RCSID__ = "$Id$"

def getMQueue( queueName ):
  """ Get parameter of a MQ queue from the CS

  :param str queueName: name of the queue either just the queue name, in this case
                       the default MQServer will be used, or in th form <MQServer>::<queueName>
  :return: S_OK( parameterDict )/ S_ERROR
  """

  mqService = None
  elements = queueName.split( '::' )
  if len( elements ) == 2:
    mqService, queue = elements
  else:
    queue = queueName


  result = gConfig.getSections( '/Resources/MQServices' )
  if not result['OK']:
    return result
  sections = result['Value']
  if mqService and not mqService in sections:
    return S_ERROR( 'Requested MQService %s not found in the CS' % mqService )
  elif not mqService and len( sections ) == 1:
    mqService = sections[0]

  queuePath = ''
  servicePath = ''
  if mqService:
    servicePath = '/Resources/MQServices/%s' % mqService
    result = gConfig.getSections( '/Resources/MQServices/%s/Queues' % mqService )
    if result['OK'] and queue in result['Value']:
      queuePath = '/Resources/MQServices/%s/Queues/%s' % ( mqService, queue )
  else:
    for section in sections:
      result = gConfig.getSections( '/Resources/MQServices/%s/Queues' % section )
      if result['OK']:
        if queue in result['Value']:
          if queuePath:
            return S_ERROR( 'Ambiguous queue %s definition' % queue )
          else:
            servicePath = '/Resources/MQServices/%s' % section
            queuePath = '/Resources/MQServices/%s/Queues/%s' % ( section, queue )

  result = gConfig.getOptionsDict( servicePath )
  if not result['OK']:
    return result
  serviceDict = result['Value']

  if queuePath:
    result = gConfig.getOptionsDict( queuePath )
    if not result['OK']:
      return result
    serviceDict.update( result['Value'] )
  serviceDict['Queue'] = queue

  return S_OK( serviceDict )