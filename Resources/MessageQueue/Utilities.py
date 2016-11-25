""" Utilities for the MessageQueue package
"""

from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.CSAPI       import CSAPI
from DIRAC.Core.Utilities  import ObjectLoader
from DIRAC.Core.Utilities.DErrno import EMQUKN

__RCSID__ = "$Id$"

def getSpecializedMQConnector(mqType):
  subClassName = mqType + 'MQConnector'
  objectLoader = ObjectLoader.ObjectLoader()
  result = objectLoader.loadObject( 'Resources.MessageQueue.%s' % subClassName, subClassName )
  if not result['OK']:
    gLogger.error( 'Failed to load object', '%s: %s' % ( subClassName, result['Message'] ) )
  return result

def createMQConnector(parameters = None):
  mqType = parameters.get('MQType', None)
  result = getSpecializedMQConnector(mqType = mqType)
  if not result['OK']:
    gLogger.error( 'Failed to getSpecializedMQConnector:', '%s' % (result['Message'] ) )
    return result
  ceClass = result['Value']
  try:
    mqConnector = ceClass(parameters)
    if not result['OK']:
      return result
  except Exception as exc:
    gLogger.exception( 'Could not instantiate MQConnector object',  lExcInfo = exc )
    return S_ERROR( EMQUKN, '' )
  return S_OK( mqConnector )


def getMQParamsFromCS( destinationName ):
  """ Get parameter of a MQ destination (queue/topic) from the CS

  :param str destinationName: name of the queue/topic either just the queue/topic name, in this case
                       the default MQServer will be used, or in th form <MQServer>::<destinationName>
  :return: S_OK( parameterDict )/ S_ERROR
  """

  # API initialization is required to get an up-to-date configuration from the CS
  csAPI = CSAPI()
  csAPI.initialize()

  mqService = ''
  elements = destinationName.split( '::' )
  if len( elements ) == 2:
    mqService, queue = elements
  else:
    queue = destinationName

  # get both queues and topics
  print "this is mqService:" + mqService
  result = gConfig.getConfigurationTree( '/Resources/MQServices', mqService, queue )
  if not result['OK'] or len( result['Value'] ) == 0:
    return S_ERROR( 'Requested MQService or queue/topic not found in the CS: %s::%s' % ( mqService, queue ) )

  queuePath = None
  for path, value in result['Value'].iteritems():

    # check section paths for duplicate names
    # endswith() guarantees that similar queue names are discarded
    if not value and path.endswith( queue ):
      if queuePath:
        return S_ERROR( 'Ambiguous queue/topic %s definition' % queue )
      else:
        queuePath = path

  # set-up internal parameter depending on the destination type
  tmp = queuePath.split( 'Queues' )[0].split( 'Topics' )
  servicePath = tmp[0]

  serviceDict = {}
  if len(tmp) > 1:
    serviceDict['Topic'] = queue
  else:
    serviceDict['Queue'] = queue

  result = gConfig.getOptionsDict( servicePath )
  if not result['OK']:
    return result
  serviceDict.update( result['Value'] )

  result = gConfig.getOptionsDict( queuePath )
  if not result['OK']:
    return result
  serviceDict.update( result['Value'] )

  return S_OK( serviceDict )

def getMQService(mqURI):
  return mqURI.split("::")[0]

def getDestinationType(mqURI):
  return mqURI.split("::")[1]

def getDestinationName(mqURI):
  return mqURI.split("::")[2]

def getDestinationAddress(mqURI):
  mqType, mqName = mqURI.split("::")[-2:]
  return "/" + mqType.lower() + "/" + mqName
