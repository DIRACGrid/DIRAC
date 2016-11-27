""" Utilities for the MessageQueue package
"""

from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.CSAPI       import CSAPI
from DIRAC.Core.Utilities  import ObjectLoader
from DIRAC.Core.Utilities.DErrno import EMQUKN
import Queue

def getSpecializedMQConnector(mqType):
  """ Function loads the specialized MQConnector class based on mqType.
      It is assumed that MQConnector has a name in the format mqTypeMQConnector
      e.g. if StompMQConnector.
  Args:
    mqType(str): prefix of specialized class name e.g. Stomp.
  Returns:
    S_OK/S_ERROR: with loaded specialized class of MQConnector.
  """
  subClassName = mqType + 'MQConnector'
  objectLoader = ObjectLoader.ObjectLoader()
  result = objectLoader.loadObject( 'Resources.MessageQueue.%s' % subClassName, subClassName )
  if not result['OK']:
    gLogger.error( 'Failed to load object', '%s: %s' % ( subClassName, result['Message'] ) )
  return result

def createMQConnector(parameters = None):
  """ Function creates and returns the MQConnector object based.
  Args:
    parameters(dict): set of parameters for the MQConnector constructor,
      it should also contain pair 'MQType':mqType, where
      mqType is a string used as a prefix for the specialized MQConnector
      class.
  Returns:
    S_OK/S_ERROR: with loaded specialized class of MQConnector.
  """
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


def getMQParamsFromCS( mqURI ):
  """ Function gets parameters of a MQ destination (queue/topic) from the CS.
  Args:
    mqURI(str):Pseudo URI identifing the MQ service. It has the following format:
              mqConnection::DestinationType::DestinationName
              e.g. blabla.cern.ch::Queue::MyQueue1
    mType(str): 'consumer' or 'producer'
  Returns:
    S_OK(param_dicts)/S_ERROR: 
  """
  # API initialization is required to get an up-to-date configuration from the CS
  csAPI = CSAPI()
  csAPI.initialize()

  try :
    mqService, mqType, mqName = mqURI.split("::")
  except ValueError:
    return S_ERROR( 'Bad format of mqURI address:%s' % ( mqURI) )

  result = gConfig.getConfigurationTree( '/Resources/MQServices', mqService, mqType, mqName )
  if not result['OK'] or len( result['Value'] ) == 0:
    return S_ERROR( 'Requested destination not found in the CS: %s::%s::%s' % ( mqService, mqType, mqName ) )
  mqDestinationPath = None
  for path, value in result['Value'].iteritems():
    if not value and path.endswith( mqName ):
        mqDestinationPath = path

  # set-up internal parameter depending on the destination type
  tmp = mqDestinationPath.split( 'Queue' )[0].split( 'Topic' )
  servicePath = tmp[0]
  serviceDict = {}
  if len(tmp) > 1:
    serviceDict['Topic'] = mqName
  else:
    serviceDict['Queue'] = mqName

  result = gConfig.getOptionsDict(servicePath )
  if not result['OK']:
    return result
  serviceDict.update( result['Value'] )

  result = gConfig.getOptionsDict( mqDestinationPath )
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

def generateDefaultCallback():
  """ Function generates a default callback that can
      be used to handle the messages in the MQConsumer
      clients. It contains the internal queue (as closure)
      for the incoming messages. The queue can be accessed by the
      callback.get() method. The callback.get() method returns
      the first message or raise the exception Queue.Empty.
      e.g. myCallback = generateDefaultCallback()
          try:
             print myCallback.get()
          except Queue.Empty:
            pass
  Args:
    mqURI(str):Pseudo URI identifing MQ connection. It has the following format
              mqConnection::DestinationType::DestinationName
              e.g. blabla.cern.ch::Queue::MyQueue1
  Returns:
    object: callback function
  """
  msgQueue = Queue.Queue()
  def callback(headers, body):
    msgQueue.put(body)
  def get():
    return msgQueue.get(block = False)
  callback.get = get
  return callback
