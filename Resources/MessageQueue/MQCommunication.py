from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Resources.MessageQueue.MQProducer import MQProducer
from DIRAC.Resources.MessageQueue.MQConnectionManager import MQConnectionManager
from DIRAC.Resources.MessageQueue.Utilities import getMQParamsFromCS

from DIRAC.Core.Utilities  import ObjectLoader
from DIRAC.Core.Utilities.DErrno import EMQUKN
from DIRAC.Resources.MessageQueue.MQConnector import MQConnector

connectionManager = MQConnectionManager()

#def createConsumer(destination):
  #result = setupConnection(destination)
  #if not result['OK']:
    #gLogger.error( 'Failed to createMQConnection:', '%s' % (result['Message'] ) )
    #return result
  #conn = result['Value']
  #return MQConsumer(conn = conn)

def createProducer(mqURI):
  result = setupConnection(mqURI = mqURI, messangerType = "producer")
  if not result['OK']:
    gLogger.error( 'Failed to createProducer:', '%s' % (result['Message'] ) )
    return result
  return MQProducer(mqManager = connectionManager, mqURI  = mqURI)


def setupConnection(mqURI, messangerType):
  result = getMQParamsFromCS(destinationName = mqURI)
  if not result['OK']:
    gLogger.error( 'Failed to setupConnection:', '%s' % (result['Message'] ) )
    return result
  params = result['Value']
  #mqService, destinationType, destinationName = mqURI.split( '::' )

  #mqType = params['MQType']
  #dest = params[destinationType]
  #conn = createMQConnector(mqType = mqType, parameters = params)
  ##conn.start()
  #(messangerId, conn) = connectionManager.addConnectionIfNotExist(connectionInfo={"connection":conn, "destination":dest} , mqServiceId = mqService)
  return S_OK(params)

def getSpecializedMQConnector( mqType):
  subClassName = mqType + 'MQConnector'
  objectLoader = ObjectLoader.ObjectLoader()
  result = objectLoader.loadObject( 'Resources.MessageQueue.%s' % subClassName, subClassName )
  if not result['OK']:
    gLogger.error( 'Failed to load object', '%s: %s' % ( subClassName, result['Message'] ) )
  return result

def createMQConnector(mqType,  parameters = None):
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

#Resources
#{
  #MQServices
  #{
    #mardirac3.in2p3.fr
    #{
      #MQType = Stomp
      #Host = mardirac3.in2p3.fr
      #Port = 9165
      #User = guest
      #Password = guest
      #Queues
      #{
        #TestQueue
        #{
          #Acknowledgement = True
        #}
      #}
      #Topics
      #{
        #TestTopic
        #{
          #Acknowledgement = True
        #}
      #}
    #}
  #}
#}
#def getMQParamsFromCS ( destination ):
  #return ('Fake', {'host':'127.0.0.1', 'destination':'/queue/testFakeQueue'})
