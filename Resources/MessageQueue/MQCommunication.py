from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Resources.MessageQueue.MQProducer import MQProducer
from DIRAC.Resources.MessageQueue.MQConsumer import MQConsumer
from DIRAC.Resources.MessageQueue.MQConnectionManager import MQConnectionManager
from DIRAC.Resources.MessageQueue.Utilities import getMQParamsFromCS


connectionManager = MQConnectionManager()

def createConsumer(mqURI, callback = None):
  result = setupConnection(mqURI = mqURI, messangerType = "consumers")
  if not result['OK']:
    gLogger.error( 'Failed to createConsumer:', '%s' % (result['Message'] ) )
    return result
  return MQConsumer(mqManager = connectionManager, mqURI  = mqURI, consumerId = result['Value'], callback = callback)

def createProducer(mqURI):
  result = setupConnection(mqURI = mqURI, messangerType = "producers")
  if not result['OK']:
    gLogger.error( 'Failed to createProducer:', '%s' % (result['Message'] ) )
    return result
  return MQProducer(mqManager = connectionManager, mqURI  = mqURI, producerId = result['Value'])


def setupConnection(mqURI, messangerType):
  result = getMQParamsFromCS(destinationName = mqURI)
  if not result['OK']:
    gLogger.error( 'Failed to setupConnection:', '%s' % (result['Message'] ) )
    return result
  params = result['Value']
  return connectionManager.setupConnection(mqURI, params, messangerType)



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
