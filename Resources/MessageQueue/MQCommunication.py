from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Resources.MessageQueue.MQProducer import MQProducer
from DIRAC.Resources.MessageQueue.MQConnectionManager import MQConnectionManager
from DIRAC.Resources.MessageQueue.Utilities import getMQParamsFromCS


connectionManager = MQConnectionManager()

#def createConsumer(destination):
  #result = setupConnection(destination)
  #if not result['OK']:
    #gLogger.error( 'Failed to createMQConnection:', '%s' % (result['Message'] ) )
    #return result
  #conn = result['Value']
  #return MQConsumer(conn = conn)

def createProducer(mqURI):
  #todo change it to producer
  result = setupConnection(mqURI = mqURI, messangerType = "producers")
  print mqURI
  print connectionManager._connectionStorage
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
  print params
  return connectionManager.addOrUpdateConnection(mqURI, params, messangerType)



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
