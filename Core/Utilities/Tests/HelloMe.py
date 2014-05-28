from DIRAC.Core.DISET.RPCClient import RPCClient

simpleMessageService = RPCClient('Framework/Hello')
result = simpleMessageService.sayHello( 'you' )
if not result['OK']:
  print "Error while calling the service:", result['Message'] #Here, in DIRAC, you better use the gLogger
else:
  print result[ 'Value' ] #Here, in DIRAC, you better use the gLogger
