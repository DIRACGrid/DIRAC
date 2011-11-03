class PolicyCaller( object ):
  
  def __init__( self, commandCallerIn = None, **clients ):
    pass
  
  def policyInvocation( self, VOExtension, granularity = None, name = None, 
                        status = None, policy = None, args = None, pName = None, 
                        pModule = None, extraArgs = None, commandIn = None ):
    return { 'PolicyName' : '', 'Status': '', 'Reason' : '' }