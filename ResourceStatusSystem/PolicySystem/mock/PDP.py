class PDP( object ):

  def __init__( self, **clients ):
    pass

  def setup( self, granularity = None, name = None, statusType = None,
             status = None, formerStatus = None, reason = None, siteType = None,
             serviceType = None, resourceType = None, useNewRes = False ):
    pass

  def takeDecision(self, policyIn = None, argsIn = None, knownInfo = None ):
    return { 'PolicyCombinedResult' : {} }
