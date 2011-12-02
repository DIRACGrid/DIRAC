class InfoGetter( object ):

  def __init__( self ):
    pass

  def getInfoToApply( self, args, granularity, statusType = None, status = None,
                      formerStatus = None, siteType = None, serviceType = None,
                      resourceType = None, useNewRes = False ):
    res = {}
    res['Policies'] = []
    res['PolicyType'] = []
    return res

  def getNewPolicyType( self, granularity, newStatus ):
    return []
