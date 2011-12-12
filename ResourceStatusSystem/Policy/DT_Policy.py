########################################################################
# $HeadURL:
########################################################################

""" The DT_Policy class is a policy class satisfied when a site is in downtime,
    or when a downtime is revoked
"""

__RCSID__ = "$Id: "

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

class DT_Policy(PolicyBase):

  def evaluate(self):
    """
    Evaluate policy on possible ongoing or scheduled downtimes.

    :returns:
        {
          'Status':Unknown|Active|Bad|Banned,
          'Reason':'DT:None'|'DT:OUTAGE|'DT:AT_RISK',
          'EndDate':datetime (if needed)
        }
    """
    status = super(DT_Policy, self).evaluate()

    if not status:
      return {'Status':'Error', 'Reason':'GOCDB request did not succeed'}

    if status == 'Unknown':
      return {'Status':'Unknown'}

    if status['DT'] == None:
      self.result['Status']  = 'Active'
      self.result['Reason']  = 'No DownTime announced'
      return self.result

    elif 'OUTAGE' in status['DT']:
      self.result['Status']  = 'Banned'

    elif 'WARNING' in status['DT']:
      self.result['Status']  = 'Bad'

    else:
      return {'Status':'Error', 'Reason':'GOCDB returned an unknown value for DT'}

    self.result['EndDate'] = status['EndDate']
    self.result['Reason'] = 'DownTime found: %s' % status['DT']
    return self.result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
