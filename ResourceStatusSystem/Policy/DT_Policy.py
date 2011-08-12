########################################################################
# $HeadURL:
########################################################################

""" The DT_Policy class is a policy class satisfied when a site is in downtime, 
    or when a downtime is revoked
"""

__RCSID__ = "$Id: "

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

class DT_Policy( PolicyBase ):

  def evaluate( self ):
    """ 
    Evaluate policy on possible ongoing or scheduled downtimes. 
        
    :returns:
        { 
          'SAT':True|False, 
          'Status':Active|Probing|Bad|Banned, 
          'Reason':'DT:None'|'DT:OUTAGE|'DT:AT_RISK',
          'EndDate':datetime (if needed)
        }
    """

    status = super( DT_Policy, self ).evaluate()

    if status == 'Unknown':
      return {'SAT':'Unknown'}

    if self.oldStatus == 'Active':
      if status['DT'] == None:
        self.result['SAT'] = False
        self.result['Status'] = 'Active'
      else:
        if 'OUTAGE' in status['DT']:
          self.result['SAT'] = True
          self.result['Status'] = 'Banned'
          self.result['EndDate'] = status['EndDate']
        elif 'AT_RISK' in status['DT']:
          self.result['SAT'] = True
          self.result['Status'] = 'Probing'
          self.result['EndDate'] = status['EndDate']

    elif self.oldStatus == 'Probing':
      if status['DT'] == None:
        self.result['SAT'] = True
        self.result['Status'] = 'Active'
      else:
        if 'OUTAGE' in status['DT']:
          self.result['SAT'] = True
          self.result['Status'] = 'Banned'
          self.result['EndDate'] = status['EndDate']
        elif 'AT_RISK' in status['DT']:
          self.result['SAT'] = False
          self.result['Status'] = 'Probing'
          self.result['EndDate'] = status['EndDate']

    elif self.oldStatus == 'Bad':
      if status['DT'] == None:
        self.result['SAT'] = True
        self.result['Status'] = 'Active'
      else:
        if 'OUTAGE' in status['DT']:
          self.result['SAT'] = True
          self.result['Status'] = 'Banned'
          self.result['EndDate'] = status['EndDate']
        elif 'AT_RISK' in status['DT']:
          self.result['SAT'] = False
          self.result['Status'] = 'Bad'
          self.result['EndDate'] = status['EndDate']

    elif self.oldStatus == 'Banned':
      if status['DT'] == None:
        self.result['SAT'] = True
        self.result['Status'] = 'Active'
      else:
        if 'OUTAGE' in status['DT']:
          self.result['SAT'] = False
          self.result['Status'] = 'Banned'
          self.result['EndDate'] = status['EndDate']
        elif 'AT_RISK' in status['DT']:
          self.result['SAT'] = True
          self.result['Status'] = 'Probing'
          self.result['EndDate'] = status['EndDate']

    if status['DT'] == None:
      self.result['Reason'] = 'No DownTime announced'
    else:
      self.result['Reason'] = 'DownTime found: %s' % status['DT']

    return self.result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
