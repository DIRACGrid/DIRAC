""" ResourcePolType Actions
"""

def ResourcePolTypeActions( granularity, name, resDecisions, res, rsDB, rmDB ):
  # Update the DB

  token = 'RS_SVC'

  if res['Action']:

    if res['Status'] == 'Probing':
      token = 'RS_Hold'

    if granularity == 'Site':
      # Sites are hammered once they are in the Probing state
      rsDB.setSiteStatus( name, res['Status'], res['Reason'], token )
      rsDB.setMonitoredToBeChecked( ['Service', 'Resource', 'StorageElement'], granularity, name )

    elif granularity == 'Service':
      rsDB.setServiceStatus( name, res['Status'], res['Reason'], 'RS_SVC' )
      rsDB.setMonitoredToBeChecked( ['Site', 'Resource', 'StorageElement'], granularity, name )

    elif granularity == 'Resource':
      rsDB.setResourceStatus( name, res['Status'], res['Reason'], 'RS_SVC' )
      rsDB.setMonitoredToBeChecked( ['Site', 'Service', 'StorageElement'], granularity, name )

    elif granularity == 'StorageElementRead':
      rsDB.setStorageElementStatus( name, res['Status'], res['Reason'], 'RS_SVC', 'Read' )
      rsDB.setMonitoredToBeChecked( ['Site', 'Service', 'Resource'], granularity, name )
    
    elif granularity == 'StorageElementWrite':
      rsDB.setStorageElementStatus( name, res['Status'], res['Reason'], 'RS_SVC', 'Write' )
      rsDB.setMonitoredToBeChecked( ['Site', 'Service', 'Resource'], granularity, name )

  else:
    rsDB.setMonitoredReason( granularity, name, res['Reason'], 'RS_SVC' )

  rsDB.setLastMonitoredCheckTime( granularity, name )

  for resP in resDecisions['SinglePolicyResults']:
    if not resP.has_key( 'OLD' ):
      rmDB.addOrModifyPolicyRes( granularity, name,
                                 resP['PolicyName'], resP['Status'], resP['Reason'] )

  if res.has_key( 'EndDate' ):
    rsDB.setDateEnd( granularity, name, res['EndDate'] )
