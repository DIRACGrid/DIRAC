""" ResourcePolType Actions
"""

def ResourcePolTypeActions(granularity, name, resDecisions, res, rsDB, rmDB):
  # Update the DB

  if res['Action']:
    if granularity == 'Site':
      rsDB.setSiteStatus(name, res['Status'], res['Reason'], 'RS_SVC')
      rsDB.setMonitoredToBeChecked(['Service', 'Resource', 'StorageElement'], 'Site', name)

    elif granularity == 'Service':
      rsDB.setServiceStatus(name, res['Status'], res['Reason'], 'RS_SVC')
      rsDB.setMonitoredToBeChecked(['Site', 'Resource', 'StorageElement'], 'Service', name)

    elif granularity == 'Resource':
      rsDB.setResourceStatus(name, res['Status'], res['Reason'], 'RS_SVC')
      rsDB.setMonitoredToBeChecked(['Site', 'Service', 'StorageElement'], 'Resource', name)

    elif granularity == 'StorageElement':
      rsDB.setStorageElementStatus(name, res['Status'], res['Reason'], 'RS_SVC')
      rsDB.setMonitoredToBeChecked(['Site', 'Service', 'Resource'], 'StorageElement', name)

  else:
    rsDB.setMonitoredReason(granularity, name, res['Reason'], 'RS_SVC')

  rsDB.setLastMonitoredCheckTime(granularity, name)

  for resP in resDecisions['SinglePolicyResults']:
    if not resP.has_key('OLD'):
      rmDB.addOrModifyPolicyRes(granularity, name,
                                resP['PolicyName'], resP['Status'], resP['Reason'])

  if res.has_key('EndDate'):
    rsDB.setDateEnd(granularity, name, res['EndDate'])
