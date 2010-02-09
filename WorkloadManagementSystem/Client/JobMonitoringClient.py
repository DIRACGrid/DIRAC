""" Class that contains client access to the job monitoring handler. """
########################################################################
# $Id: JobMonitoringClient.py 20695 2010-01-29 10:19:50Z acsmith $
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/WorkloadManagementSystem/Client/JobMonitoringClient.py $
########################################################################
__RCSID__ = "$Id: JobMonitoringClient.py 20695 2010-01-29 10:19:50Z acsmith $"

from DIRAC.Core.Base.Client                         import Client
    
class JobMonitoringClient(Client):
 
  def __init__(self):
    self.setServer('WorkloadManagement/JobMonitoring')
