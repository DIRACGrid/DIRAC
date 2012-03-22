""" Class that contains client access to the job monitoring handler. """
########################################################################
# $Id$
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client                         import Client
    
class JobMonitoringClient(Client):
 
  def __init__(self):
    self.setServer('WorkloadManagement/JobMonitoring')
