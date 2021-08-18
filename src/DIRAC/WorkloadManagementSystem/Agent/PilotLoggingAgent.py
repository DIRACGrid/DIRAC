""" :mod: PilotLoggingAgent

    PilotLoggingAgent sends Pilot log files to an SE
"""

# # imports
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Security.Locations import getHostCertificateAndKeyLocation, getCAsLocation
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.WorkloadManagementSystem.Service.TornadoPilotLoggingHandler import TornadoPilotLoggingHandler
import requests

__RCSID__ = "Id$"

class PilotLoggingAgent(AgentModule):
  """
  .. class:: PilotLoggingAgent

  The agent sends completed pilot log files to permanent storage for analysis.
  """

  def initialize(self):
    """
    agent's initalisation

    :param self: self reference
    """
    # Determine the VO for shifter
    # The PilotLoggingAgent might use a VO-specific shifter proxy.
    self.vo = self.am_getOption("VO", '')
    if not self.vo:
        self.vo = self.am_getOption("Community", '')
    if not self.vo:
        self.vo = CSGlobals.getVO()

    # The SiteDirector is for a particular user group
    #self.group = self.am_getOption("Group", '')

    # get shifter proxy for uploads
    self.am_setOption('shifterProxy', 'DataManager')
    self.uploadSE = self.am_getOption('UploadSE', 'UKI-LT2-IC-HEP-disk')

    self.message = self.am_getOption('Message', "PilotLoggingAgent initialised.")
    self.log.info("message = %s" % self.message)
    self.certAndKeyLocation = getHostCertificateAndKeyLocation()
    self.casLocation = getCAsLocation()

    data = {'method': 'getMetadata'}
    self.server = self.am_getOption("DownloadLocation", None)

    if not self.server:
        return S_ERROR("No DownloadLocation set in the CS !")
    try:
      with requests.post(self.server, data=data, verify=self.casLocation, cert=self.certAndKeyLocation) as res:
        if res.status_code not in (200, 202):
            message = "Could not get metadata from %s: status %s" % (self.server, res.status_code)
            self.log.error(message)
            return S_ERROR(message)
        resDict = res.json()
    except Exception as exc:
        message = "Call to server %s failed with %s " % (self.server, exc)
        self.log.error(message)
        return S_ERROR(message)
    if resDict['OK']:
        meta = resDict['Value']
        self.pilotLogPath = meta['LogPath']
    else:
        return S_ERROR(resDict['Message'])
    self.log.info("Pilot log files location = %s " % self.pilotLogPath)
    return S_OK()

  def execute(self):
    """ execution in one agent's cycle

    :param self: self reference
    """

    self.log.info("Pilot files upload cycle started.")
    files = [f for f in os.listdir(self.pilotLogPath)
             if os.path.isfile(os.path.join(self.pilotLogPath, f)) and f.endswith('log')]
    for elem in files:
      lfn = os.path.join('/gridpp/pilotlogs/', elem)
      name = os.path.join(self.pilotLogPath, elem)
      res = DataManager().putAndRegister(lfn=lfn, fileName=name, diracSE=self.uploadSE, overwrite=True)
      if not res['OK']:
        self.log.error("Could not upload", "to %s: %s" % (self.uploadSE, res['Message']))
      else:
        self.log.info("File uploaded: ", "LFN = %s" % res['Value'])
        try:
          os.remove(name)
        except Exception as excp:
          self.log.exception("Cannot remove a local file after uploading", lException=excp)
    return S_OK()
