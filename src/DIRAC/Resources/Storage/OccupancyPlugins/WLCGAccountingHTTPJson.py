"""
Defines the plugin to take storage space information given by WLCG Accounting Json
https://twiki.cern.ch/twiki/bin/view/LCG/AccountingTaskForce#Storage_Space_Accounting
https://twiki.cern.ch/twiki/pub/LCG/AccountingTaskForce/storage_service_v4.txt
https://docs.google.com/document/d/1yzCvKpxsbcQC5K9MyvXc-vBF1HGPBk4vhjw3MEXoXf8

When this is used, the OccupancyLFN has to be the full HTTP(s) URL

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import requests

from DIRAC.Core.Security.Locations import getCAsLocation
from DIRAC.Resources.Storage.OccupancyPlugins.WLCGAccountingJson import WLCGAccountingJson


class WLCGAccountingHTTPJson(WLCGAccountingJson):
  """ .. class:: WLCGAccountingHTTPJson

  Occupancy plugin to return the space information given by WLCG HTTP Accounting Json

  """

  def __init__(self, se):
    """
        c'tor

        :param se: reference to the StorageElement object from which we are called
    """

    super(WLCGAccountingHTTPJson, self).__init__(se)

    self.log = se.log.getSubLogger('WLCGAccountingHTTPJson')

  def _downloadJsonFile(self, occupancyLFN, filePath):
    """ Download the json file at the location using requests

        :param occupancyLFN: this is actually a full https URL
        :param filePath: destination path for the file

    """

    try:
      with open(filePath, 'wt') as fd:
        caPath = getCAsLocation()
        res = requests.get(occupancyLFN, verify=caPath)
        res.raise_for_status()
        fd.write(res.content)
    except Exception as e:
      self.log.debug("Exception while copying", repr(e))
