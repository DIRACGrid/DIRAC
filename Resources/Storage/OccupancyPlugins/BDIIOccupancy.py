"""
  Defines the plugin to take storage space information given by BDII
"""

__RCSID__ = "$Id$"

import os

from DIRAC import gLogger, gConfig
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.Grid import ldapsearchBDII


class BDIIOccupancy(object):
  """ .. class:: BDIIOccupancy

  Occupancy plugin to return the space information given by BDII
  Assuming the protocol is SRM
  """
  def __init__(self, se):
    # flag to show initalization status of the plugin
    self.isUsable = False
    self.log = se.log.getSubLogger('BDIIOccupancy')
    # BDII host to query
    self.bdii = 'lcg-bdii.cern.ch:2170'
    if 'LCG_GFAL_INFOSYS' in os.environ:
      self.bdii = os.environ['LCG_GFAL_INFOSYS']
    self.vo = se.vo
    # assume given SE speaks SRM
    ret = se.getStorageParameters(protocol='srm')
    if not ret['OK']:
      self.log.error(ret['Message'])
      return
    if 'Host' not in ret['Value']:
      self.log.error('No Host is found from StorageParameters')
      return
    self.host = ret['Value']['Host']
    self.isUsable = True

  def getOccupancy(self):
    """ Returns the space information given by BDII
        Total and Free space are taken from GlueSATotalOnlineSize and GlueSAFreeOnlineSize, respectively.

        :returns: S_OK with dict (keys: Total, Free)
    """
    sTokenDict = {'Total': 0, 'Free': 0}
    BDIIAttr = ['GlueSATotalOnlineSize', 'GlueSAFreeOnlineSize']

    filt = "(&(GlueSAAccessControlBaseRule=VO:%s)(GlueChunkKey=GlueSEUniqueID=%s))" % (self.vo, self.host)
    ret = ldapsearchBDII(filt, BDIIAttr, host=self.bdii)
    if not ret['OK']:
      return ret
    for value in ret['Value']:
      if 'attr' in value:
        attr = value['attr']
        sTokenDict['Total'] = float(attr.get(BDIIAttr[0], 0)) * 1024 * 1024 * 1024
        sTokenDict['Free'] = float(attr.get(BDIIAttr[1], 0)) * 1024 * 1024 * 1024
    return S_OK(sTokenDict)
