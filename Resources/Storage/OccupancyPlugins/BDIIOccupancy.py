import os

from DIRAC import gLogger, gConfig
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.Grid import ldapsearchBDII


class BDIIOccupancy(object):
  def __init__(self):
    self.host = 'lcg-bdii.cern.ch:2170'
    if 'LCG_GFAL_INFOSYS' in os.environ:
      self.host = os.environ['LCG_GFAL_INFOSYS']

  def getOccupancy(self, sePlugin):
    sTokenDict = {'Total': 0, 'Free': 0}
    bdiiAttr = ['GlueSATotalOnlineSize', 'GlueSAFreeOnlineSize']
    filt = "(&(GlueSAAccessControlBaseRule=VO:%s)" % sePlugin.voName
    filt += "(GlueChunkKey=GlueSEUniqueID=%s))" % sePlugin.protocolParameters['Host']
    ret = ldapsearchBDII(filt, bdiiAttr, host=self.host)
    if not ret['OK']:
      return ret
    if len(ret['Value']) > 0:
      if 'attr' in ret['Value'][0]:
        attr = ret['Value'][0]['attr']
        sTokenDict['Total'] = float(attr.get(bdiiAttr[0], 0)) * 1024 * 1024 * 1024
        sTokenDict['Free'] = float(attr.get(bdiiAttr[1], 0)) * 1024 * 1024 * 1024
    return S_OK(sTokenDict)
