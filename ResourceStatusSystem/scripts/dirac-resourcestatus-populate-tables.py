#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from datetime import datetime
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC import gConfig
from DIRAC.Core.Utilities.SiteCEMapping import getSiteCEMapping
from DIRAC.Core.Utilities.SiteSEMapping import getSiteSEMapping

server = RPCClient('ResourceStatus/ResourceStatus', timeout=120)

T1List = gConfig.getSections('Resources/Sites/LCG', True)['Value'][0:7]
T2List = gConfig.getSections('Resources/Sites/LCG', True)['Value'][7:]

for site in T1List:
  res = server.addOrModifySite(site, 'T1', 'one of the T1', 'Active', 'init', datetime.utcnow(), 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59))
  if not res['OK']:
    print 'ERROR' + res['Message']

for site in T2List:
  res = server.addOrModifySite(site, 'T2', 'one of the T2', 'Active', 'init', datetime.utcnow(), 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59))
  if not res['OK']:
    print 'ERROR' + res['Message']

siteCE = getSiteCEMapping('LCG')['Value']
for site in siteCE.keys():
  for ce in siteCE[site]:
    res = server.addOrModifyResource(ce, 'CE', site, 'Active', 'init', datetime.utcnow(), 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59))
    if not res['OK']:
      print 'ERROR' + res['Message']

#siteSE = getSiteSEMapping('LCG')['Value']
#for site in siteSE.keys():
#  for se in siteSE[site]:
#    res = server.addOrModifyResource(se, 'SE', site, 'Active', 'init', datetime.utcnow(), 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59))
#    if not res['OK']:
#      print 'ERROR' + res['Message']

#SEList = ['srm-lhcb.cern.ch', 'srm-v2.cr.cnaf.infn.it', 'storm-fe-lhcb.cr.cnaf.infn.it', 
#'gridka-dCache.fzk.de', 'ccsrm.in2p3.fr', 'srm.grid.sara.nl', 'srmlhcb.pic.es', 
#'srm-lhcb.gridpp.rl.ac.uk', 'srm.epcc.ed.ac.uk', 'grid05.lal.in2pr.fr', 'srm.glite.ecdf.ed.ac.uk', 'gridstore.cs.tcd.ie', 'marsedpm.in2p3.fr', 'lcgse01.phy.bris.ac.uk']
#for se in SEList:
res = server.addOrModifyResource('srm-lhcb.cern.ch', 'SE', 'LCG.CERN.ch', 'Active', 'init', datetime.utcnow(), 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59))
res = server.addOrModifyResource('srm-v2.cr.cnaf.infn.it', 'SE', 'LCG.CNAF.it', 'Active', 'init', datetime.utcnow(), 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59))
res = server.addOrModifyResource('storm-fe-lhcb.cr.cnaf.infn.it', 'SE', 'LCG.CNAF.it', 'Active', 'init', datetime.utcnow(), 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59))
res = server.addOrModifyResource('gridka-dCache.fzk.de', 'SE', 'LCG.GRIDKA.de', 'Active', 'init', datetime.utcnow(), 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59))
res = server.addOrModifyResource('ccsrm.in2p3.fr', 'SE', 'LCG.IN2P3.fr', 'Active', 'init', datetime.utcnow(), 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59))
res = server.addOrModifyResource('srm.grid.sara.nl', 'SE', 'LCG.NIKHEF.nl', 'Active', 'init', datetime.utcnow(), 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59))
res = server.addOrModifyResource('srmlhcb.pic.es', 'SE', 'LCG.PIC.es', 'Active', 'init', datetime.utcnow(), 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59))
res = server.addOrModifyResource('srm-lhcb.gridpp.rl.ac.uk', 'SE', 'LCG.RAL.uk', 'Active', 'init', datetime.utcnow(), 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59))

