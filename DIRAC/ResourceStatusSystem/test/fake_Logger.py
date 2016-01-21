################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id$"

""" 
  fake gLogger 
  Every function can simply return S_OK() (or nothing)
"""

class Logger:
  
  def __init__(self):
    pass

  def info( self, sMsg, sVarMsg = '' ):
    pass

  def error( self, sMsg, sVarMsg = '' ):
    print sMsg
  
  def exception( self, sMsg = "", sVarMsg = '', lException = False, lExcInfo = False ):
    print sMsg
    print lException

################################################################################
################################################################################
  
gLogger = Logger()

################################################################################
################################################################################

class Config:
  
  def __init__(self):
    pass
  
  def addListenerToNewVersionEvent(self, a):
    pass

  def getValue(self, a):
    return "LHCb"

  def getSections(self, a = '', b = ''):
    return {'OK':True, 'Value': ['a', 'b']}

################################################################################
################################################################################

gConfig = Config()

################################################################################
################################################################################

class DiracAdmin:
  pass

################################################################################
################################################################################

class CSAPI:
  pass

################################################################################
################################################################################

def getExt():
  return "LHCb"

def getSetup():
  return {'OK': True, 
          'Value': 'LHCb-Development'}

def getSENodes(SE):
  return {'OK': True, 
          'Value': ['srm-lhcb.cern.ch', 'storm-fe-lhcb.cr.cnaf.infn.it']}

def getLFCSites():
  return {'OK': True, 
          'Value': ['LCG.CERN.ch', 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr', 'LCG.NIKHEF.nl', 
                    'LCG.PIC.es', 'LCG.RAL.uk']}

def getLFCNode( sites = None, readable = None ):
  return {'OK': True, 
          'Value': ['lfc-lhcb-ro.cern.ch', 'prod-lfc-lhcb-central.cern.ch', 'lfc-lhcb-ro.cr.cnaf.infn.it', 
                    'lhcb-lfc-fzk.gridka.de', 'lfc-lhcb-ro.in2p3.fr', 'lfc-lhcb.grid.sara.nl', 'lfclhcb.pic.es', 
                    'lhcb-lfc.gridpp.rl.ac.uk']}

def getGOCSiteName(diracSiteName):
  return {'OK': True, 
          'Value': 'CERN-PROD'}

def getDIRACSiteName(a):
  return {'OK': True, 
          'Value': ['LCG.CERN.ch', 'LCG.CERN-MPP.ch']}

def getFTSSites():
  return {'OK': True, 'Value': ['LCG.NIKHEF.nl', 'LCG.CNAF.it', 'LCG.IN2P3.fr', 'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.GRIDKA.de', 'LCG.CERN.ch', 'LCG.SARA.nl']}

def getFTSEndpoint( sites = None ):
  return {'OK': True, 'Value': ['fts.grid.sara.nl', 'fts.cr.cnaf.infn.it', 'cclcgftsprod.in2p3.fr', 'fts.pic.es', 'lcgfts.gridpp.rl.ac.uk', 'fts-fzk.gridka.de', 'fts22-t0-export.cern.ch']}

def getVOMSEndpoints():
  return {'OK': True, 'Value': ['voms.cern.ch', 'lcg-voms.cern.ch']}

def getCEType(a= None, b = None):
  return {'OK': True, 'Value': 'LCG'}

def getStorageElements(a = None):
  return {'OK': True, 
          'Value': ['CERN_M-DST', 'CERN-tape', 'CERN-RDST', 'CERN-disk', 'CERN-RAW', 
                    'CERN-FAILOVER', 'CERN-HIST', 'CERN-ARCHIVE', 'CERN-Failover', 'CERN_MC_M-DST', 
                    'CERN-DEBUG', 'CERN-USER']}

def getStorageElementStatus(a, b):
  return {'OK': True, 'Value': 'Active'}


def getSiteCEMapping(gridName=''):
  return {'OK': True, 
          'Value': {'LCG.Cambridge.uk': ['serv07.hep.phy.cam.ac.uk'], 'LCG.LPN.fr': ['polgrid1.in2p3.fr'], 
                    'LCG.Legnaro.it': ['t2-ce-01.lnl.infn.it', 't2-ce-02.lnl.infn.it', 't2-ce-03.lnl.infn.it', 't2-ce-04.lnl.infn.it', 't2-ce-05.lnl.infn.it']
                    }}

def getSiteSEMapping(gridName=''):
  return {'OK': True, 
          'Value': {'LCG.NIKHEF.nl': ['NIKHEF-DST', 'NIKHEF_M-DST', 'NIKHEF_MC_M-DST', 'NIKHEF_MC-DST', 'NIKHEF-USER', 'NIKHEF-FAILOVER'], 
                    'LCG.TCD.ie': ['TCD-disk'], 'LCG.UKI-SCOTGRID-ECDF.uk': ['Edinburgh-DST'], 
                    'LCG.CNAF.it': ['CNAF-RAW', 'CNAF-DST', 'CNAF_M-DST', 'CNAF-USER', 'CNAF-FAILOVER', 'CNAF-disk', 'CNAF-tape', 'CNAF-RDST', 'CNAF_MC_M-DST', 'CNAF_MC-DST'], 'LCG.LAL.fr': ['GRIF-USER'], 'LCG.IN2P3.fr': ['IN2P3-RAW', 'IN2P3-DST', 'IN2P3_M-DST', 'IN2P3-USER', 'IN2P3-FAILOVER', 'IN2P3-disk', 'IN2P3-tape', 'IN2P3-RDST', 'IN2P3_MC_M-DST', 'IN2P3_MC-DST'], 'LCG.PIC.es': ['PIC-DST', 'PIC-RDST', 'PIC_M-DST', 'PIC-USER', 'PIC-FAILOVER', 'PIC-RAW', 'PIC-disk', 'PIC-tape', 'PIC_MC_M-DST', 'PIC_MC-DST'], 'LCG.RAL.uk': ['RAL-RAW', 'RAL-DST', 'RAL_M-DST', 'RAL-USER', 'RAL-FAILOVER', 'RAL-disk', 'RAL-tape', 'RAL-RDST', 'RAL_MC_M-DST', 'RAL_MC-DST'], 'LCG.CPPM.fr': ['CPPM-USER'], 'LCG.SARA.nl': ['NIKHEF-RAW', 'NIKHEF-RDST'], 'LCG.Dortmund.de': ['Dortmund-disk'], 'LCG.CERN.ch': ['CERN-RAW', 'CERN_M-DST', 'CERN-USER', 'CERN-FAILOVER', 'CERN-disk', 'CERN-tape', 'CERN-RDST', 'CERN_MC_M-DST', 'CERN-HIST'], 'LCG.GRIDKA.de': ['GRIDKA-RAW', 'GRIDKA-DST', 'GRIDKA_M-DST', 'GRIDKA-USER', 'GRIDKA-FAILOVER', 'GRIDKA-disk', 'GRIDKA-tape', 'GRIDKA-RDST', 'GRIDKA_MC_M-DST', 'GRIDKA_MC-DST'], 'LCG.RAL-test.uk': ['RAL-USER-TEST219', 'RAL-DST-TEST219', 'RAL_M-DST-TEST219', 'RAL-RAW-TEST219', 'RAL-RDST-TEST219', 'RAL-FAILOVER-TEST219']}}
  
def getMailForUser( users ):
  return {'OK' : True,
          'Value': []
          }
  
def getOperationMails( op ):
  return {'OK' : True,
          'Value': []
          }
  
def getSites( grids = None ):
  return {'Ok': True,
          'Value' : ['LCG.NIKHEF.nl', 'LCG.CNAF.it', 'LCG.IN2P3.fr', 'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.GRIDKA.de', 'LCG.CERN.ch', 'LCG.SARA.nl']}
   
def getSiteTier( sites ):
  return {'Ok': True,
          'Value' : [0]}
  
################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
################################################################################

'''
  HOW DOES THIS WORK.
    
    will come soon...
'''
            
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  