"""
The Data Management Request contains all the necessary information for
a data management operation
"""

import xml.dom.minidom, time
from DIRAC.Core.Utilities.File import makeGuid
from DIRAC import gLogger, S_OK, S_ERROR

class DataManagementRequest:

  def __init__(self,request=None):

    # A common set of keys are defined for all operations.
    self.request_keys = ['RequestID','LFN','TargetSE','Status',
                         'Operation','Retry']
    # These keys are used in addition for transfers...
    self.transfer_keys = ['Size','LocalPFN','SourceSE','GUID']
    # ...registrations...
    self.register_keys = ['Size','PFN','Catalog','GUID','Md5','Addler']
    # ...removals...
    self.removal_keys = ['PFN','Catalog']
    # ...stages.
    self.stage_keys = []

    # These contain lists of all the type of sub requests in the current request.
    self.transfers = []
    self.registers = []
    self.removals = []
    self.stages = []

    self.jobid = None
    self.ownerDN = ''
    self.date = time.strftime('%Y-%m-%d %H:%M:%S')
    self.mode = ''
    self.dirac_instance = ''

    if request:
      dom = xml.dom.minidom.parseString(request)
      header = dom.getElementsByTagName('Header')[0]
      self.jobid = header.getAttribute('JobID')
      self.ownerDN = header.getAttribute('OwnerDN')
      self.date = header.getAttribute('Date')
      self.mode = header.getAttribute('Mode')

      if dom.getElementsByTagName('TRANSFER_REQUEST'):
        for subRequest in dom.getElementsByTagName('TRANSFER_REQUEST'):
          reqDic = {}
          keys = self.request_keys+self.transfer_keys
          for key in keys:
            recDic[key] = subRequest.getAttribute(key)
          self.addTransfer(reqDic)

      if dom.getElementsByTagName('REGISTER_REQUEST'):
        for subRequest in dom.getElementsByTagName('REGISTER_REQUEST'):
          reqDic = {}
          keys = self.request_keys+self.register_keys
          for key in keys:
            reqDic[key] = subRequest.getAttribute(key)
          self.addRegister(reqDic)

      if dom.getElementsByTagName('REMOVAL_REQUEST'):
        for subRequest in dom.getElementsByTagName('REMOVAL_REQUEST'):
          reqDic = {}
          keys = self.request_keys+self.removal_keys
          for key in keys:
            reqDic[key] = subRequest.getAttribute(key)
          self.addRemoval(reqDic)

      if dom.getElementsByTagName('STAGE_REQUEST'):
        for subRequest in dom.getElementsByTagName('STAGE_REQUEST'):
          reqDic = {}
          keys = self.request_keys+self.stage_keys
          for key in keys:
            reqDic[key] = subRequest.getAttribute(key)
          self.addStage(reqDic)

###############################################################

  def setCurrentDate(self):
    """ Set the request date to the current date and time
    """
    self.date = time.strftime('%Y-%m-%d %H:%M:%S')

  def getCurrentDate(self):
    """ Get the date the request was set
    """
    return self.date

  def setJobID(self,jobid):
    """ Set the associated Job ID
    """
    self.jobid = jobid

  def getJobID(self):
    """ Get the request job ID
    """
    return self.jobid

  def setOwnerDN(self,ownerDN):
    """ Set the request owner DN
    """
    self.ownerDN = ownerDN

  def getOwnerDN(self):
    """ get the request owner DN
    """
    return self.ownerDN

  def setMode(self,mode):
    """ Set the DIRAC WMS mode ( instance )
    """
    self.mode = mode

  def getMode(self):
    """ Set the DIRAC WMS mode ( instance )
    """
    return self.mode

  def setDiracInstance(self,instance):
    """ Set the DIRAC WMS mode ( instance )
    """
    self.dirac_instance = instance

  def getDiracInstance(self):
    """ Set the DIRAC WMS mode ( instance )
    """
    return self.dirac_instance

  def getTransfer(self,ind):
    """ Get the transfer operation specified by its index
    """
    return self.transfers[ind]

  def getRegister(self,ind):
    """ Get the register operation specified by its index
    """
    return self.registers[ind]

  def getRemoval(self,ind):
    """ Get the removal operation specified by its index
    """
    return self.removals[ind]

  def getStage(self,ind):
    """ Get the stage operation specified by its index
    """
    return self.stages[ind]

  def getNumberOfTransfers(self):
    """ Get the number of transfer operations
    """
    return len(self.transfers)

  def getNumberOfRegisters(self):
    """  Get the number of registration operations
    """
    return len(self.registers)

  def getNumberOfRemovals(self):
    """  Get the number of removal operations
    """
    return len(self.removals)

  def getNumberOfStages(self):
    """  Get the number of stage operations
    """
    return len(self.stages)

  def setTransferDone(self,ind):
    """ Set the transfer operation to Done status
    """
    self.transfers[ind]['Status'] = 'Done'

  def setRegisterDone(self,ind):
    """ Set the registration operation to Done status
    """
    self.registers[ind]['Status'] = 'Done'

  def setRemovalDone(self,ind):
    """ Set the removal operation to Done status
    """
    self.removals[ind]['Status'] = 'Done'

  def setStageDone(self,ind):
    """ Set the stage operation to Done status
    """
    self.stages[ind]['Status'] = 'Done'

###############################################################

  def isEmpty(self):
    """ Check if the request contains more operations to be performed
    """
    result = 1
    for tdic in self.transfers:
      if tdic['Status'] == "Waiting":
        return 0
    for tdic in self.registers:
      if tdic['Status'] == "Waiting":
        return 0
    for tdic in self.removals:
      if tdic['Status'] == "Waiting":
        return 0
    for tdic in self.stages:
      if tdic['Status'] == "Waiting":
        return 0
    return result

###############################################################

  def addTransfer(self,rdic):
    """ Add a new transfer operation
    """
    reqDic = {}
    keys = self.request_keys+self.transfer_keys
    for key in keys:
      if rdic.has_key(key):
        reqDic[key] = rdic[key]
      else:
        reqDic[key] = ''

    if not reqDic['Status']:
      reqDic['Status'] = 'Waiting'
    if not reqDic['Retry']:
      reqDic['Retry'] = 0
    if not reqDic['GUID'] or reqDic['GUID'] == "None":
      reqDic['GUID'] = makeGuid()
    self.transfers.append(reqDic)

  def addRegister(self,rdic,catalog=None):
    """ Add a new registration operation
    """
    reqDic = {}
    keys = self.request_keys+self.register_keys
    for key in keys:
      if rdic.has_key(key):
        reqDic[key] = rdic[key]
      else:
        reqDic[key] = ''

    if not reqDic['Status']:
      reqDic['Status'] = 'Waiting'
    if not reqDic['Retry']:
      reqDic['Retry'] = 0
    if not reqDic['GUID'] or reqDic['GUID'] == "None":
      reqDic['GUID'] = makeGuid()
    if catalog:
      reqDic['Catalog'] = catalog
    self.registers.append(reqDic)

  def addRemoval(self,rdic,catalog=None):
    """ Add a new removal operation
    """
    reqDic = {}
    keys = self.request_keys+self.removal_keys
    for key in keys:
      if rdic.has_key(key):
        reqDic[key] = rdic[key]
      else:
        reqDic[key] = ''

    if not reqDic['Status']:
      reqDic['Status'] = 'Waiting'
    if not reqDic['Retry']:
      reqDic['Retry'] = 0
    if catalog:
      reqDic['Catalog'] = catalog
    self.removals.append(reqDic)

  def addStage(self,rdic):
    """ Add a new stage operation
    """
    reqDic = {}
    keys = self.request_keys+self.removal_keys
    for key in keys:
      if rdic.has_key(key):
        reqDic[key] = rdic[key]
      else:
        reqDic[key] = ''

    if not reqDic['Status']:
      reqDic['Status'] = 'Waiting'
    if not reqDic['Retry']:
      reqDic['Retry'] = 0
    self.stages.append(reqDic)

###############################################################

  def dump(self):
    """ Sent to the logger all the sub-requests in this DM request.
    """
    ind = 1
    for rdic in self.transfers:
      gLogger.info( '\n====== Transfer =======',ind,'====================' )
      for key in rdic.keys():
        gLogger.info( (key+':').ljust(15),rdic[key] )
      ind = ind+1
    gLogger.info( '===============================================\n' )
    ind = 1
    for rdic in self.registers:
      gLogger.info( '\n====== Register =======',ind,'====================' )
      for key in rdic.keys():
        gLogger.info( (key+':').ljust(15),rdic[key] )
      ind = ind+1
    gLogger.info( '===============================================\n' )
    ind = 1
    for rdic in self.removals:
      gLogger.info( '\n====== Removal =======',ind,'====================' )
      for key in rdic.keys():
        gLogger.info( (key+':').ljust(15),rdic[key] )
      ind = ind+1
    gLogger.info( '===============================================\n' )
    ind = 1
    for rdic in self.stages:
      gLogger.info( '\n====== Stage =======',ind,'====================' )
      for key in rdic.keys():
        gLogger.info( (key+':').ljust(15),rdic[key] )
      ind = ind+1
    gLogger.info( '===============================================\n' )

###############################################################

  def dumpShortToString(self):
    """ Generate summary string for all the sub-requests in this request.
    """
    out = ''
    for rdic in self.transfers:
      if rdic['Status'] == "Waiting":
        out = out + 'Transfer: '+rdic['LFN']+' '+rdic['Operation']+' to '+rdic['TargetSE']+'\n'
    for rdic in self.registers:
      if rdic['Status'] == "Waiting":
        out = out + 'Register: '+rdic['LFN']+' '+rdic['Operation']+' to '+rdic['TargetSE']+'\n'
    for rdic in self.removals:
      if rdic['Status'] == "Waiting":
        out = out + 'Removal: '+rdic['LFN']+' '+rdic['Operation']+' to '+rdic['TargetSE']+'\n'
    for rdic in self.stages:
      if rdic['Status'] == "Waiting":
        out = out + 'Stage: '+rdic['LFN']+' '+rdic['Operation']+' at '+tdic['TargetSE']+'\n'
    return out

###############################################################

  def toXML(self):
    """ Output the request (including all sub-requests) to XML.
    """

    out =  '<?xml version="1.0" encoding="UTF-8" ?>\n\n'
    out = out + '<DATA_MANAGEMENT_REQUEST>\n\n'

    attributes = ''
    if self.jobid:
      attributes = attributes + ' JobID="'+self.jobid+'" '
    else:
      attributes = attributes + ' JobID="0" '
    if self.ownerDN:
      attributes = attributes + ' OwnerDN="'+self.ownerDN+'" '
    if self.date:
      attributes = attributes + ' Date="'+self.date+'" '
    if self.mode:
      attributes = attributes + ' Mode="'+self.mode+'" '
    if self.dirac_instance:
      attributes = attributes + ' DiracInstance="'+self.dirac_instance+'" '

    out = out + '<Header '+attributes+' />\n\n'

    for rdic in self.transfers:
      out = out+'  <TRANSFER_REQUEST\n'
      for key,value in rdic.items():
        out = out + '    '+key+'="'+str(value)+'"\n'
      out = out+'  />\n\n'

    for rdic in self.registers:
      out = out+'  <REGISTER_REQUEST\n'
      for key,value in rdic.items():
        out = out + '    '+key+'="'+str(value)+'"\n'
      out = out+'  />\n\n'

    for rdic in self.removals:
      out = out+'  <REMOVAL_REQUEST\n'
      for key,value in rdic.items():
        out = out + '    '+key+'="'+str(value)+'"\n'
      out = out+'  />\n\n'

    for rdic in self.stages:
      out = out+'  <STAGE_REQUEST\n'
      for key,value in rdic.items():
        out = out + '    '+key+'="'+str(value)+'"\n'
      out = out+'  />\n\n'

    out = out + '</DATA_MANAGEMENT_REQUEST>\n'
    return out

###############################################################

  def toFile(self,fname):
    reqfile = open(fname,'w')
    xmlstr = self.toXML()
    reqfile.write(xmlstr)
    reqfile.close()
