# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/DataManagementSystem/Client/FileContainer.py,v 1.2 2009/07/28 14:02:38 acsmith Exp $
__RCSID__ = "$Id: FileContainer.py,v 1.2 2009/07/28 14:02:38 acsmith Exp $"

""" The file container is to store all information associated to a file
"""

import os
from DIRAC.Core.Utilities.File import makeGuid
from DIRAC import S_OK, S_ERROR

class FileContainer:

  def __init__(self,attributeDict={}):
    # These are the possible attributes for the file
    self.fileAttributes = { 'FileID'     :  0,
                            'Status'     :  'Waiting',
                            'LFN'        :  '',
                            'PFN'        :  '',
                            'Size'       :  0,
                            'GUID'       :  makeGuid(),
                            'Md5'        :  '',
                            'Adler'      :  '',
                            'Attempt'    :  0,
                            'Error'      :  ''}
    for key,value in attributeDict.items():
      self.fileAttributes[key] = value

  #####################################################################
  #
  #  Attribute access methods
  #

  def __getattr__(self,name):
    """ Generic method to access request attributes or parameters
    """
    # If the client has attempted to get a file attribute
    if name.find('get') ==0:
      item = name[3:]
      self.item_called = item
      if item in self.fileAttributes.keys():
        return self.__get_attribute
      else:
        return S_ERROR("Item %s not known" % item)
    # If the client has attempted to set a file attribute
    elif name.find('set') == 0:
      item = name[3:]
      self.item_called = item
      if item in self.fileAttributes.keys():
        return self.__set_attribute
      else:
        return S_ERROR("Item %s not known" % item)
    # Otherwise they are trying to do something we dont like
    else:
      return S_ERROR("Action %s not known" % name)

  def __get_attribute(self):
    """ Generic method to get attributes
    """
    return S_OK(self.fileAttributes[self.item_called])

  def __set_attribute(self,value):
    """ Generic method to set attribute value
    """
    if type(value) != type(self.fileAttributes[self.item_called]):
      return S_ERROR("%s should be type %s and not %s" % (self.item_called,type(self.fileAttributes[self.item_called]),type(value)))
    self.fileAttributes[self.item_called] = value
    return S_OK()

  def getAttributes(self):
    """ Get the dictionary of the file attributes
    """
    return S_OK(self.fileAttributes)

  def isEmpty(self):
    if self.fileAttributes['Status'] in ['Done','Failed']:
      return S_OK(1)
    return S_OK(0)

  def getDigest(self):
    """ Get short description string of file and status 
    """
    digestList = []
    fname = ''
    if self.fileAttributes['LFN']:
      fname = os.path.basename(self.fileAttributes['LFN'])
    elif self.fileAttributes['PFN']:
      fname = os.path.basename(self.fileAttributes['PFN'])
    digestList.append(fname)
    digestList.append(self.fileAttributes['Status'])
    if self.fileAttributes['Attempt']:
      digestList.append(self.fileAttributes['Attempt'])
      digestList.append(self.fileAttributes['Error'])
    digestString = ":".join(digestList)
    return S_OK(digestString)
