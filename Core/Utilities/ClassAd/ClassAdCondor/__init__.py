##############################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/ClassAd/ClassAdCondor/__init__.py,v 1.5 2007/11/29 22:57:46 atsareg Exp $
##############################################################

"""This is a Python binding module for the Condor ClassAd
   library. The ClassAd API is extended for the use in the
   DIRAC project
"""

from DIRAC import S_OK, S_ERROR
from ClassAdBase import ClassAd, MatchClassAd
import string

# Provide the API used in the DIRAC project

def insertAttributeStringList(self,name,attrList):
  """ Insert an attribute of type list of strings
  """
  attrString = '{"'+string.join(attrList,'","')+'"}'
  result = self.insertExpression(name,attrString)
  return result

ClassAd.insertAttributeStringList = insertAttributeStringList

def getAttributeStringList(self,name):
  """ Get a list of values from a given expression
  """

  tempString = self.get_expression(name)
  if tempString != '<error:null expr>':
    tempString = tempString.replace("{","").replace("}","").replace("\"","").replace(" ","")
    return tempString.split(',')
  else:
    return []

ClassAd.getAttributeStringList = getAttributeStringList

def lookupAttribute(self,name):
  expr = self.get_expression(name) 
  if expr is not None and expr != '<error:null expr>':
    return True
  else:
    return False  

ClassAd.lookupAttribute = lookupAttribute

def isOK(self):
  return self.this is not None

ClassAd.isOK = isOK

def getAttributeString(self,name):
  result,success = self.get_attribute_string(name)
  if success:
    return result
  else:
    return None
    
def getAttributeInt(self,name):
  result,success = self.get_attribute_int(name)
  if success:
    return result
  else:
    return None      
    
def getAttributeFloat(self,name):
  result,success = self.get_attribute_float(name)
  if success:
    return result
  else:
    return None    
    
def getAttributeBool(self,name):
  result,success = self.get_attribute_bool(name)
  if success:
    return result
  else:
    return None    
    
ClassAd.getAttributeString = getAttributeString
ClassAd.getAttributeInt = getAttributeInt
ClassAd.getAttributeFloat = getAttributeFloat
ClassAd.getAttributeBool = getAttributeBool
  

def matchClassAd(ca1,ca2):
  """ Match the 2 ClassAds and provide the result as tuple of 3 booleans:
      'symmetricMatch','leftMatchesRight','rightMatchesLeft'
  """

  mca = MatchClassAd()
  mca.initialize(ca1,ca2)

  result_s = mca.getAttributeBool('symmetricMatch')
  result_l = mca.getAttributeBool('leftMatchesRight')
  result_r = mca.getAttributeBool('rightMatchesLeft')

  # Have to release the MatchClassAd to avoid Seg Fault while gabarge collection
  mca.release()

  if result_s is not None and result_l is not None and result_r is not None:
    return S_OK((result_s, result_l, result_r))
  else:
    result = S_ERROR('Failed to match the given ClassAds: sym:%s left:%s right"%s' % (result_s,result_l,result_r))
    result['Value'] = (result_s, result_l, result_r)
    return result
