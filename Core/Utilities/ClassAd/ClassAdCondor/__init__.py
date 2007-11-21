##############################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/ClassAd/ClassAdCondor/__init__.py,v 1.2 2007/11/21 18:39:21 atsareg Exp $
##############################################################

"""This is a Python binding module for the Condor ClassAd
   library. The ClassAd API is extended for the use in the
   DIRAC project
"""

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
  tempString = tempString.replace("{","").replace("}","").replace("\"","").replace(" ","")

  return tempString.split(',')

def lookupAttribute(self,name):
  return self.get_expression(name) is not None

ClassAd.lookupAttribute = lookupAttribute

def isOK(self):
  return self.this is not None

ClassAd.isOK = isOK

def matchClassAd(ca1,ca2):
  """ Match the 2 ClassAds and provide the result as tuple of 3 booleans:
      'symmetricMatch','leftMatchesRight','rightMatchesLeft'
  """

  mca = MatchClassAd()
  mca.initialize(ca1,ca2)

  result_s,error_s = mca.getAttributeBool('symmetricMatch')
  result_l,error_l = mca.getAttributeBool('leftMatchesRight')
  result_r,error_r = mca.getAttributeBool('rightMatchesLeft')

  # Have to release the MatchClassAd to avoid Seg Fault while gabarge collection
  mca.release()

  if error_s and error_l and error_r:
    return result_s, result_l, result_r
  else:
    return S_ERROR('Failed to match the given ClassAds: sym:%d left:%d right"%d' % (error_s,error_l,error_r))
