########################################################################
# $Id: ClassAdLight.py,v 1.2 2008/04/14 10:54:07 atsareg Exp $
########################################################################

""" ClassAd Class - a light purely Python representation of the
    Condor ClassAd library.
"""

__RCSID__ = "$Id: ClassAdLight.py,v 1.2 2008/04/14 10:54:07 atsareg Exp $"

import string

class ClassAd:

  def __init__(self,jdl):
    """ClassAd constructor from a JDL string
    """
    self.contents = {}
    result = self.__analyse_jdl(jdl)
    if result:
      self.contents = result

  def __analyse_jdl(self,jdl,index = 0):
    """Analyse one [] jdl enclosure
    """

    jdl=jdl.strip()

    # Strip all the blanks first
    #temp = jdl.replace(' ','').replace('\n','')
    temp = jdl

    result = {}

    if temp[0] != '[' or temp[-1] != ']':
      print "Invalid JDL: it should start with [ and end with ]"
      return result

    # Parse the jdl string now
    body = temp[1:-1]
    index = 0
    namemode = 1
    valuemode = 0
    while index < len(body):
      if namemode:
        ind = body.find("=",index)
        if ind != -1:
          name = body[index:ind]
          index = ind+1
          valuemode = 1
          namemode = 0
        else:
          break
      elif valuemode:
        ind1 = body.find("[",index)
        ind2 = body.find(";",index)
        if ind1 != -1 and ind1 < ind2:
          value,newind = self.__find_subjdl(body,ind1)
        elif ind1 == -1 and ind2 == -1:
          value = body[index:]
          newind = len(body)
        else:
          if index == ind2:
            return {}
          else:
            value = body[index:ind2]
            newind = ind2+1

        result[name.strip()] = value.strip().replace('\n','')
        index = newind
        valuemode = 0
        namemode = 1

    return result

  def __find_subjdl(self,body,index):
    """ Find a full [] enclosure starting from index
    """
    result = ''
    if body[index] != '[':
      return (result,0)

    depth = 0
    ind = index
    while ( depth < 10 ):
      ind1 = body.find(']',ind+1)
      ind2 = body.find('[',ind+1)
      if ind2 != -1 and ind2 < ind1:
        depth += 1
        ind = ind2
      else:
        if depth > 0:
          depth -= 1
          ind = ind1
        else:
          result = body[index:ind1+1]
          if body[ind1+1] == ";":
            return (result,ind1+2)
          else:
            return result,0

    return result,0

  def insertAttributeInt(self,name,attribute):
    """Insert a named integer attribute
    """

    self.contents[name]= str(attribute)

  def insertAttributeBool(self,name,attribute):
    """Insert a named boolean attribute
    """

    if attribute:
      self.contents[name]= 'true'
    else:
      self.contents[name]= 'false'

  def insertAttributeString(self,name,attribute):
    """Insert a named string attribute
    """

    self.contents[name]= '"'+str(attribute)+'"'

  def insertAttributeVectorString(self,name,attributelist):
    """Insert a named string list attribute
    """

    tmp = map ( lambda x : '"'+x+'"',attributelist )
    tmpstr = string.join(tmp,',')
    self.contents[name]= '{'+tmpstr+'}'

  def lookupAttribute(self,name):
    """Check the presence of the given attribute
    """

    return self.contents.has_key(name)

  def set_expression(self,name,attribute):
    """Insert a named expression attribute
    """

    self.contents[name]= str(attribute)

  def get_expression(self,name):
    """Get expression corresponding to a named attribute
    """

    if self.contents.has_key(name):
      if type(self.contents[name])==type(1):
        return str(self.contents[name])
      else :
        return self.contents[name]
    else:
      return ""

  def getListFromExpression(self,name):
    """ Get a list of values from a given expression
    """

    tempString = self.get_expression(name)
    tempString = tempString.replace("{","").replace("}","").replace("\"","").replace(" ","")

    return tempString.split(',')

  def getDictionaryFromSubJDL(self,name):
    """ Get a dictionary of the JDL attributes from a subsection
    """

    tempList = self.get_expression(name)[1:-1]
    resDict = {}
    for p in tempList.split(';'):
      if len(p.split('=')) == 2:
        resDict[p.split('=')[0].strip()] = p.split('=')[1].strip().replace('"','')
      else:
        return {}

    return resDict

  def deleteAttribute(self,name):
    """Delete a named attribute
    """

    if self.contents.has_key(name):
      del self.contents[name]
      return 1
    else:
      return 0

  def isOK(self):
    """Check the JDL validity - to be defined
    """

    if self.contents:
      return 1
    else:
      return 0

  def asJDL(self):
    """Convert the JDL description into a string
    """

    result = ''
    for name,value in self.contents.items():
      if value[0:1] == "{":
        result = result + 4*' '+name+" = \n"
        result = result +  8*' '+'{\n'
        strings = value[1:-1].split(',')
        for st in strings:
          result = result + 12*' '+st+',\n'
        result = result[:-2] + '\n' +  8*' '+'};\n'
      elif value[0:1] == "[":
        tempad = ClassAd(value)
        tempjdl = tempad.asJDL()+';'
        lines = tempjdl.split('\n')
        result = result + 4*' '+name+" = \n"
        for line in lines:
          result = result + 8*' '+line+'\n'

      else:
        result = result + 4*' ' + name+' = '+ str(value) + ';\n'

    return "[ \n"+result[:-2]+"\n]"


