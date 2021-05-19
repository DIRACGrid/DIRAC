"""
    This module defines a classs for a generic Workflow Parameter. It also defines
    a ParameterCollection class as a list of parameters as well as an AttributeCollection
    class which is the base class for the main Workflow classes.
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

import six
from DIRAC.Core.Workflow.Utility import *

__RCSID__ = "$Id$"

# unbound method, returns indentated string


def indent(indent=0):
  return indent * 2 * ' '


class Parameter(object):

  def __init__(self, name=None, value=None, type=None, linked_module=None,
               linked_parameter=None, typein=None, typeout=None, description=None, parameter=None):
    # the priority to assign values
    # if parameter exists all values taken from there
    # and then owerriten by values taken from the arguments
    if isinstance(parameter, Parameter):
      self.name = parameter.name
      self.type = parameter.type
      self.value = parameter.value
      self.description = parameter.description
      self.linked_module = parameter.linked_module
      self.linked_parameter = parameter.linked_parameter
      self.typein = bool(parameter.typein)
      self.typeout = bool(parameter.typeout)
    else:
      #  default values
      self.name = ""
      self.type = "string"
      self.value = ""
      self.description = ""
      self.linked_module = ""
      self.linked_parameter = ""
      self.typein = False
      self.typeout = False

    if name is not None:
      self.name = name
    if type is not None:
      self.type = type
    if value is not None:
      self.setValue(value)
    if description is not None:
      self.description = description
    if linked_module is not None:
      self.linked_module = linked_module
    if linked_parameter is not None:
      self.linked_parameter = linked_parameter
    if typein is not None:
      self.setInput(typein)
    if typeout is not None:
      self.setOutput(typeout)

  def getName(self):
    return self.name

  def setName(self, n):
    self.name = n  # if collection=None it still will work fine

  def getValue(self):
    return self.value

  def getValueTypeCorrected(self):
    # this method used to generate code for the workflow
    # it NOT used to geterate XML!!!
    if self.isTypeString():
      return '"""' + str(self.value).replace('"', r'\"').replace("'", r"\'") + '"""'
    return self.value

  def setValue(self, value, type_=None):
    if type_ is not None:
      self.setType(type_)
    self.setValueByType(value)

  def setValueByType(self, value):
    type = self.type.lower()  # change the register
    if self.isTypeString():
      self.value = str(value)
    elif type == 'float':
      self.value = float(value)
    elif type == 'int':
      self.value = int(value)
    elif type == 'bool':
      self.value = bool(value)
    else:
      # raise TypeError('Can not assing value '+value+' of unknown type '\
      # + self.type + ' to the Parameter '+ str(self.name))
      # print 'WARNING: we do not have established conversion algorithm to
      # assing value ',value,' of unknown type ',self.type, ' to the Parameter
      # ', str(self.name)
      self.value = value

  def getType(self):
    return self.type

  def setType(self, type_):
    self.type = type_

  def isTypeString(self):
    """returns True if type is the string kind"""
    return self.type.lower() in ('string', 'jdl', 'option', 'parameter', 'jdlreqt')

  def getDescription(self):
    return self.description

  def setDescription(self, descr):
    self.description = descr

  def link(self, module, parameter):
    self.linked_module = module
    self.linked_parameter = parameter

  def unlink(self):
    self.linked_module = ""
    self.linked_parameter = ""

  def getLinkedModule(self):
    return self.linked_module

  def getLinkedParameter(self):
    return self.linked_parameter

  def getLink(self):
    # we have 4 possibilities
    # two fields can be filled independently
    # it is possible to fill one field with the valid information
    # spaces shall be ignored ( using strip() function)
    if (self.linked_module is None) or (self.linked_module.strip() == ''):
      if (self.linked_parameter is None) or (self.linked_parameter.strip() == ''):
        # both empty
        return ""
      else:
        # parameter filled
        return self.linked_parameter
    else:
      if (self.linked_parameter is None) or (self.linked_parameter.strip() == ''):
        return self.linked_module
    return self.linked_module + '.' + self.linked_parameter

  def isLinked(self):
    if (self.linked_module is None) or (self.linked_module.strip() == ''):
      if (self.linked_parameter is None) or (self.linked_parameter.strip() == ''):
        return False
    return True

  def preExecute(self):
    """ method to request watever parameter need to be defined before calling execute method
    returns TRUE if it needs to be done, FALSE otherwise
    PS: parameters with the output status only going to be left out"""
    return (not self.isOutput()) or self.isInput()

  def isInput(self):
    return self.typein

  def isOutput(self):
    return self.typeout

  def setInput(self, i):
    if isinstance(i, six.string_types):
      self.typein = self.__setBooleanFromString(i)
    else:
      self.typein = bool(i)

  def setOutput(self, i):
    if isinstance(i, six.string_types):
      self.typeout = self.__setBooleanFromString(i)
    else:
      self.typeout = bool(i)

  def __setBooleanFromString(self, i):
    return i.upper() == "TRUE"

  def __str__(self):
    return str(type(self)) + ": name=" + self.name + " value=" \
        + str(self.getValueTypeCorrected()) + " type=" + str(self.type)\
        + " linked_module=" + str(self.linked_module) + " linked_parameter=" + str(self.linked_parameter)\
        + " in=" + str(self.typein) + " out=" + str(self.typeout)\
        + " description=" + str(self.description)

  def toXML(self):
    return '<Parameter name="' + self.name + '" type="' + str(self.type)\
        + '" linked_module="' + str(self.linked_module) + '" linked_parameter="' + str(self.linked_parameter)\
        + '" in="' + str(self.typein) + '" out="' + str(self.typeout)\
        + '" description="' + str(self.description) + '">'\
        + '<value><![CDATA[' + str(self.getValue()) + ']]></value>'\
        + '</Parameter>\n'

  def compare(self, s):
    if isinstance(s, Parameter):
      return (self.name == s.name) and \
          (self.value == s.value) and \
          (self.type == s.type) and \
          (self.linked_module == s.linked_module) and \
          (self.linked_parameter == s.linked_parameter) and \
          (self.typein == s.typein) and \
          (self.typeout == s.typeout) and \
          (self.description == s.description)
    else:
      return False

  def copy(self, parameter):
    if not isinstance(parameter, Parameter):
      raise TypeError('Can not make a copy of object ' + str(type(self)) + ' from the ' + str(type(parameter)))
    self.name = parameter.name
    self.value = parameter.value
    self.type = parameter.type
    self.description = parameter.description
    self.linked_module = parameter.linked_module
    self.linked_parameter = parameter.linked_parameter
    self.typein = parameter.typein
    self.typeout = parameter.typeout

  def createParameterCode(self, ind=0, instance_name=None):
    if (instance_name is None) or (instance_name == ''):
      ret = indent(ind) + self.getName() + ' = ' + self.getValueTypeCorrected()
    else:
      if self.isLinked():
        ret = indent(ind) + instance_name + '.' + self.getName() + ' = ' + self.getLink()
      else:
        ret = indent(ind) + instance_name + '.' + self.getName() + ' = ' + str(self.getValueTypeCorrected())

    return ret + '  # type=' + self.getType() + ' in=' + str(self.isInput()) + ' out=' + \
        str(self.isOutput()) + ' ' + self.getDescription() + '\n'


class ParameterCollection(list):
  """ Parameter collection class representing a list of Parameters
  """
  def __init__(self, coll=None):
    list.__init__(self)
    if isinstance(coll, ParameterCollection):
      # makes a deep copy of the parameters
      for v in coll:
        self.append(Parameter(parameter=v))
    elif coll is not None:
      raise TypeError('Can not create object type ' + str(type(self)) + ' from the ' + str(type(coll)))

  def appendOrOverwrite(self, opt):
    index = self.findIndex(opt.getName())
    if index > -1:
      # print "Warning: Overriting Parameter %s = \"%s\" with the value
      # \"%s\""%(self[index].getName(), self[index].getValue(), opt.getValue())
      self[index] = opt
    else:
      list.append(self, opt)

  def append(self, opt):
    if isinstance(opt, ParameterCollection):
      for p in opt:
        self.appendOrOverwrite(p)
    elif isinstance(opt, Parameter):
      self.appendOrOverwrite(opt)
      return opt
    else:
      raise TypeError(
          'Can not append object type ' +
          str(type(opt)) + ' to the ' +
          str(type(self)) + '. Parameter type appendable only')

  def appendCopy(self, opt, prefix="", postfix=""):
    if isinstance(opt, ParameterCollection):
      for p in opt:
        self.appendOrOverwrite(Parameter(name=prefix + p.getName() + postfix, parameter=p))
    elif isinstance(opt, Parameter):
      self.appendOrOverwrite(Parameter(name=prefix + opt.getName() + postfix, parameter=opt))
    else:
      raise TypeError(
          'Can not append object type ' +
          str(type(opt)) + ' to the ' +
          str(type(self)) + '. Parameter type appendable only')

  def appendCopyLinked(self, opt, prefix="", postfix=""):
    if isinstance(opt, ParameterCollection):
      for p in opt:
        if p.isLinked():
          self.appendOrOverwrite(Parameter(name=prefix + p.getName() + postfix, parameter=p))
    elif isinstance(opt, Parameter):
      if opt.isLinked():
        self.appendOrOverwrite(Parameter(name=prefix + opt.getName() + postfix, parameter=opt))
    else:
      raise TypeError(
          'Can not append object type ' +
          str(type(opt)) + ' to the ' +
          str(type(self)) + '. Parameter type appendable only')

  def setValue(self, name, value, vtype=None):
    """ Method finds parameter with the name "name" and if exists its set value
    Returns True if successful
    """
    par = self.find(name)
    if par is None:
      print("ERROR ParameterCollection.setValue() can not find parameter with "
            "the name=%s to set Value=%s" % (name, value))
      return False
    else:
      par.setValue(value, vtype)
      return True

  def getInput(self):
    """ Get input linked parameters
    """
    return self.get(input=True)

  def getOutput(self):
    """ Get output linked parameters
    """
    return self.get(output=True)

  def getLinked(self):
    """ Get linked parameters
    """
    return self.get(input=True, output=True)

  def get(self, input=False, output=False):
    """ Get a copy of parameters. If input or output is True, get corresponding
        io type parameters only. Otherwise, get all the parameters
    """
    all = not input and not output

    params = ParameterCollection()
    for p in self:
      OK = False
      if all:
        OK = True
      elif input and p.isInput():
        OK = True
      elif output and p.isOutput():
        OK = True
      if OK:
        params.append(Parameter(parameter=p))

    return params

  def setLink(self, name, module_name, parameter_name):
    """ Method finds parameter with the name "name" and if exists its set value
    Returns True if successful
    """
    par = self.find(name)
    if par is None:
      print(
          "ERROR ParameterCollection.setLink() can not find parameter with the name=%s to link it with %s.%s" %
          (name, module_name, parameter_name))
      return False
    else:
      par.link(module_name, parameter_name)
      return True

  def linkUp(self, opt, prefix="", postfix="", objname="self"):
    """ This is a GROUP method operates on the 'obj' parameters using only parameters listed in 'opt' list
    Method will link self.parameters with the outer object (self) perameters using prefix and postfix
    for example if we want to link module instance with the step or step instance with the workflow
    opt - ParameterCollection or single Parameter (WARNING: used as reference to get a names, opt is not changing!!!)
    opt ALSO can be a list of string with the names of parameters to link
    objname - name of the object to connect with, usually 'self'
    """
    if isinstance(opt, ParameterCollection):
      # if parameter in the list opt is not present in the self
      # we are going to ignore this
      for p in opt:
        par = self.find(p.getName())
        if par is None:
          print("WARNING ParameterCollection.linkUp can not find parameter with the name=", p.getName(), " IGNORING")
        else:
          par.link(objname, prefix + p.getName() + postfix)
    elif isinstance(opt, Parameter):
      self.setLink(opt.getName(), objname, prefix + opt.getName() + postfix)
    elif isinstance(opt, list) and isinstance(opt[0], str):
      for s in opt:
        par = self.find(s)
        if par is None:
          print("ERROR ParameterCollection.linkUp() can not find parameter with the name=%s" % (s))
        else:
          par.link(objname, prefix + p.getName() + postfix)
    elif isinstance(opt, str):
      par = self.find(opt)
      if par is None:
        print("ERROR ParameterCollection.linkUp() can not find parameter with the name=%s" % (par))
      else:
        par.link(objname, prefix + par.getName() + postfix)
    else:
      raise TypeError('Can not link object type ' + str(type(opt)) + ' to the ' + str(type(self)) + '.')

  def unlink(self, opt):
    """ This is a GROUP method operates on the 'obj' parameters using only parameters listed in 'opt' list
    Method will unlink some self.parameters
    opt - ParameterCollection or single Parameter (WARNING: used as reference to get a names, opt is not changing!!!)
    opt ALSO can be a list of string with the names of parameters to link
    objname - name of the object to connect with, usually 'self'
    """
    if isinstance(opt, ParameterCollection):
      # if parameter in the list opt is not present in the self
      # we are going to ignore this
      for p in opt:
        par = self.find(p.getName())
        if par is None:
          print("WARNING ParameterCollection.linkUp can not find parameter with the name=", p.getName(), " IGNORING")
        else:
          par.unlink()
    elif isinstance(opt, Parameter):
      opt.unlink()
    elif isinstance(opt, list) and isinstance(opt[0], str):
      for s in opt:
        par = self.find(s)
        if par is None:
          print("ERROR ParameterCollection.unlink() can not find parameter with the name=%s" % (s))
        else:
          par.unlink()
    elif isinstance(opt, str):
      par = self.find(opt)
      if par is None:
        print("ERROR ParameterCollection.unlink() can not find parameter with the name=%s" % (s))
      else:
        par.unlink()
    else:
      raise TypeError('Can not unlink object type ' + str(type(opt)) + ' to the ' + str(type(self)) + '.')

  def removeAllParameters(self):
    self[:] = []

  def remove(self, name_or_ind):
    """ Removes a parameter given its name, or the index (the latter is not suggested), and only if it exists

        If there are 2 parameters with the same name, only the first will be removed
    """
    if isinstance(name_or_ind, list) and isinstance(name_or_ind[0], str):
      for s in name_or_ind:
        par = self.find(s)
        if par is None:
          print("ERROR ParameterCollection.remove() can not find parameter with the name=%s" % (s))
        else:
          index = self.findIndex(s)
          if index > -1:
            del self[index]

    elif isinstance(name_or_ind, str):  # we give a name
      index = self.findIndex(name_or_ind)
    elif isinstance(name_or_ind, int):  # we give the index
      index = name_or_ind

    if index > -1:
      del self[index]

  def find(self, name_or_ind):
    """ Method to find Parameters
    Return: Parameter """
    # work for index as well as for the string
    if isinstance(name_or_ind, str):  # we given name
      for v in self:
        if v.getName() == name_or_ind:
          return v
      return None

    elif isinstance(name_or_ind, six.integer_types):  # we given index
      return self[name_or_ind]
    return self[int(name_or_ind)]

  def findLinked(self, name_or_ind, linked_status=True):
    """ Method to find Parameters
    if linked_status is True it returns only linked Var from the list
    if linked_status is False it returns only NOTlinked Var from the list
    Return: Parameter """
    v = self.find(name_or_ind)
    if (v is not None) and (v.isLinked() != linked_status):
      return None
    return v

  def findIndex(self, name):
    for i, v in enumerate(self):
      if v.getName() == name:
        return i
    return -1

  def getParametersNames(self):
    return [v.getName() for v in self]

  def compare(self, s):
    # we comparing parameters only, the attributes will be compared in hierarchy above
    # we ignore the position of the Parameter in the list
    # we assume that names of the Parameters are DIFFERENT otherwise we have to change algorithm!!!
    if (not isinstance(s, ParameterCollection)) or (len(s) != len(self)):
      return False
    for v in self:
      for i in s:
        if v.getName() == i.getName() and v.compare(i):
          break
        else:
          return False
      return True

  def __str__(self):
    ret = [str(type(self)) + ':']
    for v in self:
      ret += [str(v)]
    return "\n".join(ret + [""])

  def toXML(self):
    return "".join(v.toXML() for v in self)

  def createParametersCode(self, indent=0, instance_name=None):
    return "".join(
        v.createParameterCode(indent, instance_name)
        for v in self if v.preExecute()
    )

  def resolveGlobalVars(self, wf_parameters=None, step_parameters=None):
    """This function resolves global parameters of type @{value} within the ParameterCollection
    """
    recurrency_max = 12
    for v in self:
      recurrency = 0
      skip_list = []
      substitute_vars = getSubstitute(v.value)
      while True:
        for substitute_var in substitute_vars:

          # looking in the current scope
          v_other = self.find(substitute_var)

          # looking in the scope of step instance
          if v_other is None and step_parameters is not None:
            v_other = step_parameters.findLinked(substitute_var, False)

          # looking in the scope of workflow
          if v_other is None and wf_parameters is not None:
            v_other = wf_parameters.findLinked(substitute_var, False)

          # finally the action itself
          if v_other is not None and not v_other.isLinked():
            v.value = substitute(v.value, substitute_var, v_other.value)
          elif v_other is not None:
            print("Leaving %s variable for dynamic resolution" % substitute_var)
            skip_list.append(substitute_var)
          else:  # if nothing helped tough!
            print("Can not resolve ", substitute_var, str(v))

        recurrency += 1
        if recurrency > recurrency_max:
          # must be an exception
          print("ERROR! reached maximum recurrency level", recurrency, "within the parameter ", str(v))
          if step_parameters is None:
            if wf_parameters is None:
              print("on the level of Workflow")
            else:
              print("on the level of Step")
          else:
            if wf_parameters is not None:
              print("on the level of Module")
          break
        else:
          substitute_vars = getSubstitute(v.value, skip_list)
          if not substitute_vars:
            break


class AttributeCollection(dict):
  """ Attribute Collection class contains Parameter Collection as a data member
  """

  def __init__(self):
    dict.__init__(self)
    self.parameters = None
    self.parent = None

  def __str__(self):
    ret = ''
    for v in self.keys():
      ret = ret + v + ' = ' + str(self[v]) + '\n'
    return ret

  def toXMLString(self):
    return self.toXML()

  def toXMLFile(self, filename):
    with open(filename, 'w+') as f:
      sarray = self.toXML()
      for element in sarray:
        f.write(element)
    return

  def toXML(self):
    ret = ""
    for v in sorted(self):
      if v == 'parent':
        continue  # doing nothing
      elif v in ['body', 'description']:
        ret = ret + '<' + v + '><![CDATA[' + str(self[v]) + ']]></' + v + '>\n'
      else:
        ret = ret + '<' + v + '>' + str(self[v]) + '</' + v + '>\n'
    return ret

  def addParameter(self, opt, prefix="", postfix=""):
    self.parameters.appendCopy(opt, prefix, postfix)

  def addParameterLinked(self, opt, prefix="", postfix=""):
    self.parameters.appendCopyLinked(opt, prefix, postfix)

  def linkUp(self, opt, prefix="", postfix="", objname="self"):
    self.parameters.linkUp(opt, prefix, postfix, objname)

  def unlink(self, opt):
    self.parameters.unlink(opt)

  def removeParameter(self, name_or_ind):
    self.parameters.remove(name_or_ind)

  def removeAllParameters(self):
    self.parameters.removeAllParameters()

  def findParameter(self, name_or_ind):
    return self.parameters.find(name_or_ind)

  def findParameterIndex(self, ind):
    return self.parameters.findIndex(ind)

  def compareParameters(self, s):
    return self.parameters.compare(s)

  def setValue(self, name, value, type_=None):
    if not self.parameters.setValue(name, value, type_):
      print(" in the object=", type(self), "with name=", self.getName(), "of type=", self.getType())

  def setLink(self, name, module_name, parameter_name):
    if not self.parameters.setLink(name, module_name, parameter_name):
      print(" in the object=", type(self), "with name=", self.getName(), "of type=", self.getType())

  def compare(self, s):
    return (self == s) and self.parameters.compare(s.parameters)

  def setParent(self, parent):
    self.parent = parent

  def getParent(self):
    return self.parent

  # ------------- common functions -----------
  def setName(self, name):
    self['name'] = name

  def getName(self):
    return self.get('name', '')

  def setType(self, att_type):
    self['type'] = att_type

  def getType(self):
    return self.get('type', '')

  def setRequired(self, required):
    self['required'] = required

  def getRequired(self):
    return self['required']

  def setDescription(self, description):
    self['description'] = description

  def getDescription(self):
    return self['description']

  def setDescrShort(self, descr_short):
    self['descr_short'] = descr_short

  def getDescrShort(self):
    return self['descr_short']

  def setBody(self, body):
    self['body'] = body

  def getBody(self):
    return self['body']

  def setOrigin(self, origin):
    self['origin'] = origin

  def getOrigin(self):
    return self['origin']

  def setVersion(self, ver):
    self['version'] = ver

  def getVersion(self):
    return self['version']

  def resolveGlobalVars(self, wf_parameters=None, step_parameters=None):
    self.parameters.resolveGlobalVars(wf_parameters, step_parameters)
