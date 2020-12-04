""" Implementation of Module
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=unused-wildcard-import,wildcard-import

import copy
import os

# try: # this part to import as part of the DIRAC framework
from DIRAC.Core.Workflow.Parameter import *

__RCSID__ = "$Id$"


class ModuleDefinition(AttributeCollection):

  def __init__(self, type=None, obj=None, parent=None):
    # we can create an object from another module
    # or from the ParameterCollection
    AttributeCollection.__init__(self)
    self.main_class_obj = None  # used for the interpretation only
    # self.module_obj = None     # used for the interpretation only
    self.parent = parent

    if (obj is None) or isinstance(obj, ParameterCollection):
      self.setType('nitgiven')
      self.setDescrShort('')
      self.setDescription('')
      self.setRequired('')
      self.setBody('')
      self.setOrigin('')
      self.setVersion(0.0)
      self.parameters = ParameterCollection(obj)  # creating copy

    elif isinstance(obj, ModuleDefinition):
      self.setType(obj.getType())
      self.setDescrShort(obj.getDescrShort())
      self.setDescription(obj.getDescription())
      self.setBody(obj.getBody())
      self.setRequired(obj.getRequired())
      self.setOrigin(obj.getOrigin())
      self.setVersion(obj.getVersion())
      self.parameters = ParameterCollection(obj.parameters)
    else:
      raise TypeError('Can not create object type ' + str(type(self)) + ' from the ' + str(type(obj)))
    if type:
      self.setType(type)

  def createCode(self):
    return self.getBody() + '\n'

  def __str__(self):
    return str(type(self)) + ':\n' + AttributeCollection.__str__(self) + self.parameters.__str__()

  def toXML(self):
    ret = '<ModuleDefinition>\n'
    ret = ret + AttributeCollection.toXML(self)
    ret = ret + self.parameters.toXML()
    ret = ret + '</ModuleDefinition>\n'
    return ret

  def toXMLFile(self, outFile):
    if os.path.exists(outFile):
      os.remove(outFile)
    with open(outFile, 'w') as xmlfile:
      xmlfile.write(self.toXML())

  def loadCode(self):
    # print 'Loading code of the Module =', self.getType()
    # version 1 - OLD sample
    #ret = compile(self.getBody(),'<string>','exec')
    # eval(ret)
    # return ret #returning ref just in case we might need it
    #
    if self.getBody():  # checking the size of the string
      # version 2 - we assume that each self.body is a module oblect
      # module = new.module(self.getType())    # create empty module object
      # sys.modules[self.getType()] = module   # add reference for the import operator
      # exec self.getBody() in module.__dict__ # execute code itself
      # self.module_obj = module               # save pointer to this module
      # if module.__dict__.has_key(self.getType()):
      #    self.main_class_obj = module.__dict__[self.getType()] # save class object

      # version 3
      # A.T.  Use vars() function to inspect local objects instead of playing with
      #       fake modules. We assume that after the body execution there will be
      #       a class with name "self.getType()" defined in the local scope.
      exec(self.getBody())
      if self.getType() in vars():
        self.main_class_obj = vars()[self.getType()]  # save class object
      else:
        # it is possible to have this class in another module, we have to check for this
        # but it is advisible to use 'from module import class' operator
        # otherwise i could not find the module. But it is possible that
        # in the future I can change this code to do it more wisely
        sErr = 'Can not find class '\
               + self.getType()\
               + ' in the module created from the body of the module '\
               + self.getOrigin()
        raise TypeError(sErr)
    else:
      raise TypeError('The body of the Module ' + self.getType() + ' seems empty')
    return self.main_class_obj


class ModuleInstance(AttributeCollection):

  def __init__(self, name, obj=None, parent=None):
    AttributeCollection.__init__(self)
    self.instance_obj = None  # used for the interpretation only
    self.parent = parent

    if obj is None:
      self.parameters = ParameterCollection()
    elif isinstance(obj, ModuleInstance) or isinstance(obj, ModuleDefinition):
      if name is None:
        self.setName(obj.getName())
      else:
        self.setName(name)
      self.setType(obj.getType())
      self.setDescrShort(obj.getDescrShort())
      self.parameters = ParameterCollection(obj.parameters)
    elif isinstance(obj, ParameterCollection):
      # set attributes
      self.setName(name)
      self.setType("")
      self.setDescrShort("")
      self.parameters = ParameterCollection(obj)
    elif obj is not None:
      raise TypeError('Can not create object type ' + str(type(self)) + ' from the ' + str(type(obj)))

  def createCode(self, ind=2):
    str = indent(ind) + self.getName() + ' = ' + self.getType() + '()\n'
    str = str + self.parameters.createParametersCode(ind, self.getName())
    str = str + indent(ind) + self.getName() + '.execute()\n\n'
    return str

  def __str__(self):
    return str(type(self)) + ':\n' + AttributeCollection.__str__(self) + self.parameters.__str__()

  def toXML(self):
    ret = '<ModuleInstance>\n'
    ret = ret + AttributeCollection.toXML(self)
    ret = ret + self.parameters.toXML()
    ret = ret + '</ModuleInstance>\n'
    return ret

  def execute(self, step_parameters, definitions):
    # print 'Executing ModuleInstance ',self.getName(),'of type',self.getType()
    self.instance_obj = definitions[self.getType()].main_class_obj()  # creating instance
    # FIXME: pylint complains that ParameterCollection doesn't have execute. What should this be?
    self.parameters.execute(self.getName())  # pylint: disable=no-member
    self.instance_obj.execute2()


class DefinitionsPool(dict):

  def __init__(self, parent, pool=None):
    dict.__init__(self)
    self.parent = parent  # this is a cache value, we propagate it into next level
    if isinstance(pool, DefinitionsPool):
      for k in pool:
        v = pool[k]
        if isinstance(v, ModuleDefinition):
          obj = ModuleDefinition(None, v, self.parent)
        elif isinstance(v, StepDefinition):  # pylint: disable=undefined-variable
          obj = StepDefinition(None, v, self.parent)  # pylint: disable=undefined-variable
        else:
          raise TypeError('Error: __init__ Wrong type of object stored in the DefinitionPool ' + str(type(pool[v])))
        self.append(obj)

    elif pool is not None:
      raise TypeError('Can not create object type ' + str(type(self)) + ' from the ' + str(type(pool)))

  def __setitem__(self, i, obj):
    if i not in self:
      dict.__setitem__(self, i, obj)
#       print 'We need to write piece of code to replace existent DefinitionsPool.__setitem__()'
#       print 'For now we ignore it for the', obj.getType()

  def append(self, obj):
    """ We add new Definition (Module, Step)
    """
    self[obj.getType()] = obj
    obj.setParent(self.parent)
    return obj

  def remove(self, obj):
    del self[obj.getType()]
    obj.setParent(None)

  def compare(self, s):
    if not isinstance(s, DefinitionsPool):
      return False  # chacking types of objects
    if len(s) != len(self):
      return False  # checkin size
    # we need to compare the keys of dictionaries
    if set(self) != set(s):
      return False
    for k in self:
      if (k not in s) or (not self[k].compare(s[k])):
        return False
    return True

  def __str__(self):
    ret = str(type(self)) + ': number of Definitions:' + str(len(self)) + '\n'
    index = 0
    for k in self:
      ret = ret + 'definition(' + str(index) + ')=' + str(self[k]) + '\n'
      index = index + 1
    return ret

  def setParent(self, parent):
    self.parent = parent
    # we need to propagate it just in case it was different one
    for k in self:
      self[k].setParent(parent)

  def getParent(self):
    return self.parent

  def updateParents(self, parent):
    self.parent = parent
    for k in self:
      self[k].updateParents(parent)

  def toXML(self):
    ret = ''
    for k in self:
      ret = ret + self[k].toXML()
    return ret

  def createCode(self):
    str = ''
    for k in self:
      # str=str+indent(2)+'# flush code for instance\n'
      str = str + self[k].createCode()
    return str

  def loadCode(self):
    for k in self:
      # load code of the modules
      self[k].loadCode()


class InstancesPool(list):

  def __init__(self, parent, pool=None):
    list.__init__(self)
    self.parent = None  # this is a cache value, we propagate it into next level
    if isinstance(pool, InstancesPool):
      for v in pool:
        # I need to check this fubction
        # if it would be a costructor we coul pass parent into it
        self.append(copy.deepcopy(v))
        if isinstance(v, ModuleInstance):
          obj = ModuleInstance(None, v, self.parent)
        elif isinstance(v, StepInstance):  # pylint: disable=undefined-variable
          obj = StepInstance(None, v, self.parent)  # pylint: disable=undefined-variable
        else:
          raise TypeError('Error: __init__ Wrong type of object stored in the DefinitionPool ' + str(type(pool[v])))
        self.append(obj)

    elif pool is not None:
      raise TypeError('Can not create object type ' + str(type(self)) + ' from the ' + str(type(pool)))

  def __str__(self):
    ret = str(type(self)) + ': number of Instances:' + str(len(self)) + '\n'
    index = 0
    for v in self:
      ret = ret + 'instance(' + str(index) + ')=' + str(v) + '\n'
      index = index + 1
    return ret

  def setParent(self, parent):
    self.parent = parent
    for v in self:
      v.setParent(parent)

  def getParent(self):
    return self.parent

  def updateParents(self, parent):
    self.parent = parent
    for v in self:
      v.updateParents(parent)

  def append(self, obj):
    list.append(self, obj)
    obj.setParent(self.parent)

  def toXML(self):
    ret = ''
    for v in self:
      ret = ret + v.toXML()
    return ret

  def findIndex(self, name):
    i = 0
    for v in self:
      if v.getName() == name:
        return i
      i = i + 1
    return - 1

  def find(self, name):
    for v in self:
      if v.getName() == name:
        return v
    return None

  def delete(self, name):
    for v in self:
      if v.getName() == name:
        self.remove(v)
        v.setParent(None)

  def compare(self, s):
    if (not isinstance(s, InstancesPool) or (len(s) != len(self))):
      return False
    for v in self:
      for i in s:
        if v.getName() == i.getName():
          if not v.compare(i):
            return False
          else:
            break
      else:
        # if we reached this place naturally we can not find matching name
        return False
    return True

  def createCode(self):
    str = ''
    for inst in self:
      str = str + inst.createCode()
      str = str + indent(2) + '# output assignment\n'
      for v in inst.parameters:
        if v.isOutput():
          str = str + v.createParameterCode(2, 'self')
      str = str + '\n'
    return str
