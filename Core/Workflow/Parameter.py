# $Id: Parameter.py,v 1.10 2007/12/05 15:28:41 gkuznets Exp $
"""
    This is a comment
"""
__RCSID__ = "$Revision: 1.10 $"

# unbinded method, returns indentation string
def indent(indent=0):
    return indent*2*' '

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
            self.typein = parameter.typein
            self.typeout = parameter.typeout
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

        if name != None:
            self.name = name
        if type != None:
            self.type = type
        if value != None:
            self.setValue(value)
        if description != None:
            self.description = description
        if linked_module != None:
            self.linked_module = linked_module
        if linked_parameter != None:
            self.linked_parameter = linked_parameter
        if typein != None:
            self.typein = typein
        if typeout != None:
            self.typeout = typeout

    def getName(self):
        return self.name

    def setName(self, n):
        self.name=n # if collection=None it still will work fine

    def getValue(self):
        return self.value

    def getValueTypeCorrected(self):
        if self.isTypeString():
            return "'"+self.value+"'"
        return self.value

    def setValue(self, value, type=None):
        if type != None:
            self.setType(type)
        self.setValueByType(value)

    def setValueByType(self, value):
        type = self.type.lower() # change the register
        if self.isTypeString():
            self.value = str(value)
        elif type == 'float':
            self.value = float(value)
        elif type == 'int':
            self.value = int(value)
        elif type == 'bool':
            self.value = bool(value)
        else:
            #raise TypeError('Can not assing value '+value+' of unknown type '+ self.type + ' to the Parameter '+ str(self.name))
            print 'WARNING: we do not have established conversion algorithm to assing value '+value+' of unknown type '+ self.type + ' to the Parameter '+ str(self.name)
            self.value = value

    def getType(self):
        return self.type

    def setType(self, type):
        self.type=type

    def isTypeString(self):
        """returns True if type is the string kind"""
        type = self.type.lower() # change the register
        if type == 'string' or type == 'jdl' or \
           type == 'option'  or type == 'parameter' or \
           type == 'jdlreqt':
           return True
        return False

    def getDescription(self):
        return self.description

    def setDescription(self, descr):
        self.description = descr

    def link(self, module, parameter):
        self.linked_module = module
        self.linked_parameter = parameter

    def unlink(self):
        self.linked_module = None
        self.linked_parameter = None

    def getLinkedModule(self):
        return self.linked_module

    def getLinkedParameter(self):
        return self.linked_parameter

    def getLink(self):
        # we have 4 possibilities
        # two fields can be filled independently
        # it is possible to fill one fiels with the valid information
        # spaces shall be ignored ( using strip() function)
        if (self.linked_module==None) or (self.linked_module.strip()==''):
            if (self.linked_parameter==None) or (self.linked_parameter.strip()==''):
                # both empty
                return ""
            else:
                # parameter fielled
                return self.linked_parameter
        else:
            if (self.linked_parameter==None) or (self.linked_parameter.strip()==''):
                return self.linked_module
        return self.linked_module+'.'+self.linked_parameter

    def isLinked(self):
        if (self.linked_module.strip()==None) or (self.linked_module.strip()==''):
            if (self.linked_parameter==None) or (self.linked_parameter.strip()==''):
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
        if i :
            self.typein = True
        else:
            self.typein = False

    def setOutput(self, i):
        if i :
            self.typeout = True
        else:
            self.typeout = False

    def __str__(self):
        return str(type(self))+": name="+self.name + " value="+str(self.value) +" type="+str(self.type)\
        +" linked_module="+str(self.linked_module) + " linked_parameter="+str(self.linked_parameter)\
        +" in="+ str(self.typein)+ " out="+str(self.typeout)\
        +" description="+str(self.description)

        return ret

    def toXML(self):
        return '<Parameter name="'+self.name +'" type="'+str(self.type)\
        +'" linked_module="'+str(self.linked_module) + '" linked_parameter="'+str(self.linked_parameter)\
        +'" in="'+ str(self.typein)+ '" out="'+str(self.typeout)\
        +'" description="'+ str(self.description)+'">'\
        +'<value><![CDATA['+str(self.value)+']]></value>'\
        +'</Parameter>\n'

# we got a problem with the index() function
#    def __eq__(self, s):
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
            return false
#
#    def __deepcopy__(self, memo):
#        return Parameter(parameter=self)
#
#    def __copy__(self):
#        return self.__deepcopy__({})

    def copy(self, parameter):
        if isinstance(parameter, Parameter):
            self.name = parameter.name
            self.value = parameter.value
            self.type = parameter.type
            self.description = parameter.description
            self.linked_module = parameter.linked_module
            self.linked_parameter = parameter.linked_parameter
            self.typein = parameter.typein
            self.typeout = parameter.typeout
        else:
            raise TypeError('Can not make a copy of object '+ str(type(self)) + ' from the '+ str(type(parameter)))


    def createParameterCode(self, ind=0, instance_name=None):

        if (instance_name == None) or (instance_name == ''):
            ret = indent(ind) + self.getName()+' = '+self.getValueTypeCorrected()
        else:
            if self.isLinked():
                ret = indent(ind)+instance_name+'.'+ self.getName()+' = '+self.getLink()
            else:
                ret = indent(ind)+instance_name+'.'+ self.getName()+' = '+str(self.getValueTypeCorrected())

        return ret+'  # type='+self.getType()+' in='+str(self.isInput())+' out='+str(self.isOutput())+' ' +self.getDescription()+'\n'

class ParameterCollection(list):

    def __init__(self, coll=None):
        list.__init__(self)
        if isinstance(coll, ParameterCollection):
            # makes a deep copy of the parameters
            for v in coll:
                self.append(Parameter(parameter=v))
        elif coll != None:
            raise TypeError('Can not create object type '+str(type(self))+' from the '+ str(type(coll)))

    def append(self, opt):
        if isinstance(opt, ParameterCollection):
            for p in opt:
                list.append(self, p)
        elif isinstance(opt, Parameter):
            list.append(self, opt)
            return opt
        else:
            raise TypeError('Can not append object type '+ str(type(opt))+' to the '+str(type(self))+'. Parameter type appendable only')

    def appendCopy(self, opt):
        if isinstance(opt, ParameterCollection):
            for p in opt:
                list.append(self, Parameter(parameter=p))
        elif isinstance(opt, Parameter):
            list.append(self, Parameter(parameter=opt))
            return opt
        else:
            raise TypeError('Can not append object type '+ str(type(opt))+' to the '+str(type(self))+'. Parameter type appendable only')

    def removeAllParameters(self):
        self[:]=[]

    def remove(self, name_or_ind):
        # work for index as well as for the string
        if isinstance(name_or_ind, string): # we given name
            del self[self.findIndex(name_or_ind)]
        elif isinstance(name_or_ind, int) or isinstance(name_or_ind): # we given index
            del self[name_or_ind]

    def find(self, name_or_ind):
        # work for index as well as for the string
        if isinstance(name_or_ind, str): # we given name
            for v in self:
                if v.getName() == name_or_ind:
                    return v
            return None

        elif isinstance(name_or_ind, int) or isinstance(name_or_ind, long): # we given index
            return self[name_or_ind]
        return self[int(name_or_ind)]


    def findIndex(self, name):
        i=0
        for v in self:
            if v.getName() == name:
                return i
            i=i+1
        return -1

    def getParametersNames(self):
        list=[]
        for v in self:
            list.append(v.getName())
        return list

    def compare(self, s):
        # we compearing parameters only, the attributes will be compared in hierarhy above
        # we ignore the position of the Parameter in the list
        # we assume that names of the Parameters are DIFFERENT otherwise we have to change alghorithm!!!
        if (not isinstance(s, ParameterCollection)) or (len(s) != len(self)):
            return False
        for v in self:
            for i in s:
                if v.getName() == i.getName():
                    if not v.compare(i):
                        return False
                    else:
                        break
            else:
                #if we reached this place naturally we can not find matching name
                return False
        return True


    def __str__(self):
        ret=str(type(self))+':\n'
        for v in self:
            ret=ret+ str(v)+'\n'
        return ret

    def toXML(self):
        ret=""
        for v in self:
            ret=ret+v.toXML()
        return ret

    def createParametersCode(self, indent=0, instance_name=None):
        str=''
        for v in self:
            if v.preExecute():
                str=str+v.createParameterCode(indent, instance_name)
        return str

    def resolveGlobalVars(self, wf_parameters=None, step_parameters=None):
        """This function resolves global parameters of type @{value} within the ParameterCollection
        """
        recurrency_max=12
        recurrency=0
        # let us find the
        for v in self:
            if v.isTypeString():
                start=v.value.find('@{')
                stop=-1
                while start > -1 :
                    stop=v.value.find('}',start+1)
                    parameterName=v.value[start+2:stop]
                    #print v.value, start, stop, parameterName, v.value[start:stop+1]
                    # looking in the currens scope
                    v_other = self.find(parameterName)
                    # looking in the scope of step instance
                    if v_other == None and step_parameters != None :
                        v_other = step_parameters.find(parameterName)
                    # looking in the scope of workflow
                    if v_other == None and wf_parameters != None :
                        v_other = wf_parameters.find(parameterName)
                    if v_other != None:
                        v.value = v.value[:start]+v_other.value+v.value[stop+1:]
                        # we replaced part of the string so we need to reset indexes
                        start=0
                    else: # if nothing helped tough!
                        print "can not resolve ", v.value[start:stop+1]
                        return
                    recurrency=recurrency+1
                    if recurrency > recurrency_max:
                        # mast be an exception
                        print "ERROR! reached maxumum recurrency level", recurrency, "within the parameter ", v.value
                        if step_parameters == None:
                            if wf_parameters == None:
                                print "on the level of Workflow"
                            else:
                                print "on the level of Step"
                        else:
                            if wf_parameters != None:
                                print "on the level of Module"
                        return
                    start=v.value.find('@{', start)

class AttributeCollection(dict):

    def __init__(self):
        dict.__init__(self)
        self.parameters=None
        self.parent=None

    def __str__(self):
        ret = ''
        for v in self.keys():
            ret=ret+v+' = '+str(self[v])+'\n'
        return ret

    def toXMLString(self):
        return self.toXML()

    def toXMLFile(self, filename):
        f = open(filename,'w+')
        sarray = self.toXML()
        for element in sarray:
            f.write(element)
        f.close()
        return

    def toXML(self):
        ret = ""
        for v in self.keys():
            if v == 'parent':
                continue # doing nothing
            elif v == 'body' or v == 'description':
                ret=ret+'<'+v+'><![CDATA['+str(self[v])+']]></'+v+'>\n'
            else:
                ret=ret+'<'+v+'>'+str(self[v])+'</'+v+'>\n'
        return ret


    def appendParameter(self, opt):
        self.parameters.append(opt)

    def appendParameterCopy(self, opt):
        self.parameters.appendCopy(opt)

    def removeParameter(self, name_or_ind):
        self.parameters.remove(name_or_ind)

    def removeAllParameters(self):
        self.parameters.removeAllParameters()

    def findParameter(self, name_or_ind):
        return self.parameters.find(name_or_ind)

    def findParameterIndex(self, name):
        return self.parameters.findIndex(name_or_ind)

    def compareParameters(self, s):
        return self.parameters.compare(s)

    def compare(self, s):
        return (self == s) and  self.parameters.compare(s.parameters)

    def setParent(self, parent):
        self.parent=parent

    def getParent(self):
        return self.parent

    # ------------- common functions -----------
    def setName(self, name):
        # we have to replace _ with the printable character
        # for that we create temporary string
        if name:
            nametmp = name.replace('_', '0')
            if not nametmp.isalnum( ):
                raise AttributeError('Can not have NOT alphnumeric name for the object'+ str(type(self))+' requested name='+name)
        self['name'] = name

    def getName(self):
        return self['name']

    def setType(self, type_):
        # we have to replace _ with the printable character
        # for that we create temporary string
        if type_:
            typetmp = type_.replace('_', '0')
            if not typetmp.isalnum():
                raise AttributeError('We can have alphnumeric characters only as type for the object'+ str(type(self))+' type='+type_)
        self['type'] = type_

    def getType(self):
        return self['type']

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
        self['version']=ver

    def getVersion(self):
        return self['version']

    def resolveGlobalVars(self, wf_parameters=None, step_parameters=None):
        self.parameters.resolveGlobalVars(wf_parameters, step_parameters)

