# This file was created automatically by SWIG.
# Don't modify this file, modify the SWIG interface instead.
# This file is compatible with both classic and new-style classes.

import _ClassAdBase

def _swig_setattr(self,class_type,name,value):
    if (name == "this"):
        if isinstance(value, class_type):
            self.__dict__[name] = value.this
            if hasattr(value,"thisown"): self.__dict__["thisown"] = value.thisown
            del value.thisown
            return
    method = class_type.__swig_setmethods__.get(name,None)
    if method: return method(self,value)
    self.__dict__[name] = value

def _swig_getattr(self,class_type,name):
    method = class_type.__swig_getmethods__.get(name,None)
    if method: return method(self)
    raise AttributeError,name

import types
try:
    _object = types.ObjectType
    _newclass = 1
except AttributeError:
    class _object : pass
    _newclass = 0
del types


class ClassAd(_object):
    __swig_setmethods__ = {}
    __setattr__ = lambda self, name, value: _swig_setattr(self, ClassAd, name, value)
    __swig_getmethods__ = {}
    __getattr__ = lambda self, name: _swig_getattr(self, ClassAd, name)
    def __repr__(self):
        return "<C ClassAd instance at %s>" % (self.this,)
    def __del__(self, destroy=_ClassAdBase.delete_ClassAd):
        try:
            if self.thisown: destroy(self)
        except: pass
    def __init__(self, *args):
        _swig_setattr(self, ClassAd, 'this', _ClassAdBase.new_ClassAd(*args))
        _swig_setattr(self, ClassAd, 'thisown', 1)
    def insertExpression(*args): return _ClassAdBase.ClassAd_insertExpression(*args)
    def insertAttributeClassAd(*args): return _ClassAdBase.ClassAd_insertAttributeClassAd(*args)
    def insertAttributeInt(*args): return _ClassAdBase.ClassAd_insertAttributeInt(*args)
    def insertAttributeFloat(*args): return _ClassAdBase.ClassAd_insertAttributeFloat(*args)
    def insertAttributeBool(*args): return _ClassAdBase.ClassAd_insertAttributeBool(*args)
    def insertAttributeString(*args): return _ClassAdBase.ClassAd_insertAttributeString(*args)
    def get_expression(*args): return _ClassAdBase.ClassAd_get_expression(*args)
    def clear(*args): return _ClassAdBase.ClassAd_clear(*args)
    def deleteAttribute(*args): return _ClassAdBase.ClassAd_deleteAttribute(*args)
    def get_expression_string(*args): return _ClassAdBase.ClassAd_get_expression_string(*args)
    def get_attribute_int(*args): return _ClassAdBase.ClassAd_get_attribute_int(*args)
    def get_attribute_float(*args): return _ClassAdBase.ClassAd_get_attribute_float(*args)
    def get_attribute_string(*args): return _ClassAdBase.ClassAd_get_attribute_string(*args)
    def get_attribute_bool(*args): return _ClassAdBase.ClassAd_get_attribute_bool(*args)
    def update(*args): return _ClassAdBase.ClassAd_update(*args)
    def modify(*args): return _ClassAdBase.ClassAd_modify(*args)
    def copy(*args): return _ClassAdBase.ClassAd_copy(*args)
    def copyFrom(*args): return _ClassAdBase.ClassAd_copyFrom(*args)
    def sameAs(*args): return _ClassAdBase.ClassAd_sameAs(*args)
    def flatten(*args): return _ClassAdBase.ClassAd_flatten(*args)
    def asJDL(*args): return _ClassAdBase.ClassAd_asJDL(*args)

class ClassAdPtr(ClassAd):
    def __init__(self, this):
        _swig_setattr(self, ClassAd, 'this', this)
        if not hasattr(self,"thisown"): _swig_setattr(self, ClassAd, 'thisown', 0)
        _swig_setattr(self, ClassAd,self.__class__,ClassAd)
_ClassAdBase.ClassAd_swigregister(ClassAdPtr)

class MatchClassAd(ClassAd):
    __swig_setmethods__ = {}
    for _s in [ClassAd]: __swig_setmethods__.update(_s.__swig_setmethods__)
    __setattr__ = lambda self, name, value: _swig_setattr(self, MatchClassAd, name, value)
    __swig_getmethods__ = {}
    for _s in [ClassAd]: __swig_getmethods__.update(_s.__swig_getmethods__)
    __getattr__ = lambda self, name: _swig_getattr(self, MatchClassAd, name)
    def __repr__(self):
        return "<C MatchClassAd instance at %s>" % (self.this,)
    def __init__(self, *args):
        _swig_setattr(self, MatchClassAd, 'this', _ClassAdBase.new_MatchClassAd(*args))
        _swig_setattr(self, MatchClassAd, 'thisown', 1)
    def __del__(self, destroy=_ClassAdBase.delete_MatchClassAd):
        try:
            if self.thisown: destroy(self)
        except: pass
    def initialize(*args): return _ClassAdBase.MatchClassAd_initialize(*args)
    def release(*args): return _ClassAdBase.MatchClassAd_release(*args)

class MatchClassAdPtr(MatchClassAd):
    def __init__(self, this):
        _swig_setattr(self, MatchClassAd, 'this', this)
        if not hasattr(self,"thisown"): _swig_setattr(self, MatchClassAd, 'thisown', 0)
        _swig_setattr(self, MatchClassAd,self.__class__,MatchClassAd)
_ClassAdBase.MatchClassAd_swigregister(MatchClassAdPtr)


