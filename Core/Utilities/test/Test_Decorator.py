from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from DIRAC.Core.Utilities.Decorators import executeOnlyIf


class AnyClass(object):

  def __init__(self):
    self.attr = None

  @executeOnlyIf('attr', False)
  def withoutSpecificValue(self):
    """ Decorator with no specific value """
    return True

  @executeOnlyIf('undefined', False)
  def withoutSpecificValueUndefined(self):
    """ Decorator with no specific value on undefined attribute"""
    return True

  @executeOnlyIf('attr', False, attrVal='specific')
  def withSpecificValue(self):
    """ Decorator with specific value """
    return True

  @executeOnlyIf('undefined', False, attrVal='specific')
  def withSpecificValueUndefined(self):
    """ Decorator with specific value on undefined attribute"""
    return True

  @executeOnlyIf('attr1', 'attr1Missing')
  @executeOnlyIf('attr2', 'attr2Missing')
  def doubleDeco(self):
    """ Double deco"""
    return True


# Undefined attribute

def test_without_specific_value_undefined():
  """ Test on undefined value"""
  a = AnyClass()
  assert a.withoutSpecificValueUndefined() is False


def test_with_specific_value_undefined():
  """ Test on undefined value"""
  a = AnyClass()
  assert a.withSpecificValueUndefined() is False


# Not initialized

def test_without_specific_value_not_initialized():
  """ Test when the value was not initialized"""
  a = AnyClass()
  assert a.withoutSpecificValue() is False


def test_with_specific_value_not_initialized():
  """ Test when the value was not initialized"""
  a = AnyClass()
  assert a.withSpecificValue() is False


def test_without_specific_value_initialized():
  """ Test when the value was initialized to anything that eval to True"""
  a = AnyClass()

  a.attr = True
  assert a.withoutSpecificValue()

  a.attr = 1
  assert a.withoutSpecificValue()

  a.attr = 'str'
  assert a.withoutSpecificValue()


def test_with_specific_value_initialized():
  """ Test when the value was initialized to anything that eval to True but not what we want"""
  a = AnyClass()

  a.attr = True
  assert a.withSpecificValue() is False

  a.attr = 1
  assert a.withSpecificValue() is False

  a.attr = 'str'
  assert a.withSpecificValue() is False


def test_without_specific_value_bad_initialized():
  """ Test when the value was initialized but with something that evaluate to False """
  a = AnyClass()

  a.attr = False
  assert a.withoutSpecificValue() is False

  a.attr = []
  assert a.withoutSpecificValue() is False


def test_with_specific_value_correct():
  """ Test when the value was not initialized to anything that should evaluate to True"""
  a = AnyClass()
  a.attr = 'specific'
  assert a.withSpecificValue()


def test_doubleDeco():
  """ Test when the value was not initialized to anything that should evaluate to True"""
  a = AnyClass()

  assert a.doubleDeco() == 'attr1Missing'

  a.attr2 = True
  assert a.doubleDeco() == 'attr1Missing'

  a.attr2 = False
  a.attr1 = True

  assert a.doubleDeco() == 'attr2Missing'

  a.attr2 = True
  assert a.doubleDeco()
