"""Tests for LogFilters."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest


from DIRAC.FrameworkSystem.private.standardLogging.LogLevels import LogLevels
from DIRAC.Resources.LogFilters.PatternFilter import PatternFilter
from DIRAC.Resources.LogFilters.ModuleFilter import ModuleFilter, LEVEL


class Record(object):
  def __init__(self, msg, varmessage='', name=None, level=10):
    self.args = (msg,)
    self.varmessage = varmessage
    self.name = name
    self.levelno = level


@pytest.fixture
def pf():
  options = {'Accept': 'some,Words', 'Reject': 'Foo'}
  pf = PatternFilter(options)
  assert pf._accept == ['some', 'Words']
  assert pf._reject == ['Foo']
  return pf


@pytest.mark.parametrize('record, result', [
    (('Some',), False),
    (('some, Words',), True),
    (('some, Words', 'Foo'), False),
])
def test_filter1(pf, record, result):
  assert pf.filter(Record(*record)) == result


@pytest.fixture
def mf():
  options = {'dirac': 'ERROR', 'l1': 'ERROR', 'l1.l2': 'INFO', 'll1.ll2.ll3': 'VERBOSE'}
  mf = ModuleFilter(options)
  assert mf._configDict == \
      {'dirac': {LEVEL: LogLevels.ERROR},
       'l1': {LEVEL: LogLevels.ERROR,
              'l2': {LEVEL: LogLevels.INFO}},
       'll1': {LEVEL: LogLevels.DEBUG,
               'll2': {LEVEL: LogLevels.DEBUG,
                       'll3': {LEVEL: LogLevels.VERBOSE}}},
       }
  return mf


@pytest.mark.parametrize('name, level, result', [
    ('dirac', LogLevels.INFO, False),
    ('dirac', LogLevels.ERROR, True),
    ('l1', LogLevels.INFO, False),
    ('l1', LogLevels.ERROR, True),
    ('l1.ll2', LogLevels.INFO, False),  # inherits from l1
    ('l1.ll2', LogLevels.ERROR, True),  # inherits from l1
    ('l1.l2', LogLevels.INFO, True),  # set to INFO
    ('l1.l2.l3', LogLevels.INFO, True),  # inherits from l1.l2
    ('ll1.ll2', LogLevels.DEBUG, True),  # base level is DEBUG by default
    ('ll1.ll2.ll3', LogLevels.DEBUG, False),  # set to VERBOSE
    ('ll1.ll2.ll3', LogLevels.INFO, True),  # set to VERBOSE
])
def test_mf(mf, name, level, result):
  assert mf.filter(Record('blabla', name=name, level=level)) == result
