########################################################################
# File :   Path.py
# Author : Ricardo Graciani
########################################################################
"""
Some Helper class to build CFG paths from tuples
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id$"

import os
import six

cfgInstallSection = 'LocalInstallation'
cfgResourceSection = 'Resources'


def cfgPath(*args):
  """
  Basic method to make a path out of a tuple of string, any of them can be already a path
  """
  path = os.path.join(*[str(k) for k in args])
  return os.path.normpath(path)


def cfgInstallPath(*args):
  """
  Path to Installation/Configuration Options
  """
  return cfgPath(cfgInstallSection, *args)


def cfgPathToList(arg):
  """
  Basic method to split a cfgPath in to a list of strings
  """
  listPath = []
  if not isinstance(arg, six.string_types):
    return listPath
  while arg.find('/') == 0:
    arg = arg[1:]
  return arg.split('/')
