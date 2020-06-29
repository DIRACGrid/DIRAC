""" few utilities
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import shutil

# pylint: disable=missing-docstring


def cleanTestDir():
  for fileIn in os.listdir('.'):
    if 'Local' in fileIn and os.path.isdir(fileIn):
      shutil.rmtree(fileIn)
  for fileToRemove in ['std.out', 'std.err']:
    try:
      os.remove(fileToRemove)
    except OSError:
      continue


def find_all(name, path, directory=None):
  """ Simple files finder
  """
  result = []
  for root, _dirs, files in os.walk(path):
    if name in files:
      result.append(os.path.join(root, name))
  result = [os.path.abspath(p) for p in result]
  if directory:
    if directory not in os.getcwd():
      return [x for x in result if directory in x]
  return result


class MatchStringWith(str):
  """ helper class to match sub strings in a mock.assert_called_with

  >>> myMock.log.error.assert_called_with( MatchStringWith('error mess') )
  """

  def __eq__(self, other):
    return self in str(other)
