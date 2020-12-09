#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: lintao

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division


class HelperReadOnly(object):
  def __init__(self, val):
    self.val = val

  def __get__(self, obj, objtype):
    return self.val

  def __set__(self, obj, val):
    raise AttributeError("can't modify attribute")


class AbsFileSystem(object):
  fs_name = HelperReadOnly("AbsFileSystem")
  seq = HelperReadOnly("/")

  def list_dir(self, path):
    raise NotImplementedError

  def is_dir(self, path):
    raise NotImplementedError


import os
import os.path


class UnixLikeFileSystem(AbsFileSystem):
  fs_name = HelperReadOnly("UnixLikeFileSystem")
  seq = HelperReadOnly("/")

  def list_dir(self, path):
    if not self.is_dir(path):
      raise StopIteration
    for entry in os.listdir(path):
      if self.is_dir(os.path.join(path, entry)):
        entry += self.seq
      yield entry

  def is_dir(self, path):
    return os.path.isdir(path)
  pass


class DFCFileSystem(AbsFileSystem):
  fs_name = HelperReadOnly("DFCFileSystem")
  seq = HelperReadOnly("/")

  def __init__(self, fc):
    self.fc = fc

  def list_dir(self, path):
    if path.endswith('/'):
      path = path.replace('//', '/')
      path = os.path.normpath(path)
    if not self.is_dir(path):
      print("It is not Directory")
      raise StopIteration

    result = self.fc.listDirectory(path, False)
    if not result['OK']:
      print("some errors.")
      raise StopIteration

    content = result['Value']['Successful'].get(path, False)
    if not content:
      raise StopIteration

    if content['Files']:
      for fn in content['Files']:
        yield self.gen_no_prefix_content(fn, path)
    if content['SubDirs']:
      for dn in content['SubDirs']:
        yield self.gen_no_prefix_content(dn, path) + "/"

  def gen_no_prefix_content(self, dn, parent_dn):
    subdn = dn
    if dn.startswith(parent_dn):
      # remove the prefix
      subdn = dn[len(parent_dn):]
      if subdn.startswith("/"):
        subdn = subdn[1:]
    return subdn

  def is_dir(self, path):
    if path.endswith('/'):
      path = path.replace('//', '/')
      path = os.path.normpath(path)
    result = self.fc.isDirectory(path)
    if not result['OK']:
      return False
    return result['Value']['Successful'].get(path, False)


if __name__ == "__main__":
  ulfs = UnixLikeFileSystem()
  print("FS", ulfs.fs_name)
  print("SEQ", ulfs.seq)

  print(list(ulfs.list_dir("/")))
  print(list(ulfs.list_dir("/bad")))
