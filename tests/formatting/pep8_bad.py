
#! /usr/bin/env python
# -*- coding: utf-8 -*-
""" Inspired from https://gist.github.com/RichardBronosky/454964087739a449da04
See http://www.python.org/dev/peps/pep-0008/ for more PEP-8 details



This module's docstring summary line.

This is a multi-line docstring. Paragraphs are separated with blank lines.
Normally we should have a limit of a 79 according to pep8, but in DIRAC we allow 120. I am writting a very long line on purpose, but it will not be split

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# I am writting a very long line on purpose, and contrary to the very long line in the docstring, this comment will be split


import os  # STD lib imports first
import sys  # alphabetical

import some_third_party_lib  # 3rd party stuff next
import some_third_party_other_lib  # alphabetical


import local_stuff  # local stuff last
import more_local_stuff
import dont_import_two, modules_in_one_line  # IMPORTANT! DO NOT IMPORT TWO MODULE IN ONE LINE

_a_global_var = 2  # so it won't get imported by 'from foo import *'
_b_global_var = 3

A_CONSTANT = 'ugh.'


# 2 empty lines between top-level funcs + classes
# This function uses 4 space indentation, in DIRAC we want two.
def naming_convention():
    """Write docstrings for ALL public classes, funcs and methods.
       Functions use snake_case.
    """
    if x == 4:  # x is blue <== USEFUL 1-liner comment (2 spaces before #)
        x, y = y, x  # inverse x and y <== USELESS COMMENT (1 space after #)
    c = (a + b) * (a - b)  # operator spacing should improve readability.
    dict['key'] = dict[0] = {'x': 2, 'cat': 'not a dog'}


class NamingConvention(object):
  """First line of a docstring is short and next to the quotes.
     Class and exception names are CapWords.
     Closing quotes are on their own line
     """

  a = 2
  b = 4
  _internal_variable = 3
  class_ = 'foo'  # trailing underscore to avoid conflict with builtin

  # this will trigger name mangling to further discourage use from outside
  # this is also very useful if you intend your class to be subclassed, and
  # the children might also use the same var name for something else; e.g.
  # for simple variables like 'a' above. Name mangling will ensure that
  # *your* a and the children's a will not collide.
  __internal_var = 4

  # NEVER use double leading and trailing underscores for your own names
  __nooooooodontdoit__ = 0

  # don't call anything (because some fonts are hard to distiguish):
  l = 1
  O = 2
  I = 3

  # Long line, bigger than 79
  # There should be no space between the parameter and the default value
  def __init__(self, width, height, color = 'black', emphasis = None, highlight=0,andI=0,willAdd=1,some=2,evenmuch=7,longer=9,argsforhavinglongline = 0):
    if width == 0 and height == 0 and \
       color == 'red' and emphasis == 'strong' or \
       highlight > 100:
      raise ValueError('sorry, you lose')
    if width == 0 and height == 0 and (color == 'red' or emphasis is None):
      raise ValueError("I don't think so -- values are %s, %s" % (width, height))
    Blob.__init__(self, width, height, color, emphasis, highlight)

  # empty lines within method to enhance readability; no set rule
  short_foo_dict = {'loooooooooooooooooooong_element_name': 'cat', 'other_element': 'dog'}

  long_foo_dict_with_many_elements = {'foo': 'cat', 'bar': 'dog'}

  # 1 empty line between in-class def'ns
  def foo_method(self, x, y=None):
    """Method and function names are lower_case_with_underscores.
       Always use self as first arg.
    """
    pass

  @classmethod
  def bar(cls):
    """Use cls!"""
    pass



"""
Common naming convention names:
snake_case
MACRO_CASE
camelCase -> DIRAC
CapWords
"""

# Newline at end of file