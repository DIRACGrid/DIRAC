#!/usr/bin/env python
"""dirac-docs-build-commands.py

  Build scripts documentation from the scripts docstrings. The scripts are not
  very uniform

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import sys

from diracdoctools.cmd.commandReference import run
from diracdoctools.Config import CLParser
sys.exit(run(**(CLParser().optionDict())))
